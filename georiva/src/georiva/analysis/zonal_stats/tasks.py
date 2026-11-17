import logging
from datetime import timedelta

from georiva.config.celery import app
from georiva.events.tasks import EntryTask, ReadSideTask
from georiva.utils.periodic import PeriodicSpec, register_periodic_tasks

logger = logging.getLogger(__name__)


@app.task(
    name="georiva.analysis.zonal_stats.tasks.compute_boundary_zonal_stats",
    base=ReadSideTask,
    bind=True,
    max_retries=3,
    acks_late=True,
    # Deferrable bookkeeping: an ingested COG is already servable without its
    # zonal statistics — tiles, STAC and EDR all answer without them — and one
    # large fetch fans this out per asset.  Keeping it off georiva-ingestion
    # stops that fan-out delaying the next file to be extracted (#398).
    # Idempotent — persist_stats upserts on its unique constraint — so it is
    # safe on a multi-slot worker.  A dropped run is re-issued by
    # sweep_missing_boundary_stats (#402) and, for a harvested asset of any
    # age, by the next harvest of its feed (#716).
    queue="georiva-bookkeeping",
)
def compute_boundary_zonal_stats(self, asset_id: int) -> None:
    """
    Compute and persist zonal statistics for one COG Asset.

    Steps
    -----
    1. Load the Asset, Item, Variable, and Collection.
    2. Check collection.boundary_stats_level — exit early if not set.
    3. Resolve AdminBoundary queryset for the configured level.
    4. Read band 1, opening the COG by range where it lives — our bucket or
       upstream (ADR 0043 §7).
    5. Compute stats via compute_stats_from_band(), once per level.
    6. Persist via persist_stats().

    Retries up to 3 times on transient failures (network, MinIO or upstream timeout).
    """
    from django.db import InterfaceError, OperationalError
    from rasterio.errors import RasterioError

    from georiva.core.models import Asset

    from .service import (
        compute_stats_from_band,
        get_boundaries_for_collection,
        persist_stats,
        read_band,
    )

    try:
        asset = Asset.objects.select_related(
            "item",
            "item__collection",
            "item__collection__catalog",
            "variable",
        ).get(pk=asset_id, format=Asset.Format.COG)
    except Asset.DoesNotExist:
        logger.warning(
            "compute_boundary_zonal_stats: asset %d not found or not COG",
            asset_id,
        )
        return

    collection = asset.item.collection
    boundaries_by_level = get_boundaries_for_collection(collection)

    if not boundaries_by_level:
        logger.debug(
            "compute_boundary_zonal_stats: no boundary level configured for collection %s — skipping",
            collection.slug,
        )
        return

    try:
        # Opening is reading now (by range, ADR 0043 §7), so an unopenable COG
        # surfaces here as a RasterioError and takes the not-computable path
        # below — unless it is Remote and the error is I/O, which is retried
        # like any I/O failure on the way.
        band = read_band(asset.location)
        for level, boundaries in boundaries_by_level.items():
            stats_rows = compute_stats_from_band(band, boundaries)

            count = persist_stats(
                item=asset.item,
                variable=asset.variable,
                stats_rows=stats_rows,
            )

            logger.info(
                "compute_boundary_zonal_stats: %d row(s) at level %d for asset %d",
                count,
                level,
                asset_id,
            )
    except RasterioError as exc:
        if asset.location.is_remote and isinstance(exc, OSError):
            # An Upstream that does not answer — a 5xx, a timeout — is not a bad
            # COG: the read is retried under the same bound as a MinIO hiccup.
            # Nothing here wrote a catalog row, so a failure changes none (#639).
            logger.warning(
                "compute_boundary_zonal_stats: upstream read of asset %d failed (will retry): %s",
                asset_id,
                exc,
            )
            raise self.retry(exc=exc) from exc
        # An unreadable COG fails the same way every time — retrying only burns
        # the budget.  The sweep re-issues it, and it surfaces in the log.
        logger.exception("compute_boundary_zonal_stats: asset %d is not computable", asset_id)
        raise
    except (OSError, OperationalError, InterfaceError) as exc:
        # Transient: a DB blip, or an I/O hiccup reading MinIO or upstream.  Once retries are exhausted
        # sweep_missing_boundary_stats picks the asset up.
        logger.warning("compute_boundary_zonal_stats: asset %d failed (will retry): %s", asset_id, exc)
        raise self.retry(exc=exc) from exc


def loaded_levels(collection) -> list[int]:
    """
    The boundary levels ``collection`` computes that have boundaries loaded:
    the only ones a run of ``compute_boundary_zonal_stats`` can write a row at.
    """
    from adminboundarymanager.models import AdminBoundary

    levels = getattr(collection, "boundary_stats_levels", None)
    if not levels:
        return []
    return sorted(set(AdminBoundary.objects.filter(level__in=levels).values_list("level", flat=True)))


def computes_boundary_stats(collection) -> bool:
    """
    Whether a run of ``compute_boundary_zonal_stats`` over ``collection`` could
    write a row: it computes some boundary level, and boundaries of one of
    those levels are loaded. Asked once by a writer before it enqueues.
    """
    from adminboundarymanager.models import AdminBoundary

    levels = getattr(collection, "boundary_stats_levels", None)
    return bool(levels) and AdminBoundary.objects.filter(level__in=levels).exists()


def enqueue_boundary_stats(asset_id: int) -> None:
    """
    Enqueue ``compute_boundary_zonal_stats`` for one asset once the caller's
    writes commit, having asked :func:`computes_boundary_stats` first. Used by
    the after-save hook of a materialized COG (#715) and, through
    :func:`enqueue_harvested_boundary_stats`, by a harvest of Remote assets
    (#639); a worker that ran before the commit would not find the asset.

    Enqueue failure is logged, never raised: the writer must not fail over
    bookkeeping. A dropped run is re-issued by ``sweep_missing_boundary_stats``
    within its recency window, and for a harvested asset by the next harvest
    (:func:`enqueue_lost_boundary_stats`), however old.
    """
    from django.db import transaction

    def dispatch():
        try:
            compute_boundary_zonal_stats.delay(asset_id)
        except Exception as exc:
            logger.warning("enqueue_boundary_stats: enqueue failed for asset %d: %s", asset_id, exc)

    transaction.on_commit(dispatch)


#: How long a harvest leaves an asset it enqueued before its recovery may
#: enqueue it again (#716): long enough for a deep bookkeeping backlog to drain,
#: short enough that a lost run is not lost for long. An upstream file that is
#: dead for good costs its bounded retries once per period, not once per harvest.
HARVEST_STATS_CLAIM = timedelta(hours=6)

#: How many missing assets one recovery reads, newest first, looking for unclaimed
#: ones to enqueue. The database is asked for at most this many — its cursors are
#: not server-side behind PgBouncer — so more claimed ones than this, say a whole
#: dead upstream, can hold the older ones back until their claims lapse.
HARVEST_RECOVERY_SCAN = 5000


def harvest_claim_key(asset_id: int) -> str:
    """The cache key a harvest claims an asset under. Its own, not the sweep's:
    a harvest's claim must not hold the sweep back from a recent asset."""
    return f"zonal-stats-harvest-claim:{asset_id}"


def enqueue_harvested_boundary_stats(asset_id: int) -> None:
    """
    Enqueue the statistics of a Remote asset a harvest has just created or
    re-synced, claiming it for ``HARVEST_STATS_CLAIM`` — afresh, though a claim
    is there: the asset was just written. The claim keeps the harvest's own
    recovery from taking a run still waiting in the queue for a lost one. It is
    taken before the commit, so a write that rolls back holds an asset no
    longer there, harmlessly, for one claim period.
    """
    _hold_claim(harvest_claim_key(asset_id), HARVEST_STATS_CLAIM)
    enqueue_boundary_stats(asset_id)


def enqueue_lost_boundary_stats(collection, assets, *, window) -> int:
    """
    Enqueue again the assets of ``assets`` with no ``BoundaryZonalStats`` row at
    one of ``collection``'s loaded levels — a run that was lost: never enqueued,
    dropped by the broker, or out of retries (#716). Returns how many.

    For a harvest, which calls it with the Remote COGs of its Jurisdiction in
    its ``window`` (``(start, end)``, either end open), so the work is bounded
    by the window and not by an item's age: a harvested archive is mostly past
    the sweep's reach. An asset claimed within ``HARVEST_STATS_CLAIM`` is
    skipped, and at most ``MISSING_STATS_BATCH`` are enqueued per call, so a
    deep backlog drains over several harvests.
    """
    levels = loaded_levels(collection)
    if not levels:
        return 0

    start, end = window
    missing = _missing_a_level(assets, levels, since=start, until=end)
    sent = 0
    for asset_id in missing.order_by("-item__time", "pk").values_list("pk", flat=True)[:HARVEST_RECOVERY_SCAN]:
        if sent >= MISSING_STATS_BATCH:
            break
        if not _try_claim(harvest_claim_key(asset_id), HARVEST_STATS_CLAIM):
            continue
        enqueue_boundary_stats(asset_id)
        sent += 1
    if sent:
        logger.info("enqueue_lost_boundary_stats: re-enqueued %d run(s) for collection %s", sent, collection.slug)
    return sent


def _missing_a_level(assets, levels, *, since=None, until=None):
    """
    The assets of ``assets`` missing a ``BoundaryZonalStats`` row at any of
    ``levels`` — a run that died between levels leaves the earlier ones
    written. ``since`` and ``until`` bound the rows' ``time`` (an item's), which
    keeps the hypertable scan to the chunks that can hold them.
    """
    from django.db.models import Exists, OuterRef, Q

    from .models import BoundaryZonalStats

    # A row's time is its item's (persist_stats): matching it lets the unique
    # index, which leads with time, answer each probe in one chunk.
    rows = BoundaryZonalStats.objects.filter(
        time=OuterRef("item__time"), item=OuterRef("item"), variable=OuterRef("variable")
    )
    if since is not None:
        rows = rows.filter(time__gte=since)
    if until is not None:
        rows = rows.filter(time__lte=until)

    missing = Q()
    for level in levels:
        missing |= Q(pk__in=assets.exclude(Exists(rows.filter(boundary__level=level))).values("pk"))
    return assets.filter(missing)


@app.task(
    name="georiva.analysis.zonal_stats.tasks.sweep_stale_boundary_stats",
    base=EntryTask,
    queue="georiva-default",
)
def sweep_stale_boundary_stats() -> None:
    """
    Periodic cleanup for forecast boundary stats.

    Mirrors Collection forecast pruning logic:
      - retain_past_forecasts=False  → delete stats where valid_time < now
      - retain_latest_run_only=True  → delete stats not in the latest run

    Runs every 5 minutes via django-celery-beat.
    """
    from django.utils import timezone

    from georiva.core.models import Collection

    from .models import BoundaryZonalStats
    from .service import retained_items_q

    now = timezone.now()
    pruned = 0

    for collection in Collection.objects.filter(is_forecast=True, is_active=True):
        latest_ref = (
            BoundaryZonalStats.objects.filter(item__collection=collection)
            .order_by("-item__reference_time")
            .values_list("item__reference_time", flat=True)
            .first()
        )
        kept = retained_items_q(collection, now, latest_ref)
        if kept is None:
            continue
        deleted, _ = BoundaryZonalStats.objects.filter(item__collection=collection).exclude(kept).delete()
        pruned += deleted

    if pruned:
        logger.info("sweep_stale_boundary_stats: pruned %d stale row(s)", pruned)


#: How far back the recovery sweep looks.  Bounds the NOT EXISTS scan.  A
#: harvested asset older than this is recovered by its feed's next harvest
#: (``enqueue_lost_boundary_stats``, #716); any other older gap is the
#: ``compute_boundary_stats`` command's.
MISSING_STATS_WINDOW = timedelta(days=7)

#: An asset younger than this still has its own after-save run in flight.
MISSING_STATS_GRACE = timedelta(minutes=15)

#: How long a re-dispatched asset is left alone before the sweep may queue it again.
MISSING_STATS_CLAIM = timedelta(minutes=30)

#: Dispatch ceiling per pass, so a large outage drains over several passes
#: instead of flooding georiva-bookkeeping at once.
MISSING_STATS_BATCH = 500


@app.task(
    name="georiva.analysis.zonal_stats.tasks.sweep_missing_boundary_stats",
    base=EntryTask,
    queue="georiva-default",
)
def sweep_missing_boundary_stats() -> None:
    """
    Recovery for a dropped ``compute_boundary_zonal_stats`` run (#402).

    The absence of ``BoundaryZonalStats`` rows for a COG asset is the signal:
    any asset in the recency window, past the grace period, with no row at one
    of its collection's configured levels is re-dispatched.  Every boundary gets
    a row (all-None when nothing intersects), so a finished run never looks
    missing.  ``persist_stats`` upserts, so a redundant run is harmless; a
    per-asset cache claim still keeps a backed-up queue from receiving the same
    asset on every pass.

    Retention comes from ``retained_items_q``, the rule the pruner deletes by.

    Only the last ``MISSING_STATS_WINDOW`` of items is looked at. A harvested
    asset older than that is recovered by its feed's next harvest
    (``enqueue_lost_boundary_stats``, #716), under a claim of its own, so
    neither recovery holds the other back.

    Runs every 5 minutes via django-celery-beat.
    """
    from django.utils import timezone

    from georiva.core.models import Asset, Collection

    from .service import retained_items_q

    now = timezone.now()
    since = now - MISSING_STATS_WINDOW
    settled = now - MISSING_STATS_GRACE
    dispatched = 0

    collections = Collection.objects.filter(is_active=True, boundary_stats_levels__isnull=False).exclude(
        boundary_stats_levels=[]
    )

    for collection in collections:
        if dispatched >= MISSING_STATS_BATCH:
            break

        # Levels with no boundaries loaded can never produce a row; without
        # this every pass would re-dispatch the same futile runs.
        levels = loaded_levels(collection)
        if not levels:
            continue

        # COGs only, which leaves Store assets out: zonal statistics are not
        # computed over an upstream store (ADR 0044, out of scope).
        assets = Asset.objects.filter(
            item__collection=collection,
            format=Asset.Format.COG,
            item__time__gte=since,
            created__lte=settled,
        )

        assets = _missing_a_level(assets, levels, since=since)

        latest_ref = (
            collection.items.order_by("-reference_time").values_list("reference_time", flat=True).first()
            if collection.is_forecast
            else None
        )
        kept = retained_items_q(collection, now, latest_ref)
        if kept is not None:
            assets = assets.filter(kept)

        room = MISSING_STATS_BATCH - dispatched
        for asset_id in assets.order_by("-item__time", "pk").values_list("pk", flat=True)[:room]:
            if not _claim_redispatch(asset_id):
                continue
            try:
                compute_boundary_zonal_stats.delay(asset_id)
                dispatched += 1
            except Exception as exc:
                logger.warning("sweep_missing_boundary_stats: enqueue failed for asset %d: %s", asset_id, exc)

    if dispatched:
        logger.info("sweep_missing_boundary_stats: re-dispatched %d run(s)", dispatched)


def _claim_redispatch(asset_id: int) -> bool:
    """
    True if no sweep has re-dispatched this asset within ``MISSING_STATS_CLAIM``.

    Without it, a deep ``georiva-bookkeeping`` backlog would receive the same
    asset every 5 minutes.
    """
    return _try_claim(f"zonal-stats-redispatch:{asset_id}", MISSING_STATS_CLAIM)


def _try_claim(key: str, period: timedelta) -> bool:
    """Claim ``key`` for ``period`` unless it is claimed already; True if claimed.
    A cache outage fails open: a duplicate run is harmless, a skipped recovery is not."""
    from django.core.cache import cache

    try:
        return cache.add(key, 1, timeout=int(period.total_seconds()))
    except Exception:
        return True


def _hold_claim(key: str, period: timedelta) -> None:
    """Claim ``key`` for ``period`` from now, whatever claim was there."""
    from django.core.cache import cache

    try:
        cache.set(key, 1, timeout=int(period.total_seconds()))
    except Exception:
        logger.warning("zonal stats: could not claim %s", key)


@app.on_after_finalize.connect
def setup_zonal_stats_periodic_tasks(sender, **kwargs) -> None:
    """Register the two zonal-stats sweeps as periodic tasks (every 5 min)."""
    from django_celery_beat.models import IntervalSchedule

    register_periodic_tasks(
        [
            PeriodicSpec(
                f"georiva.analysis.zonal_stats.{name}",
                f"georiva.analysis.zonal_stats.tasks.{name}",
                5,
                IntervalSchedule.MINUTES,
            )
            for name in ("sweep_stale_boundary_stats", "sweep_missing_boundary_stats")
        ]
    )
