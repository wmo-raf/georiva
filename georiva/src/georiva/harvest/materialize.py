"""
Materializing a window: a Harvest feed keeps a hot window local while its deep
archive stays Remote (ADR 0042 §8, #640).

A feed with a materialization window sends the Remote COGs of its
Jurisdiction inside that window to the ingestion queue, one asset per task and
one at a time: its lane (#727). The ingestion queue is a strict FIFO on one
pool process (ADR 0025), so a feed never has more than one materialization in
front of another feed's fresh file. A harvest opens the lane with the newest
asset due, unless the feed's lane is open already; each materialization, once
run — whatever came of it — sends the feed's next asset due, newest first, to
the back of the queue, behind whatever fresh file arrived meanwhile. A deep
window so drains without waiting for the next harvest, and a fresh file waits
behind at most one materialization per feed materializing. The lane closes
when nothing is left to send. Its lease, renewed as each task starts and as
it passes the lane on, outlasts any wait on the queue; one whose task was
lost is open again for the feed's next harvest once its lease lapses, and
that lost task, should it run after all, stands down rather than open a second.

On the ingestion queue each asset is fetched — its href, and any redirect, past the same
Asset host allowlist a harvested href passes (§9) — read north up, and handed
to the shared write step ingestion and derivation end in (`AssetMaterializer`).
That step writes the COG into our storage, rewrites the same Asset row in place
— its href now a key of ours — computes its statistics from the local bytes,
widens the collection's extent and runs the after-save hooks, so the asset gets
zonal statistics and a virtual-Zarr manifest like any ingested one.

The copy holds what the upstream file held: every band, in its own dtype, with
its own nodata value — not re-encoded as ingestion re-encodes a source file,
narrowed to band 1 with nodata as NaN (#725). A file declaring no nodata is
given none. Its statistics are band 1's, the band its Variable maps and every
reader draws, without the nodata value.

Nor is it clipped to the catalog's boundary, as an ingested file is: its Remote
neighbours are served as the Upstream publishes them, and a Mixed collection
shows one footprint across the boundary between the two (#726). A `display`
catalog crops both alike, on the map only (ADR 0038). The item keeps the
Upstream's bbox for its bounds: the copy is the very file that bbox described.

The rewritten href fails the Jurisdiction's "still Remote" test, so the asset
has left the Mirror for good: no later harvest removes or rewrites it, even
once the Upstream has removed its item, and moving the window later leaves it
local. Its Provenance stays, as history. The collection is then a Mixed
collection, and nothing downstream tells its two kinds of asset apart beyond
the href (ADR 0043 §2).

The write is taken under a lock on the row, still Remote and still the feed's:
a Mirror judging the row at the same time either removes it first — and the
materialization finds it gone and writes nothing — or waits and finds it local.

The COG is written to storage before the row is locked, so the Mirror never
waits on an upload. The upstream's checksum and metadata describe its file, not
ours: the row keeps them under `upstream` in `extra_fields`, as history.

Nothing here is retried by Celery. What fails stays Remote and stays due: a
later run of the feed sends it again once its claim has lapsed. An asset whose
href the allowlist refused is marked so, and not sent again until its href or
the feed's allowlist changes. A refusal of where its host resolves now —
privately, or not at all — says nothing of the href: that asset is sent again,
its claim doubling with each such refusal in a row, up to a day (#724). The
feed's page counts both kinds in its window, by reason (`refusals`).
"""

from __future__ import annotations

import logging
import tempfile
import uuid
from dataclasses import dataclass
from datetime import timedelta
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

from django.conf import settings
from django.db import transaction
from django.db.models import BooleanField, Case, Count, F, IntegerField, Q, Value, When
from django.db.models.expressions import RawSQL
from django.db.models.fields.json import KeyTextTransform, KeyTransform
from django.db.models.functions import Cast

from georiva.core.storage.asset_location import remote_q

from .asset_hosts import AssetHostAllowlist
from .extra_fields import rewritten
from .upstream import UpstreamUnreachable

if TYPE_CHECKING:
    import numpy

logger = logging.getLogger(__name__)

#: What one materialization came to.
MATERIALIZED = "materialized"
#: The asset is no longer one to materialize: gone, already local, or not the feed's.
LEFT = "left"
#: The asset is still Remote, and no longer inside its feed's materialization window.
NOT_DUE = "not due"
#: The asset's href no longer passes its feed's Asset host allowlist.
REFUSED = "refused"
#: The Upstream could not give the asset's bytes, or they were not a raster.
UNREACHABLE = "unreachable"

#: How long an asset sent to the ingestion queue is not sent again: long
#: enough for a busy queue to reach it, short enough that one whose bytes could
#: not be had is tried again the same day.
MATERIALIZE_CLAIM = timedelta(hours=2)

#: The longest an asset is claimed after its host was refused for where it
#: resolved, time after time: it is still tried once a day.
MATERIALIZE_CLAIM_LONGEST = timedelta(days=1)

#: How long a feed's lane stays open without word from the task in it, which
#: renews it as it starts and as it passes it on. Longer than any wait behind
#: a busy ingestion queue: a lane lapsing while its task still waited would let
#: each harvest open another. What it costs is how long a feed whose task was
#: lost waits before its next harvest opens the lane again.
MATERIALIZE_LANE_LEASE = timedelta(days=1)

#: How many due assets are looked through for an unclaimed one to send.
MATERIALIZE_SCAN = 2000


#: Where a Remote asset whose href its feed's allowlist refused keeps that
#: verdict, in `extra_fields`: with the href and the allowlist it was judged
#: by, so it is asked again only once either has changed, and why.
REFUSED_MARK = "materialize_refused"

#: Where a Remote asset whose host was refused for where it resolved keeps,
#: in `extra_fields`, the href it was judged at, why, and how many times in a
#: row: it is sent again all the same, each claim twice as long as the last (#724).
UNRESOLVED_MARK = "materialize_unresolved"

#: A row holds at most one of the two: the latest verdict on it.
_MARKS = (REFUSED_MARK, UNRESOLVED_MARK)

#: Where a Materialized asset keeps, in `extra_fields`, what the Upstream said
#: of the file it was copied from: its href, checksum, nodata and `updated`.
UPSTREAM_HISTORY = "upstream"


def claim_key(asset_id: int) -> str:
    return f"harvest-materialize-claim:{asset_id}"


def lane_key(feed_id: int | str) -> str:
    return f"harvest-materialize-lane:{feed_id}"


def claim_period(times_unresolved: int) -> timedelta:
    """How long an asset sent now is not sent again: `MATERIALIZE_CLAIM`,
    doubled for each time in a row its host was refused for where it resolved,
    up to `MATERIALIZE_CLAIM_LONGEST`."""
    # The doubling is cut short well past the cap: a timedelta overflows long before 2 ** times does.
    return min(MATERIALIZE_CLAIM * 2 ** min(times_unresolved, 16), MATERIALIZE_CLAIM_LONGEST)


def due(feed, collection):
    """The assets `feed` should make local and has not: its Jurisdiction's
    Remote COGs in `collection` whose item is inside the materialization window,
    newest first."""
    from georiva.core.models import Asset

    start, end = feed.materialization_window
    in_window = Q()
    if start is not None:
        in_window &= Q(item__time__gte=start)
    if end is not None:
        in_window &= Q(item__time__lte=end)
    return Asset.objects.filter(
        remote_q(),
        in_window,
        harvested_by_id=feed.pk,
        item__collection=collection,
        item__reference_time__isnull=True,
        format=Asset.Format.COG,
    ).order_by("-item__time", "-pk")


def enqueue_due(feed, collection) -> int:
    """Open `feed`'s lane with the newest asset it should make local and has
    not, sent to the ingestion queue as the caller's writes commit; say how
    many were sent — none while its lane is open already, or nothing is due."""
    if not feed.materializes:
        return 0
    lane = Lane.open(feed.pk)
    if lane is None:
        return 0
    try:
        sent = _send_next(feed, collection, lane)
    except Exception:
        # Not left open for a day with nothing in it.
        lane.close()
        raise
    if not sent:
        lane.close()
        return 0
    logger.info("Harvest %s: its local window is being fetched, newest first", feed.name)
    return 1


def materialize_in_lane(asset_id: int, lane: str | None, *, upstream=None, writer=None) -> str:
    """Materialize one asset, then pass its feed's `lane` on to the next asset
    due — whatever came of this one. The lane is renewed as the task starts,
    so its lease never ran out on the queue's account; a task whose lane is no
    longer its feed's — or that was sent without one — passes nothing on."""
    current = Lane.parse(lane) if lane else None
    passes_on = current is not None and current.renew()
    try:
        return materialize(asset_id, upstream=upstream, writer=writer)
    finally:
        if passes_on:
            try:
                _pass_on(current)
            except Exception:
                logger.exception("Materialization: lane %s could not be passed on; the next harvest opens it", current)


@dataclass(frozen=True)
class Lane:
    """A feed's one place on the ingestion queue (#727): the feed, and which
    opening of its lane. A lane opened again once its lease lapsed is another,
    and a task still carrying the old one stands down.

    Held in Redis under `lane_key`. Renewing and closing compare and write in
    one step, so a task holding a lapsed lane can neither take over nor close
    the one opened since. Sent with a task as its `str`."""

    feed_id: int
    token: str

    @classmethod
    def open(cls, feed_id: int) -> Lane | None:
        """The feed's lane, newly opened — or None while it is open already. A
        cache outage fails open: each harvest then sends one asset, and nothing
        passes it on, since the lane cannot be renewed."""
        lane = cls(int(feed_id), uuid.uuid4().hex)
        try:
            opened = _redis().set(lane.key, lane.token, nx=True, ex=_lease())
        except Exception:
            logger.warning("Materialization: lane of feed %s not held — the cache could not be reached", feed_id)
            return lane
        return lane if opened else None

    @classmethod
    def parse(cls, sent: str) -> Lane:
        feed_id, token = sent.split(":", 1)
        return cls(int(feed_id), token)

    def __str__(self) -> str:
        return f"{self.feed_id}:{self.token}"

    @property
    def key(self) -> str:
        return lane_key(self.feed_id)

    def renew(self) -> bool:
        """Give the lane a full lease again, if it is still the feed's; say whether it was."""
        return self._while_current(_RENEW_LANE, _lease())

    def close(self) -> None:
        """Close the lane, if it is still the feed's."""
        self._while_current(_CLOSE_LANE)

    def _while_current(self, script: str, *args) -> bool:
        try:
            return bool(_redis().eval(script, 1, self.key, self.token, *args))
        except Exception:
            logger.warning("Materialization: lane %s — the cache could not be reached", self)
            return False


#: Set the key's lease again while it still holds the token: 1 if it did.
_RENEW_LANE = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    redis.call('set', KEYS[1], ARGV[1], 'EX', ARGV[2])
    return 1
end
return 0
"""

#: Delete the key while it still holds the token: 1 if it did.
_CLOSE_LANE = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


def _redis():
    from django_redis import get_redis_connection

    return get_redis_connection("default")


def _lease() -> int:
    return int(MATERIALIZE_LANE_LEASE.total_seconds())


def _send_next(feed, collection, lane: Lane) -> bool:
    """Send the newest asset `feed` should make local, in `lane`: not one sent
    within its claim period, nor one its allowlist refused until its href or
    the allowlist changes. False when there is none."""
    from .tasks import materialize_harvested_asset

    # Refused, and nothing it was refused by has changed since: not asked again.
    unrefused = _verdicts(due(feed, collection), feed).filter(href_refused=False).annotate(times=F("times_unresolved"))
    for asset_id, times in unrefused.values_list("pk", "times")[:MATERIALIZE_SCAN]:
        if _try_claim(claim_key(asset_id), claim_period(times)):
            transaction.on_commit(partial(materialize_harvested_asset.delay, asset_id, str(lane)))
            return True
    return False


def _pass_on(lane: Lane) -> None:
    """Send the lane's feed's next asset due in it, while the lane is still
    the feed's; close it when nothing is."""
    from .models import HarvestFeed

    feed = HarvestFeed.objects.filter(pk=lane.feed_id).first()
    collection = feed.collection if feed is not None and feed.materializes else None
    if collection is not None and lane.renew() and _send_next(feed, collection, lane):
        return
    lane.close()


def materialize(asset_id: int, *, upstream=None, writer=None) -> str:
    """
    Make one Remote asset local: fetch its bytes and ingest them in place.
    Returns what came of it — one of `MATERIALIZED`, `LEFT`, `NOT_DUE`,
    `REFUSED` or `UNREACHABLE`.

    `upstream` is the Upstream catalog client, the feed's own when not given;
    `writer` the COG writer, our assets bucket's when not given.
    """
    from georiva.core.models import Asset

    from .models import HarvestFeed

    asset = (
        Asset.objects.select_related("item__collection__catalog__organisation", "variable").filter(pk=asset_id).first()
    )
    if asset is None or asset.harvested_by_id is None or not asset.location.is_remote:
        return LEFT
    feed = HarvestFeed.objects.filter(pk=asset.harvested_by_id).first()
    if feed is None:
        # Its feed is gone: the row is nobody's (ADR 0042, consequences).
        return LEFT
    item = asset.item
    if asset.format != Asset.Format.COG or item.reference_time is not None or not feed.materializes_at(item.time):
        return NOT_DUE

    upstream = upstream or feed.upstream_client()
    allowlist = AssetHostAllowlist(feed.allowed_asset_hosts, allow_http=feed.allow_http, resolve=upstream.resolve_host)
    refusal = allowlist.href_refusal(asset.href)
    if refusal:
        logger.warning("Harvest %s: asset %s not kept locally — %s", feed.name, asset.pk, refusal)
        _mark_refused(asset, feed, refusal)
        return REFUSED
    refusal = allowlist.address_refusal(asset.href)
    if refusal:
        # Where the host resolves now, not what the href is: sent again later.
        logger.warning("Harvest %s: asset %s not kept locally — %s; it is tried again", feed.name, asset.pk, refusal)
        _mark_unresolved(asset, feed, refusal)
        return REFUSED
    if UNRESOLVED_MARK in (asset.extra_fields or {}):
        # Its host resolves publicly again: a later refusal starts the count over.
        _clear_unresolved(asset, feed)

    with tempfile.TemporaryDirectory(prefix="georiva_materialize_", dir=settings.GEORIVA_TEMP_DIR) as folder:
        path = Path(folder) / "asset.tif"
        try:
            upstream.fetch_asset(asset.href, path, addresses=allowlist.addresses)
            raster = _read(path)
        except UpstreamUnreachable as unreachable:
            logger.warning("Harvest %s: asset %s not kept locally — %s", feed.name, asset.pk, unreachable)
            return UNREACHABLE
        except Exception as unreadable:
            logger.warning("Harvest %s: asset %s not kept locally — unreadable: %s", feed.name, asset.pk, unreadable)
            return UNREACHABLE

    return MATERIALIZED if _write(asset, feed, raster, writer) else LEFT


@dataclass(frozen=True)
class _Raster:
    """An upstream file as read: in its own dtype, north up — a grid for a
    one-band file, else its bands first — and the nodata value it declares, or None."""

    data: numpy.ndarray
    nodata: float | None
    bounds: tuple[float, float, float, float]
    crs: str


def _read(path: Path) -> _Raster:
    """The file's every band as it holds them, north up."""
    import numpy
    import rasterio

    from georiva.formats.geotiff import GeoTIFFFormatPlugin

    with rasterio.open(path) as src:
        data = src.read()
        bounds, _, crs, needs_flip = GeoTIFFFormatPlugin._spatial_from_rasterio(src, None)
        nodata = src.nodata
    if needs_flip:
        data = numpy.flip(data, axis=1)
    if len(data) == 1:
        data = data[0]
    return _Raster(data=data, nodata=nodata, bounds=bounds, crs=crs)


def _write(asset, feed, raster: _Raster, writer) -> bool:
    """Ingest `raster` as `asset`, if it is still the feed's and still
    Remote; False when it has left the Jurisdiction meanwhile.

    The COG is written to storage first, and only then is the row locked and
    rewritten: a Mirror judging the row waits on a row update, never on an
    upload. An object written for a row that went meanwhile is removed again,
    unless some asset already names it."""
    from georiva.core.models import Asset
    from georiva.core.storage import storage
    from georiva.core.storage.asset_deletion import drop_unrecorded
    from georiva.ingestion.asset_writer import AssetWriter
    from georiva.ingestion.materialization import AssetMaterializer

    item = asset.item
    materializer = AssetMaterializer(writer or AssetWriter(storage.assets))
    # Not clipped to the catalog's boundary: its Remote neighbours are served
    # as the Upstream publishes them, and so is it (#726).
    written = materializer.write_cog(
        item=item,
        variable=asset.variable,
        data=raster.data,
        bounds=raster.bounds,
        crs=raster.crs,
        timestamp=item.time,
        nodata=raster.nodata,
    )

    with transaction.atomic():
        upstream = (
            Asset.objects.filter(remote_q(), pk=asset.pk, harvested_by_id=feed.pk)
            .select_for_update(of=("self",))
            .values("href", "checksum", "extra_fields")
            .first()
        )
        if upstream is not None:
            # The same row: `update_or_create` on (item, Variable, COG) finds it,
            # and rewrites only what ingestion writes — its Provenance is not among them.
            (local,) = materializer.record_cog(item=item, variable=asset.variable, written=written, timestamp=item.time)
            # The upstream's checksum and metadata describe its file, not ours: kept as history only.
            Asset.objects.filter(pk=local.pk).update(
                checksum="",
                extra_fields={**local.extra_fields, UPSTREAM_HISTORY: _history(upstream)},
            )
    if upstream is None:
        drop_unrecorded(materializer.writer.bucket, written.href)
        return False
    logger.info("Harvest %s: asset %s is kept locally from now on", feed.name, asset.pk)
    return True


def _history(upstream: dict) -> dict:
    """What the Upstream said of the file a Materialized asset was copied from."""
    from .harvester import UPSTREAM_UPDATED

    extra = upstream["extra_fields"] or {}
    said = {
        "href": upstream["href"],
        "checksum": upstream["checksum"],
        "nodata": extra.get("nodata"),
        "updated": extra.get(UPSTREAM_UPDATED),
    }
    return {key: value for key, value in said.items() if value not in (None, "")}


def _judged_by(feed) -> dict:
    """The feed's allowlist as a refusal of an href was judged by it: with the
    href, what `REFUSED_MARK` is asked again only once it has changed."""
    return {"hosts": sorted(feed.allowed_asset_hosts), "allow_http": feed.allow_http}


def _verdicts(rows, feed):
    """`rows`, each with the verdicts on it that still hold, as aliases:
    `href_refused` — its href was refused, and neither the href nor the feed's
    allowlist has changed since; `times_unresolved` — how many times in a row
    its host was refused for where it resolved, at the href it has now, or 0."""
    refused = KeyTransform(REFUSED_MARK, "extra_fields")
    unresolved = KeyTransform(UNRESOLVED_MARK, "extra_fields")
    still_judged_by = {f"extra_fields__{REFUSED_MARK}__{key}": value for key, value in _judged_by(feed).items()}
    return rows.alias(
        refused_href=KeyTextTransform("href", refused),
        unresolved_href=KeyTextTransform("href", unresolved),
    ).alias(
        href_refused=Case(
            When(Q(refused_href=F("href"), **still_judged_by), then=Value(True)),
            default=Value(False),
            output_field=BooleanField(),
        ),
        times_unresolved=Case(
            When(unresolved_href=F("href"), then=Cast(KeyTextTransform("times", unresolved), IntegerField())),
            default=Value(0),
            output_field=IntegerField(),
        ),
    )


def _mark_refused(asset, feed, reason: str) -> None:
    """Note on the row that its href was refused, so it is not sent again for
    nothing, with what it was judged by and why."""
    verdict = {"href": asset.href, **_judged_by(feed), "reason": reason}
    _rewrite_marks(asset, feed, rewritten(drop=_MARKS, put={REFUSED_MARK: verdict}))


def _mark_unresolved(asset, feed, reason: str) -> None:
    """Note on the row that its host was refused for where it resolved, why,
    and how many times in a row at this href. The count is the database's, read
    at the write: two refusals at once both count."""
    _rewrite_marks(
        asset,
        feed,
        RawSQL(
            "(extra_fields - %s::text[]) || jsonb_build_object(%s::text, jsonb_build_object("
            "'href', href, 'reason', %s::text, 'times', 1 + CASE WHEN extra_fields -> %s::text ->> 'href' = href "
            "THEN COALESCE((extra_fields -> %s::text ->> 'times')::int, 0) ELSE 0 END))",
            (list(_MARKS), UNRESOLVED_MARK, reason, UNRESOLVED_MARK, UNRESOLVED_MARK),
        ),
    )


def _clear_unresolved(asset, feed) -> None:
    """Forget that the row's host was refused for where it resolved."""
    _rewrite_marks(asset, feed, rewritten(drop=(UNRESOLVED_MARK,)))


def _rewrite_marks(asset, feed, extra_fields) -> None:
    """Write `extra_fields`, an expression over the row's own, only while it is
    the row that was judged: still the feed's, still Remote, and at the same
    href. Not a save: nothing a reader sees changed."""
    from georiva.core.models import Asset

    Asset.objects.filter(remote_q(), pk=asset.pk, harvested_by_id=feed.pk, href=asset.href).update(
        extra_fields=extra_fields
    )


@dataclass(frozen=True)
class Refusal:
    """How many assets of a feed's materialization window are not kept locally
    for one reason; `retried` when it is where their host resolves, which a
    later harvest asks again."""

    reason: str
    count: int
    retried: bool


def refusals(feed, collection) -> list[Refusal]:
    """What in `feed`'s materialization window the allowlist refused, by
    reason, most first. A refusal of an href, or by an allowlist, that has
    changed since is not counted: it is sent again."""
    if not feed.materializes or collection is None:
        return []
    judged = _verdicts(due(feed, collection).order_by(), feed)
    by_href = (
        judged.filter(href_refused=True)
        .values(why=KeyTextTransform("reason", KeyTransform(REFUSED_MARK, "extra_fields")))
        .annotate(count=Count("pk"))
    )
    by_address = (
        judged.filter(times_unresolved__gt=0)
        .values(why=KeyTextTransform("reason", KeyTransform(UNRESOLVED_MARK, "extra_fields")))
        .annotate(count=Count("pk"))
    )
    found = [
        # A mark made before refusals kept their reason says only that it was refused.
        Refusal(row["why"] or "refused by the asset host allowlist", row["count"], retried=False)
        for row in by_href
    ] + [Refusal(row["why"], row["count"], retried=True) for row in by_address]
    return sorted(found, key=lambda refusal: (-refusal.count, refusal.reason))


def _try_claim(key: str, period: timedelta = MATERIALIZE_CLAIM) -> bool:
    """Claim `key` for `period` unless it is claimed already. A cache outage
    fails open: a second materialization of one asset finds it local and
    fetches nothing."""
    from django.core.cache import cache

    try:
        return cache.add(key, 1, timeout=int(period.total_seconds()))
    except Exception:
        return True
