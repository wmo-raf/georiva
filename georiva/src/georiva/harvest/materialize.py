"""
Materializing a window: a Harvest feed keeps a hot window local while its deep
archive stays Remote (ADR 0042 §8, #640).

A feed with a materialization window sends, from each of its runs, the Remote
COGs of its Jurisdiction inside that window to the ingestion queue, one asset
per task. There each is fetched — its href, and any redirect, past the same
Asset host allowlist a harvested href passes (§9) — read as ingestion reads a
GeoTIFF, and handed to the shared write step ingestion and derivation end in
(`AssetMaterializer`). That step writes the COG into our storage, rewrites the
same Asset row in place — its href now a key of ours — computes its statistics
from the local bytes, widens the collection's extent and runs the after-save
hooks, so the asset gets zonal statistics and a virtual-Zarr manifest like any
ingested one.

The rewritten href fails the Jurisdiction's "still Remote" test, so the asset
has left the Mirror for good: no later harvest removes or rewrites it, even
once the Upstream has removed its item, and moving the window later leaves it
local. Its Provenance stays, as history. The collection is then a Mixed
collection, and nothing downstream tells its two kinds of asset apart beyond
the href (ADR 0043 §2).

The write is taken under a lock on the row, still Remote and still the feed's:
a Mirror judging the row at the same time either removes it first — and the
materialization finds it gone and writes nothing — or waits and finds it local.

Nothing here is retried by Celery. What fails stays Remote and stays due: a
later run of the feed sends it again once its claim has lapsed.
"""

from __future__ import annotations

import logging
import tempfile
from datetime import timedelta
from functools import partial
from pathlib import Path

from django.conf import settings
from django.db import transaction
from django.db.models import Q

from georiva.core.storage.asset_location import remote_q

from .asset_hosts import AssetHostAllowlist
from .upstream import UpstreamUnreachable

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

#: How many assets one harvest sends at most, newest first. A deep window
#: drains over several runs rather than flooding the ingestion queue at once.
MATERIALIZE_BATCH = 200

#: How many due assets one harvest looks through for unclaimed ones.
MATERIALIZE_SCAN = 2000


def claim_key(asset_id: int) -> str:
    return f"harvest-materialize-claim:{asset_id}"


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
    """Send the assets `feed` should make local to the ingestion queue, each
    as the caller's writes commit, and say how many were sent. One sent within
    `MATERIALIZE_CLAIM` is not sent again, nor more than `MATERIALIZE_BATCH`."""
    from .tasks import materialize_harvested_asset

    if not feed.materializes:
        return 0
    sent = 0
    for asset_id in due(feed, collection).values_list("pk", flat=True)[:MATERIALIZE_SCAN]:
        if sent >= MATERIALIZE_BATCH:
            break
        if not _try_claim(claim_key(asset_id)):
            continue
        transaction.on_commit(partial(materialize_harvested_asset.delay, asset_id))
        sent += 1
    if sent:
        logger.info("Harvest %s: %d asset(s) sent to be kept locally", feed.name, sent)
    return sent


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
    refusal = allowlist.refusal(asset.href)
    if refusal:
        logger.warning("Harvest %s: asset %s not kept locally — %s", feed.name, asset.pk, refusal)
        return REFUSED

    with tempfile.TemporaryDirectory(prefix="georiva_materialize_", dir=settings.GEORIVA_TEMP_DIR) as folder:
        path = Path(folder) / "asset.tif"
        try:
            upstream.fetch_asset(asset.href, path, admits=lambda url: allowlist.refusal(url) is None)
            extracted = _read(path)
        except UpstreamUnreachable as unreachable:
            logger.warning("Harvest %s: asset %s not kept locally — %s", feed.name, asset.pk, unreachable)
            return UNREACHABLE
        except Exception as unreadable:
            logger.warning("Harvest %s: asset %s not kept locally — unreadable: %s", feed.name, asset.pk, unreadable)
            return UNREACHABLE

    return MATERIALIZED if _write(asset, feed, extracted, writer) else LEFT


def _read(path: Path):
    """The file's first band as ingestion reads a GeoTIFF: nodata as NaN, north up."""
    import numpy

    from georiva.formats.geotiff import GeoTIFFFormatPlugin

    extracted = GeoTIFFFormatPlugin().extract_variable(path, "band_1")
    if extracted.data.dtype == numpy.float64:
        # Widened only to hold NaN where nodata was: the upstream's own precision is kept.
        extracted.data = extracted.data.astype("float32")
    return extracted


def _write(asset, feed, extracted, writer) -> bool:
    """Ingest `extracted` as `asset`, if it is still the feed's and still
    Remote; False when it has left the Jurisdiction meanwhile."""
    from georiva.core.models import Asset
    from georiva.core.storage import storage
    from georiva.ingestion.asset_writer import AssetWriter
    from georiva.ingestion.materialization import AssetMaterializer
    from georiva.processing.engine import catalog_clipper

    item = asset.item
    clipper = catalog_clipper(item.collection)
    materializer = AssetMaterializer(writer or AssetWriter(storage.assets))
    data, bounds = materializer.clip_array(extracted.data, extracted.bounds, clipper)

    with transaction.atomic():
        still_mine = list(
            Asset.objects.filter(remote_q(), pk=asset.pk, harvested_by_id=feed.pk)
            .select_for_update(of=("self",))
            .values_list("pk", flat=True)
        )
        if not still_mine:
            return False
        # The same row: `update_or_create` on (item, Variable, COG) finds it,
        # and rewrites only what ingestion writes — its Provenance is not among them.
        materializer.materialize_variable(
            item=item,
            variable=asset.variable,
            data=data,
            bounds=bounds,
            crs=extracted.crs or "EPSG:4326",
            timestamp=item.time,
            clipper=clipper,
        )
    logger.info("Harvest %s: asset %s is kept locally from now on", feed.name, asset.pk)
    return True


def _try_claim(key: str) -> bool:
    """Claim `key` unless it is claimed already. A cache outage fails open: a
    second materialization of one asset finds it local and fetches nothing."""
    from django.core.cache import cache

    try:
        return cache.add(key, 1, timeout=int(MATERIALIZE_CLAIM.total_seconds()))
    except Exception:
        return True
