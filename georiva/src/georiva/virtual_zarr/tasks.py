import logging
import os
import tempfile

import pandas as pd
import pytz
from django.db.models import Prefetch
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from georiva.config.celery import app
from georiva.core.models import Asset, Item
from georiva.core.storage import storage
from georiva.virtual_zarr.virtual_zarr import MinioStoreConfig, VirtualZarrBuilder
from .models import VirtualZarrManifest

logger = logging.getLogger(__name__)


@app.task(
    name="georiva.virtual_zarr.tasks.build_virtual_zarr_manifest",
    bind=True,
    max_retries=0,  # failures go to FAILED status; sweep retries
    acks_late=True,
    queue="georiva-ingestion",
)
def build_virtual_zarr_manifest(self, manifest_id: int) -> None:
    """
    Build or rebuild the kerchunk manifest for one (Collection, Variable) pair.

    Steps
    -----
    1.  Load the VirtualZarrManifest record and lock it (BUILDING).
    2.  Query COG Asset hrefs for this (collection, variable) ordered by
        item__time
    3.  Build a url_df DataFrame from the asset hrefs.
    4.  Run VirtualZarrBuilder.build() → writes manifest JSON to a temp file.
    5.  Upload the manifest JSON to MinIO (georiva-assets bucket) under the
        canonical __manifests__/ key.
    6.  Mark the manifest READY and update coverage fields.

    On any exception the manifest is marked FAILED with the error message.
    The sweep will re-dispatch after the next 5-minute interval.
    """
    try:
        manifest = VirtualZarrManifest.objects.select_related(
            "variable",
            "variable__collection",
            "variable__collection__catalog",
        ).get(pk=manifest_id)
    except VirtualZarrManifest.DoesNotExist:
        logger.error("build_virtual_zarr_manifest: manifest %d not found", manifest_id)
        return
    
    worker_id = f"celery-{self.request.id or 'unknown'}"
    manifest.mark_building(worker_id)
    
    col = manifest.variable.collection
    logger.info(
        "build_virtual_zarr_manifest: starting %s/%s/%s",
        col.catalog.slug,
        col.slug,
        manifest.variable.slug,
    )
    
    try:
        _run_build(manifest)
    except Exception as exc:
        logger.exception(
            "build_virtual_zarr_manifest: failed for manifest %d", manifest_id
        )
        manifest.mark_failed(str(exc))


def _run_build(manifest: VirtualZarrManifest) -> None:
    """
    Core build logic.

    Query strategy: Item-first via the TimescaleDB hypertable.
    """
    
    variable = manifest.variable
    collection = variable.collection  # derived — no separate FK needed
    
    config = MinioStoreConfig.from_django_settings()
    
    # ------------------------------------------------------------------
    # 1. Collect Items from the hypertable, prefetch COG asset hrefs
    # ------------------------------------------------------------------
    cog_prefetch = Prefetch(
        "assets",
        queryset=Asset.objects.filter(
            variable=variable,
            format=Asset.Format.COG,
        ).only("href", "item_id"),  # only columns we need
        to_attr="cog_assets",
    )
    
    items_qs = (
        Item.objects
        .filter(collection=collection)
        .prefetch_related(cog_prefetch)
        .only("time")
        .order_by("time")
    )
    
    rows = []
    skipped = 0
    for item in items_qs.iterator(chunk_size=500):
        if not item.cog_assets:
            # COG missing for this timestep (ingestion gap or partial failure)
            skipped += 1
            continue
        rows.append({
            "date": pd.Timestamp(item.time),
            "url": config.url_for(item.cog_assets[0].href),
        })
    
    if skipped:
        logger.warning(
            "build_virtual_zarr_manifest: %d item(s) skipped (no COG asset) "
            "for %s/%s",
            skipped, collection.slug, variable.slug,
        )
    
    if not rows:
        raise ValueError(
            f"No COG assets found for {collection}/{variable.slug}. "
            "Ingest data before building the manifest."
        )
    
    url_df = pd.DataFrame(rows)
    item_count = len(url_df)
    
    logger.info(
        "build_virtual_zarr_manifest: %d COG asset(s) for %s/%s"
        " (%d item(s) skipped)",
        item_count, collection.slug, variable.slug, skipped,
    )
    
    # ------------------------------------------------------------------
    # 2. Build the virtual dataset → write manifest JSON to a temp file
    # ------------------------------------------------------------------
    builder = VirtualZarrBuilder(config)
    
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        tmp_path = tmp.name
    
    try:
        builder.build(
            url_df=url_df,
            output_path=tmp_path,
            variable_name=variable.slug,
        )
        
        # ------------------------------------------------------------------
        # 3. Upload the manifest JSON to MinIO
        # ------------------------------------------------------------------
        manifest_key = manifest.get_manifest_path()
        
        with open(tmp_path, "rb") as f:
            manifest_bytes = f.read()
        
        storage.zarr.save(manifest_key, manifest_bytes)
        
        logger.info(
            "build_virtual_zarr_manifest: uploaded manifest → %s (%d bytes)",
            manifest_key, len(manifest_bytes),
        )
    
    finally:
        os.unlink(tmp_path)
    
    # ------------------------------------------------------------------
    # 4. Mark READY and record coverage
    # ------------------------------------------------------------------
    
    time_start = url_df["date"].min().to_pydatetime()
    time_end = url_df["date"].max().to_pydatetime()
    
    # Ensure timezone-aware UTC
    if time_start.tzinfo is None:
        time_start = pytz.utc.localize(time_start)
    if time_end.tzinfo is None:
        time_end = pytz.utc.localize(time_end)
    
    manifest.mark_ready(
        manifest_path=manifest_key,
        item_count=item_count,
        time_start=time_start,
        time_end=time_end,
    )
    
    logger.info(
        "build_virtual_zarr_manifest: READY — %d items, %s → %s",
        item_count, time_start.date(), time_end.date(),
    )


# =============================================================================
# Sweep task
# =============================================================================

@app.task(
    name="georiva.virtual_zarr.tasks.sweep_virtual_zarr_pending",
    queue="georiva-default",
)
def sweep_virtual_zarr_pending() -> None:
    """
    Periodic safety-net for virtual Zarr manifest builds.

    Runs every 5 minutes:
      1. Reset stale BUILDING locks (crash recovery) → PENDING
      2. Dispatch build_virtual_zarr_manifest for every buildable manifest
    """
    reset_count = VirtualZarrManifest.reset_stale_locks()
    if reset_count:
        logger.info("sweep_virtual_zarr_pending: reset %d stale lock(s)", reset_count)
    
    buildable = list(VirtualZarrManifest.get_buildable().values_list("pk", flat=True))
    
    for manifest_id in buildable:
        build_virtual_zarr_manifest.apply_async(
            args=[manifest_id],
            queue="georiva-ingestion",
        )
    
    if buildable:
        logger.info(
            "sweep_virtual_zarr_pending: dispatched %d build task(s)", len(buildable)
        )


@app.on_after_finalize.connect
def setup_virtual_zarr_periodic_tasks(sender, **kwargs) -> None:
    """Register sweep_virtual_zarr_pending as a periodic task (every 5 minutes)."""
    try:
        schedule_5min, _ = IntervalSchedule.objects.get_or_create(
            every=5, period=IntervalSchedule.MINUTES
        )
        PeriodicTask.objects.update_or_create(
            name="georiva.virtual_zarr.sweep_virtual_zarr_pending",
            defaults={
                "task": "georiva.virtual_zarr.tasks.sweep_virtual_zarr_pending",
                "interval": schedule_5min,
                "enabled": True,
            },
        )
    except Exception as exc:
        logger.warning(
            "Could not register virtual Zarr periodic task: %s", exc
        )
