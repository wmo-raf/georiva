"""
GeoRiva Zarr Sync Celery Tasks

zarr_sync_store   — serialized per-store writer task (georiva-ingestion queue)
sweep_zarr_pending — periodic safety-net: resets stale locks and re-dispatches
                     stores with pending/failed sync records every 5 minutes
"""

import io
import logging

import rasterio
from django.conf import settings
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from georiva.config.celery import app
from georiva.core.models import Asset
from georiva.core.storage import storage
from .models import ZarrSyncLog
from .writer import ZarrWriter

logger = logging.getLogger(__name__)


@app.task(
    name="georiva.ingestion.zarr_tasks.zarr_sync_store",
    bind=True,
    max_retries=0,
    acks_late=True,
    queue="georiva-ingestion",
)
def zarr_sync_store(self, store_path: str):
    """
    Append all pending timesteps for a single Zarr store.

    Acquires a store-level lock via ZarrSyncLog.acquire_store(), then
    processes every pending (item, variable) record in chronological order:
      1. Read COG bytes from georiva-assets bucket
      2. Open with rasterio → numpy array + affine transform
      3. ZarrWriter.write() → init or append_dim='time'
      4. Create/update Zarr Asset DB record
      5. Mark record COMPLETED (or FAILED on any exception)

    Safe to dispatch multiple times for the same store — duplicate calls exit
    immediately if the store is already locked.

    Known v1 limitation: out-of-order time appends (late data for a past
    timestep) are caught, marked as permanently failed, and logged.
    """
    if not getattr(settings, 'GEORIVA_ZARR_ENABLED', True):
        logger.debug("Zarr sync disabled (GEORIVA_ZARR_ENABLED=False) — skipping %s", store_path)
        return
    
    worker_id = f"celery-{self.request.id or 'unknown'}"
    records = ZarrSyncLog.acquire_store(store_path, worker_id)
    
    if records is None:
        logger.debug("zarr_sync_store: store %s already locked or nothing to do", store_path)
        return
    
    records = list(records)
    if not records:
        logger.debug("zarr_sync_store: no records claimed for %s", store_path)
        return
    
    logger.info("zarr_sync_store: processing %d record(s) for %s", len(records), store_path)
    
    writer = ZarrWriter(
        bucket_name=getattr(settings, 'GEORIVA_ZARR_BUCKET', 'georiva-zarr'),
        endpoint_url=getattr(settings, 'AWS_S3_ENDPOINT_URL', ''),
        aws_key=getattr(settings, 'AWS_ACCESS_KEY_ID', ''),
        aws_secret=getattr(settings, 'AWS_SECRET_ACCESS_KEY', ''),
        time_chunk=getattr(settings, 'GEORIVA_ZARR_TIME_CHUNK', 1),
        storage_backend=getattr(settings, 'GEORIVA_STORAGE_BACKEND', 's3'),
        local_root=getattr(settings, 'GEORIVA_STORAGE_ROOT', ''),
        use_ssl=getattr(settings, 'AWS_S3_USE_SSL', False),
    )
    
    completed = 0
    failed = 0
    
    for record in records:
        item = record.item
        variable = record.variable
        
        try:
            # 1. Locate the COG asset for this (item, variable)
            cog_asset = Asset.objects.get(
                item=item,
                variable=variable,
                format=Asset.Format.COG,
            )
            
            # 2. Read COG bytes and open with rasterio
            cog_bytes = storage.assets.read_bytes(cog_asset.href)
            with rasterio.open(io.BytesIO(cog_bytes)) as src:
                data = src.read(1)  # 2-D numpy array
                transform = src.transform
                crs_obj = src.crs
                crs = crs_obj.to_wkt() if crs_obj else 'EPSG:4326'
            
            # 3. Write to Zarr store (init or append)
            writer.write(
                store_path=store_path,
                data=data,
                transform=transform,
                crs=crs,
                timestamp=item.time,
                variable_slug=variable.slug,
                units=variable.units or '',
            )
            
            # 4. Create or update the Zarr Asset DB record
            Asset.objects.update_or_create(
                item=item,
                variable=variable,
                format=Asset.Format.ZARR,
                defaults={
                    'href': store_path,
                    'media_type': 'application/vnd+zarr',
                    'roles': ['data'],
                    'width': item.width,
                    'height': item.height,
                    'bands': 1,
                    'extra_fields': {
                        'zarr_store': store_path,
                        'zarr_format': 3,
                        'time_chunk': getattr(settings, 'GEORIVA_ZARR_TIME_CHUNK', 1),
                    },
                },
            )
            
            # 5. Mark record completed
            record.mark_completed()
            completed += 1
        
        except Asset.DoesNotExist:
            error = f"COG asset not found for item={item.pk}, variable={variable.slug}"
            logger.error("zarr_sync_store: %s", error)
            record.mark_failed(error)
            failed += 1
        
        except ValueError as exc:
            msg = str(exc)
            logger.warning(
                "zarr_sync_store: non-retryable write error for item=%s in %s: %s",
                item.pk, store_path, msg,
            )
            record.mark_permanently_failed(msg[:2000])
            failed += 1
        
        except Exception as exc:
            logger.exception(
                "zarr_sync_store: unexpected error for item=%s in %s", item.pk, store_path
            )
            record.mark_failed(str(exc)[:2000])
            failed += 1
    
    logger.info(
        "zarr_sync_store: %s → completed=%d, failed=%d",
        store_path, completed, failed,
    )


@app.task(
    name="georiva.ingestion.zarr_tasks.sweep_zarr_pending",
    queue="georiva-default",
)
def sweep_zarr_pending():
    """
    Periodic safety-net for Zarr sync.

    Runs every 5 minutes on the georiva-default queue:
      1. Reset stale locks (PROCESSING → PENDING for locks > 30 min old)
      2. Dispatch zarr_sync_store for every store with pending/retryable records
    """
    reset_count = ZarrSyncLog.reset_stale_locks()
    if reset_count:
        logger.info("sweep_zarr_pending: reset %d stale lock(s)", reset_count)
    
    store_paths = list(ZarrSyncLog.get_pending_store_paths())
    for store_path in store_paths:
        zarr_sync_store.apply_async(
            args=[store_path],
            queue='georiva-ingestion',
        )
    
    if store_paths:
        logger.info("sweep_zarr_pending: dispatched sync for %d store(s)", len(store_paths))


@app.on_after_finalize.connect
def setup_zarr_periodic_tasks(sender, **kwargs):
    """Register sweep_zarr_pending as a periodic task (every 5 minutes)."""
    try:
        schedule_5min, _ = IntervalSchedule.objects.get_or_create(
            every=5, period=IntervalSchedule.MINUTES
        )
        PeriodicTask.objects.update_or_create(
            name="georiva.ingestion.sweep_zarr_pending",
            defaults={
                "task": "georiva.ingestion.zarr_tasks.sweep_zarr_pending",
                "interval": schedule_5min,
                "enabled": True,
            },
        )
    except Exception as exc:
        logger.warning("Could not register Zarr periodic task: %s", exc)
