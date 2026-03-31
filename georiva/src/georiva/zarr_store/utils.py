"""
Zarr backfill helpers shared between the management command and Wagtail admin.
"""

import logging

from georiva.core.models import Asset
from .models import ZarrSyncLog
from .tasks import zarr_sync_store

logger = logging.getLogger(__name__)


def rebuild_zarr_for_collection(collection, dry_run: bool = False) -> int:
    """
    Queue Zarr sync for all COG assets in a collection that lack a Zarr asset.

    Skips forecast items (reference_time IS NOT NULL).
    Uses bulk_create with ignore_conflicts=True so it is safe to call multiple times.

    Returns the number of ZarrSyncLog records actually inserted (or that would
    be inserted in dry-run mode). Records that already exist are silently skipped
    and not counted.
    """
    
    # COG assets for non-forecast items in this collection
    cog_assets = (
        Asset.objects
        .filter(
            format=Asset.Format.COG,
            item__collection=collection,
            item__reference_time__isnull=True,
        )
        .select_related(
            'item',
            'item__collection',
            'item__collection__catalog',
            'variable',
        )
        .order_by('item__time')
    )
    
    # (item_id, variable_id) pairs that already have a Zarr asset
    existing_zarr = set(
        Asset.objects
        .filter(format=Asset.Format.ZARR, item__collection=collection)
        .values_list('item_id', 'variable_id')
    )
    
    to_create = []
    catalog_slug = collection.catalog.slug
    collection_slug = collection.slug
    
    for asset in cog_assets:
        pair = (asset.item_id, asset.variable_id)
        if pair in existing_zarr:
            continue
        
        store_path = f"{catalog_slug}/{collection_slug}/{asset.variable.slug}.zarr"
        to_create.append(ZarrSyncLog(
            item=asset.item,
            variable=asset.variable,
            store_path=store_path,
            status=ZarrSyncLog.Status.PENDING,
        ))
    
    if dry_run:
        # Count distinct store paths that would be touched
        store_paths = {r.store_path for r in to_create}
        logger.info(
            "zarr backfill dry-run: would queue %d record(s) across %d store(s) for %s/%s",
            len(to_create), len(store_paths), catalog_slug, collection_slug,
        )
        return len(to_create)
    
    if not to_create:
        logger.info(
            "zarr backfill: nothing to queue for %s/%s",
            catalog_slug, collection_slug,
        )
        return 0
    
    # bulk_create returns only the actually-inserted rows (conflicts silently skipped).
    # Derive store_paths from created records so we don't dispatch tasks for stores
    # where every record was already present.
    created = ZarrSyncLog.objects.bulk_create(to_create, ignore_conflicts=True)
    count = len(created)
    
    store_paths = {r.store_path for r in created}
    for store_path in store_paths:
        zarr_sync_store.apply_async(args=[store_path], queue='georiva-ingestion')
    
    logger.info(
        "zarr backfill: queued %d record(s) across %d store(s) for %s/%s",
        count, len(store_paths), catalog_slug, collection_slug,
    )
    return count
