"""
ItemHandler — get-or-create an Item record and keep its spatial fields current.
"""
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from django.db.models import F

from georiva.core.models import Collection, Item
from georiva.ingestion.utils import ensure_utc, normalize_bounds

if TYPE_CHECKING:
    from georiva.ingestion.models import FileIngestion

logger = logging.getLogger(__name__)


class ItemHandler:
    """
    Manages Item creation and spatial-field maintenance.

    One Item represents one (collection, valid_time, reference_time) tuple.
    On re-ingest the Item is updated rather than duplicated so that Asset
    records always point to the most recent spatial metadata.
    """
    
    def get_or_create(
            self,
            *,
            collection: Collection,
            timestamp: datetime,
            reference_time: Optional[datetime],
            source_file: str,
            ingestion_log: Optional["FileIngestion"],
            bounds: tuple | list,
            width: int,
            height: int,
            crs: str,
    ) -> tuple[Item, bool]:
        """
        Return (item, created).

        If the Item already exists its spatial fields are updated when they
        differ from the incoming values — useful when re-ingesting a corrected
        or higher-resolution file.
        """
        ts_utc = ensure_utc(timestamp)
        ref_utc = ensure_utc(reference_time) if reference_time else None
        bounds = normalize_bounds(bounds)
        
        item, created = Item.objects.get_or_create(
            collection=collection,
            time=ts_utc,
            reference_time=ref_utc,
            defaults={
                "source_file": source_file,
                "bounds": list(bounds),
                "width": width,
                "height": height,
                "resolution_x": (
                    abs((bounds[2] - bounds[0]) / width) if width else 0
                ),
                "resolution_y": (
                    abs((bounds[3] - bounds[1]) / height) if height else 0
                ),
                "crs": crs,
            },
        )

        if not created:
            logger.info(
                "Item already exists for %s @ %s — updating assets", collection, ts_utc
            )
            update_fields = []

            if item.source_file != source_file:
                item.source_file = source_file
                update_fields.append("source_file")

            if list(item.bounds) != list(bounds):
                item.bounds = list(bounds)
                item.width = width
                item.height = height
                update_fields.extend(["bounds", "width", "height"])

            if update_fields:
                item.save(update_fields=update_fields)

        # Link the FileIngestion to the Item it produced.
        if ingestion_log and ingestion_log.item_id != item.pk:
            ingestion_log.item = item
            ingestion_log.save(update_fields=["item_id"])

        return item, created
    
    def increment_collection_item_count(self, collection: Collection) -> None:
        """Atomically increment Collection.item_count after a new Item is created."""
        Collection.objects.filter(pk=collection.pk).update(
            item_count=F("item_count") + 1
        )
    
    def delete_orphan(self, item: Item) -> None:
        """
        Delete an Item that ended up with no assets.

        Called by IngestionHandler when every variable in a timestamp fails
        to produce any output — keeps the catalog free of empty shells.
        """
        logger.warning(
            "No assets created for Item %s — deleting orphan item", item.pk
        )
        item.delete()
