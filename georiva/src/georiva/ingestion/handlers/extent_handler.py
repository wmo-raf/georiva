"""
CollectionExtentHandler — expand a Collection's temporal and spatial extent.
"""
import logging
from datetime import datetime

from georiva.core.models import Collection
from georiva.ingestion.utils import normalize_bounds

logger = logging.getLogger(__name__)


class CollectionExtentHandler:
    """
    Expands a Collection's temporal and spatial extent to include a new Item.

    Uses field-level saves (update_fields) to avoid overwriting concurrent
    updates from other ingestion workers processing the same collection.
    """
    
    def expand(
            self,
            collection: Collection,
            timestamp: datetime,
            bounds: tuple | list,
    ) -> None:
        """
        Expand *collection* extent to include *timestamp* and *bounds*.

        A no-op if the collection's current extent already covers both.
        Only the fields that actually changed are written to the database.
        """
        update_fields = []
        
        # ── Temporal extent ───────────────────────────────────────────────────
        if collection.time_start is None or timestamp < collection.time_start:
            collection.time_start = timestamp
            update_fields.append("time_start")
        
        if collection.time_end is None or timestamp > collection.time_end:
            collection.time_end = timestamp
            update_fields.append("time_end")
        
        # ── Spatial extent ────────────────────────────────────────────────────
        current = collection.bounds
        if not current or len(current) < 4:
            collection.bounds = list(bounds)
            update_fields.append("bounds")
        else:
            expanded = [
                min(current[0], bounds[0]),  # west
                min(current[1], bounds[1]),  # south
                max(current[2], bounds[2]),  # east
                max(current[3], bounds[3]),  # north
            ]
            if expanded != list(current):
                collection.bounds = normalize_bounds(expanded)
                update_fields.append("bounds")
        
        if update_fields:
            collection.save(update_fields=update_fields)
            logger.debug(
                "Updated extent for %s: fields=%s", collection.slug, update_fields
            )
