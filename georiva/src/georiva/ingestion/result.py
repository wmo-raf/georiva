import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


# =============================================================================
# Result Container
# =============================================================================

@dataclass
class IngestionResult:
    """
    Captures the outcome of processing a single incoming file.

    Tracks what was created, what failed, and metadata about the
    clipping and archiving steps. Used for logging, monitoring,
    and deciding whether to delete the source file after processing.
    """
    
    origin_file: str
    origin_bucket: str
    catalog_slug: str
    collection_slug: str
    success: bool
    timestamp: datetime
    items_created: list = field(default_factory=list)
    assets_created: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    
    # Clipping metadata — populated if catalog has a boundary configured
    clipped: bool = False
    clip_boundary: str = ""
    original_size: tuple = None
    clipped_size: tuple = None
    
    # Archive path — populated if catalog.archive_source_files is True
    archive_path: str = ""
    
    def add_error(self, msg: str):
        self.errors.append(msg)
        logger.error(msg)
    
    @property
    def size_reduction_percent(self) -> Optional[float]:
        """Storage reduction achieved by clipping, as a percentage."""
        if self.original_size and self.clipped_size:
            original_pixels = self.original_size[0] * self.original_size[1]
            clipped_pixels = self.clipped_size[0] * self.clipped_size[1]
            return 100 * (1 - clipped_pixels / original_pixels)
        return None
