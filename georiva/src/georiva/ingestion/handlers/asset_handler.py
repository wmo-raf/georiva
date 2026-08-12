"""
AssetHandler — extract raster data for a single variable.

Owns:
  - Direct and chunked raster extraction

Everything downstream of the extracted array — boundary masking, COG / JSON
writing, Asset DB records, collection extent — is the shared
AssetMaterializer (``ingestion/materialization.py``), which the derivation
engine uses too.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import numpy as np
from django.conf import settings

from georiva.core.models import Asset, Item
from georiva.ingestion.asset_writer import AssetWriter
from georiva.ingestion.clipper import BoundaryClipper
from georiva.ingestion.extractor import VariableExtractor
from georiva.ingestion.materialization import AssetMaterializer
from georiva.ingestion.utils import iter_windows

if TYPE_CHECKING:
    from georiva.core.models import Variable

logger = logging.getLogger(__name__)


class AssetHandler:
    """
    Handles the extract → write → record pipeline for one variable.

    Constructor receives the processing objects that are shared across
    all variables in a single file run — instantiated once in IngestionContext.
    """

    def __init__(
        self,
        writer: AssetWriter,
        extractor: VariableExtractor,
    ):
        self.writer = writer
        self.extractor = extractor
        self.materializer = AssetMaterializer(writer)

    # =========================================================================
    # Public entry point
    # =========================================================================

    def process_variable(
        self,
        *,
        item: Item,
        variable: "Variable",
        local_path: Path,
        timestamp: datetime,
        bounds: tuple,
        crs: str,
        width: int,
        height: int,
        clipper: Optional[BoundaryClipper] = None,
        clip_window: Optional[dict] = None,
    ) -> list[Asset]:
        """
        Run the full pipeline for *variable* at *timestamp*.

        Steps:
          1. Extract the raw float array
          2. Hand off to the shared AssetMaterializer (mask, write, record,
             expand collection extent)

        Returns the list of Asset records created.
        """
        logger.debug("Processing variable: %s", variable.slug)

        final_data = self._extract(
            variable=variable,
            local_path=local_path,
            timestamp=timestamp,
            width=width,
            height=height,
            clip_window=clip_window,
        )

        assets = self.materializer.materialize_variable(
            item=item,
            variable=variable,
            data=final_data,
            bounds=bounds,
            crs=crs,
            timestamp=timestamp,
            clipper=clipper,
        )

        # Explicitly release large arrays — can be 64 MB+ for global data.
        del final_data

        return assets

    # =========================================================================
    # Extraction
    # =========================================================================

    def _extract(
        self,
        variable: "Variable",
        local_path: Path,
        timestamp: datetime,
        width: int,
        height: int,
        clip_window: Optional[dict] = None,
    ) -> np.ndarray:
        """
        Extract raw data from the source file.

        Switches between two strategies based on raster size:

        Direct extraction  — clipped or small rasters (reads full or windowed
                             array at once).

        Chunked extraction — large unclipped rasters above
                             GEORIVA_CHUNK_THRESHOLD_PIXELS. Processes the
                             grid in 2048×2048 blocks to avoid OOM on
                             continental or global datasets.

        Boundary geometry masking happens downstream in the materializer.
        """
        use_chunked = width * height > settings.GEORIVA_CHUNK_THRESHOLD_PIXELS and clip_window is None

        if use_chunked:
            logger.debug("Using chunked extraction for %s (%dx%d)", variable.slug, width, height)
            return self._extract_chunked(
                variable=variable,
                local_path=local_path,
                timestamp=timestamp,
                width=width,
                height=height,
            )

        return self._extract_direct(
            variable=variable,
            local_path=local_path,
            timestamp=timestamp,
            clip_window=clip_window,
        )

    def _extract_direct(
        self,
        variable: "Variable",
        local_path: Path,
        timestamp: datetime,
        clip_window: Optional[dict] = None,
    ) -> np.ndarray:
        """Read the full (or windowed) array at once."""
        window = None
        if clip_window:
            window = (
                clip_window["x_off"],
                clip_window["y_off"],
                clip_window["width"],
                clip_window["height"],
            )

        return self.extractor.extract(variable, local_path, timestamp, window)

    def _extract_chunked(
        self,
        variable: "Variable",
        local_path: Path,
        timestamp: datetime,
        width: int,
        height: int,
    ) -> np.ndarray:
        """
        Process large variable in 2048×2048 pixel blocks.

        Keeps peak memory usage bounded regardless of input raster size —
        critical for global datasets (7200×3600) in memory-limited workers.
        """
        final_data = np.zeros((height, width), dtype=np.float32)

        for x, y, w, h in iter_windows(width, height, block_size=2048):
            chunk = self.extractor.extract(variable, local_path, timestamp, (x, y, w, h))
            final_data[y : y + h, x : x + w] = chunk
            del chunk

        return final_data
