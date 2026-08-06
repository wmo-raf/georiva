"""
AssetHandler — extract and encode raster data for a single variable.

Owns:
  - Direct and chunked raster extraction
  - RGBA encoding (block-wise for chunked extraction)

Everything downstream of the extracted array — boundary masking, COG / PNG /
JSON writing, Asset DB records, collection extent — is the shared
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
from georiva.ingestion.encoder import VariableEncoder
from georiva.ingestion.extractor import VariableExtractor
from georiva.ingestion.materialization import AssetMaterializer
from georiva.ingestion.utils import iter_windows

if TYPE_CHECKING:
    from georiva.core.models import Variable

logger = logging.getLogger(__name__)


class AssetHandler:
    """
    Handles the extract → encode → write → record pipeline for one variable.

    Constructor receives the three processing objects that are shared across
    all variables in a single file run — instantiated once in IngestionContext.
    """
    
    def __init__(
            self,
            writer: AssetWriter,
            extractor: VariableExtractor,
            encoder: VariableEncoder,
    ):
        self.writer = writer
        self.extractor = extractor
        self.encoder = encoder
        self.materializer = AssetMaterializer(writer, encoder)
    
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
          1. Extract raw float array + encode to RGBA
          2. Hand off to the shared AssetMaterializer (mask, write, record,
             expand collection extent)

        Returns the list of Asset records created (typically 2: COG + PNG).
        """
        logger.debug("Processing variable: %s", variable.slug)

        final_data, final_rgba = self._extract_and_encode(
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
            rgba=final_rgba,
            bounds=bounds,
            crs=crs,
            timestamp=timestamp,
            clipper=clipper,
        )

        # Explicitly release large arrays — can be 64 MB+ for global data.
        del final_data, final_rgba

        return assets
    
    # =========================================================================
    # Extraction + encoding
    # =========================================================================
    
    def _extract_and_encode(
            self,
            variable: "Variable",
            local_path: Path,
            timestamp: datetime,
            width: int,
            height: int,
            clip_window: Optional[dict] = None,
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Extract raw data from the source file and encode it to RGBA.

        Switches between two strategies based on raster size:

        Direct extraction  — clipped or small rasters (reads full or windowed
                             array at once).

        Chunked extraction — large unclipped rasters above
                             GEORIVA_CHUNK_THRESHOLD_PIXELS. Processes the
                             grid in 2048×2048 blocks to avoid OOM on
                             continental or global datasets.

        Boundary geometry masking happens downstream in the materializer.
        """
        use_chunked = (
                width * height > settings.GEORIVA_CHUNK_THRESHOLD_PIXELS
                and clip_window is None
        )
        
        if use_chunked:
            logger.debug(
                "Using chunked extraction for %s (%dx%d)", variable.slug, width, height
            )
            final_data, final_rgba = self._extract_chunked(
                variable=variable,
                local_path=local_path,
                timestamp=timestamp,
                width=width,
                height=height,
            )
        else:
            final_data, final_rgba = self._extract_direct(
                variable=variable,
                local_path=local_path,
                timestamp=timestamp,
                width=width,
                height=height,
                clip_window=clip_window,
            )

        return final_data, final_rgba
    
    def _extract_direct(
            self,
            variable: "Variable",
            local_path: Path,
            timestamp: datetime,
            width: int,
            height: int,
            clip_window: Optional[dict] = None,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Read the full (or windowed) array at once."""
        window = None
        if clip_window:
            window = (
                clip_window["x_off"],
                clip_window["y_off"],
                clip_window["width"],
                clip_window["height"],
            )
        
        final_data = self.extractor.extract(variable, local_path, timestamp, window)
        final_rgba = self.encoder.encode_to_rgba(final_data, variable)
        return final_data, final_rgba
    
    def _extract_chunked(
            self,
            variable: "Variable",
            local_path: Path,
            timestamp: datetime,
            width: int,
            height: int,
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Process large variable in 2048×2048 pixel blocks.

        Keeps peak memory usage bounded regardless of input raster size —
        critical for global datasets (7200×3600) in memory-limited workers.
        """
        final_data = np.zeros((height, width), dtype=np.float32)
        final_rgba = np.zeros((height, width, 4), dtype=np.uint8)
        
        for x, y, w, h in iter_windows(width, height, block_size=2048):
            chunk = self.extractor.extract(variable, local_path, timestamp, (x, y, w, h))
            final_data[y:y + h, x:x + w] = chunk
            final_rgba[y:y + h, x:x + w] = self.encoder.encode_to_rgba(chunk, variable)
            del chunk
        
        return final_data, final_rgba
