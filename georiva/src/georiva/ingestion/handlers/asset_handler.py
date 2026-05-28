"""
AssetHandler — extract, encode, and persist assets for a single variable.

Owns:
  - Direct and chunked raster extraction
  - RGBA encoding
  - COG / PNG / JSON asset writing
  - Asset DB record creation via update_or_create
"""
import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import numpy as np
from django.conf import settings
from wagtail import hooks

from georiva.core.models import Asset, Item
from georiva.core.storage import storage
from georiva.ingestion.asset_writer import AssetWriter
from georiva.ingestion.clipper import BoundaryClipper
from georiva.ingestion.constants import GEORIVA_AFTER_SAVE_ASSET
from georiva.ingestion.encoder import VariableEncoder
from georiva.ingestion.extractor import VariableExtractor
from georiva.ingestion.utils import compute_stats, iter_windows

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
          2. Apply geometry mask (if clipper is active)
          3. Compute statistics
          4. Write COG / PNG / JSON assets and create Asset DB records

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
            clipper=clipper,
            bounds=bounds,
        )
        
        stats = compute_stats(final_data)
        
        assets = self._save_assets(
            item=item,
            variable=variable,
            final_data=final_data,
            final_rgba=final_rgba,
            stats=stats,
            bounds=bounds,
            crs=crs,
            width=width,
            height=height,
            timestamp=timestamp,
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
            clipper: Optional[BoundaryClipper] = None,
            bounds: Optional[tuple] = None,
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

        After extraction applies the boundary geometry mask if configured.
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
        
        if clipper and clipper.is_active:
            final_data = clipper.apply_geometry_mask(
                final_data, bounds, nodata=np.nan
            )
            final_rgba = clipper.apply_rgba_mask(final_rgba, bounds)
        
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
    
    # =========================================================================
    # Asset writing + DB records
    # =========================================================================
    
    def _save_assets(
            self,
            item: Item,
            variable: "Variable",
            final_data: np.ndarray,
            final_rgba: np.ndarray,
            stats: dict,
            bounds: tuple,
            crs: str,
            width: int,
            height: int,
            timestamp: datetime,
    ) -> list[Asset]:
        """
        Write processed data to storage and create Asset DB records.

        Writes three asset types per variable per timestamp:
          COG  — Cloud-Optimized GeoTIFF (primary; TiTiler + analysis layer)
          PNG  — Encoded RGBA visual (GL web map clients)
          JSON — Metadata sidecar (frontend + API responses)

        COG failure raises immediately (PNG and JSON are skipped).
        PNG and JSON failures are non-fatal — a warning is logged.
        """
        catalog_slug = item.collection.catalog.slug
        collection_slug = item.collection.slug
        
        if item.reference_time:
            ref_str = item.reference_time.strftime("%Y%m%dT%H%M%S")
            base_name = f"{variable.slug}_{timestamp.strftime('%H%M%S')}__ref{ref_str}"
        else:
            base_name = f"{variable.slug}_{timestamp.strftime('%H%M%S')}"
        
        base_dir = storage.build_asset_path(
            catalog=catalog_slug,
            collection=collection_slug,
            variable=variable.slug,
            timestamp=timestamp,
            filename="",
        ).rstrip("/")
        
        assets: list[Asset] = []
        visual_asset: Optional[Asset] = None
        
        # ── COG ───────────────────────────────────────────────────────────────
        cog_path = f"{base_dir}/{base_name}.tif"
        try:
            stored_cog = self.writer.write_cog(final_data, cog_path, bounds, crs)
            data_asset, _ = Asset.objects.update_or_create(
                item=item,
                variable=variable,
                format=Asset.Format.COG,
                defaults={
                    "href": stored_cog,
                    "media_type": (
                        "image/tiff; application=geotiff; profile=cloud-optimized"
                    ),
                    "roles": ["data"],
                    "file_size": self._get_file_size(storage.assets, stored_cog),
                    "width": width,
                    "height": height,
                    "bands": 1,
                    "stats_min": stats.get("min"),
                    "stats_max": stats.get("max"),
                    "stats_mean": stats.get("mean"),
                    "stats_std": stats.get("std"),
                    "extra_fields": {
                        "compression": "deflate",
                        "nodata": None,
                    },
                },
            )
            assets.append(data_asset)
            self._after_save_asset(data_asset)
        except Exception as e:
            logger.error("COG save failed for %s: %s", variable.slug, e)
            raise
        
        # ── PNG ───────────────────────────────────────────────────────────────
        png_path = f"{base_dir}/{base_name}.png"
        try:
            stored_png = self.writer.write_png(final_rgba, png_path)
            visual_asset, _ = Asset.objects.update_or_create(
                item=item,
                variable=variable,
                format=Asset.Format.PNG,
                defaults={
                    "href": stored_png,
                    "media_type": "image/png",
                    "roles": ["visual"],
                    "file_size": self._get_file_size(storage.assets, stored_png),
                    "width": width,
                    "height": height,
                    "bands": 4,
                    "stats_min": stats.get("min"),
                    "stats_max": stats.get("max"),
                    "stats_mean": stats.get("mean"),
                    "stats_std": stats.get("std"),
                    "extra_fields": {
                        "imageUnscale": [variable.value_min, variable.value_max],
                        "scale": variable.scale_type or "linear",
                    },
                },
            )
            assets.append(visual_asset)
            self._after_save_asset(visual_asset)
        except Exception as e:
            logger.error("PNG save failed for %s: %s", variable.slug, e)
        
        # ── JSON sidecar ──────────────────────────────────────────────────────
        meta_path = f"{base_dir}/{base_name}.json"
        try:
            metadata = {
                "variable": variable.slug,
                "name": variable.name,
                "units": variable.unit.symbol if variable.unit else "",
                "timestamp": timestamp.isoformat(),
                "reference_time": (
                    item.reference_time.isoformat() if item.reference_time else None
                ),
                "bounds": list(bounds),
                "width": width,
                "height": height,
                "crs": crs,
                "transform": variable.transform_type,
                "imageUnscale": [variable.value_min, variable.value_max],
                "scale": variable.scale_type or "linear",
                "stats": stats,
            }
            if visual_asset:
                metadata["color_map"] = visual_asset.variable.weather_layers_palette
            
            self.writer.write_metadata(metadata, meta_path)
        except Exception as e:
            logger.warning("Metadata save failed for %s: %s", variable.slug, e)
        
        return assets
    
    # =========================================================================
    # Helpers
    # =========================================================================
    
    def _after_save_asset(self, asset: Asset) -> None:
        try:
            for fn in hooks.get_hooks(GEORIVA_AFTER_SAVE_ASSET):
                return fn(asset)
        except Exception as e:
            logger.warning("Post-save hook failed for asset %s: %s", asset.pk, e)
    
    def _get_file_size(self, bucket, path: str) -> Optional[int]:
        try:
            return bucket.size(path)
        except Exception:
            return None
