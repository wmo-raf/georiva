"""
AssetMaterializer — the shared "array in, served assets out" step.

Both pipelines that publish raster assets end at the same materialization
sequence: normalize bounds → apply the catalog boundary mask → encode the
visual RGBA → write COG + PNG + JSON sidecar → upsert Asset rows (with the
``imageUnscale``/``scale`` extra fields map clients read) → expand the owning
Collection's extent. Ingestion (``handlers/asset_handler.py``) and the
derivation engine (``processing/engine.py``) both call this class, so derived
items can no longer drift from ingested ones — the drift is what left derived
collections extent-less and their PNGs stretched across the world map.

Extraction stays per-flow: ingestion reads windowed chunks from source files,
recipes compute arrays. This service only owns what happens after an array,
its bounds, and its Variable exist.
"""
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Optional

import numpy as np
from wagtail import hooks

from georiva.core.models import Asset, Item
from georiva.core.storage import storage
from georiva.ingestion.asset_writer import AssetWriter
from georiva.ingestion.constants import GEORIVA_AFTER_SAVE_ASSET
from georiva.ingestion.encoder import VariableEncoder
from georiva.ingestion.utils import compute_stats, normalize_bounds

if TYPE_CHECKING:
    from georiva.core.models import Variable
    from georiva.ingestion.clipper import BoundaryClipper

logger = logging.getLogger(__name__)


class AssetMaterializer:
    """
    Persist one variable's raster array as the served asset trio
    (COG + visual PNG + JSON sidecar) and keep the catalog metadata honest.

    COG failure raises (the item is unservable without it); PNG and JSON
    failures are non-fatal and logged, matching ingestion's long-standing
    partial-failure contract.
    """

    def __init__(
            self,
            writer: AssetWriter,
            encoder: Optional[VariableEncoder] = None,
    ):
        # Late import: handlers/__init__ imports asset_handler, which imports
        # this module — importing the subpackage at module level would cycle.
        from georiva.ingestion.handlers.extent_handler import CollectionExtentHandler

        self.writer = writer
        self.encoder = encoder or VariableEncoder()
        self.extent_handler = CollectionExtentHandler()

    # =========================================================================
    # Public entry points
    # =========================================================================

    def clip_array(
            self,
            data: np.ndarray,
            bounds: list | tuple,
            clipper: "BoundaryClipper",
    ) -> tuple[np.ndarray, list | tuple]:
        """
        Crop a full-grid array to the clipper's pixel-snapped window.

        Used by callers whose arrays cover the whole source grid (the
        derivation engine); ingestion crops at read time instead. A window
        that cannot be computed (no intersection) keeps the full grid.
        """
        if clipper is None or not clipper.is_active:
            return data, bounds
        height, width = data.shape[:2]
        try:
            window = clipper.compute_window(tuple(bounds), width, height)
        except ValueError as e:
            logger.warning("Clip window computation failed: %s — keeping full grid", e)
            return data, bounds
        if not window:
            return data, bounds
        y0, x0 = window["y_off"], window["x_off"]
        cropped = data[y0:y0 + window["height"], x0:x0 + window["width"]]
        return cropped, window["bounds"]

    def materialize_variable(
            self,
            *,
            item: Item,
            variable: "Variable",
            data: np.ndarray,
            bounds: list | tuple,
            crs: str,
            timestamp: datetime,
            rgba: Optional[np.ndarray] = None,
            clipper: Optional["BoundaryClipper"] = None,
            stats: Optional[dict] = None,
            checksum: str = "",
    ) -> list[Asset]:
        """
        Run the shared materialization sequence for one variable's array.

        ``rgba`` may be supplied pre-encoded (ingestion's chunked path builds
        it block-by-block); otherwise it is encoded here from the masked data.
        ``clipper`` applies the precise boundary geometry mask — window
        cropping is the caller's job (``clip_array`` for full-grid arrays).
        """
        bounds = normalize_bounds(bounds)

        if clipper is not None and clipper.is_active:
            data = clipper.apply_geometry_mask(data, bounds, nodata=np.nan)
            if rgba is not None:
                rgba = clipper.apply_rgba_mask(rgba, bounds)

        if stats is None:
            stats = compute_stats(data)

        assets = self._save_assets(
            item=item, variable=variable, data=data, rgba=rgba,
            stats=stats, bounds=bounds, crs=crs, timestamp=timestamp,
            checksum=checksum,
        )

        self.extent_handler.expand(item.collection, timestamp, bounds)
        return assets

    # =========================================================================
    # Asset writing + DB records
    # =========================================================================

    def _save_assets(
            self,
            *,
            item: Item,
            variable: "Variable",
            data: np.ndarray,
            rgba: Optional[np.ndarray],
            stats: dict,
            bounds: list,
            crs: str,
            timestamp: datetime,
            checksum: str = "",
    ) -> list[Asset]:
        """
        Write the COG / PNG / JSON trio to storage and upsert Asset rows.
        """
        height, width = data.shape[:2]
        catalog = item.collection.catalog

        if item.reference_time:
            ref_str = item.reference_time.strftime("%Y%m%dT%H%M%S")
            base_name = f"{variable.slug}_{timestamp.strftime('%H%M%S')}__ref{ref_str}"
        else:
            base_name = f"{variable.slug}_{timestamp.strftime('%H%M%S')}"

        base_dir = storage.build_asset_path(
            org=catalog.organisation.slug,
            catalog=catalog.slug,
            collection=item.collection.slug,
            variable=variable.slug,
            timestamp=timestamp,
            filename="",
        ).rstrip("/")

        assets: list[Asset] = []
        visual_asset: Optional[Asset] = None

        # ── COG ───────────────────────────────────────────────────────────────
        cog_path = f"{base_dir}/{base_name}.tif"
        try:
            stored_cog = self.writer.write_cog(data, cog_path, tuple(bounds), crs)
            cog_defaults = {
                "href": stored_cog,
                "media_type": (
                    "image/tiff; application=geotiff; profile=cloud-optimized"
                ),
                "roles": ["data"],
                "file_size": self._get_file_size(stored_cog),
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
            }
            # Only stamp a checksum the caller actually supplied (derived
            # provenance) — never clobber an existing one with "".
            if checksum:
                cog_defaults["checksum"] = checksum
            data_asset, _ = Asset.objects.update_or_create(
                item=item,
                variable=variable,
                format=Asset.Format.COG,
                defaults=cog_defaults,
            )
            assets.append(data_asset)
            self._after_save_asset(data_asset)
        except Exception as e:
            logger.error("COG save failed for %s: %s", variable.slug, e)
            raise

        # ── PNG ───────────────────────────────────────────────────────────────
        png_path = f"{base_dir}/{base_name}.png"
        try:
            if rgba is None:
                rgba = self.encoder.encode_to_rgba(data, variable)
            stored_png = self.writer.write_png(rgba, png_path)
            visual_asset, _ = Asset.objects.update_or_create(
                item=item,
                variable=variable,
                format=Asset.Format.PNG,
                defaults={
                    "href": stored_png,
                    "media_type": "image/png",
                    "roles": ["visual"],
                    "file_size": self._get_file_size(stored_png),
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

    def _get_file_size(self, path: str) -> Optional[int]:
        try:
            return int(self.writer.bucket.size(path))
        except Exception:
            return None
