"""
Re-materialize existing derived items through the shared AssetMaterializer.

Derived items written before the engine adopted the shared materialization
sequence are missing the housekeeping ingestion always did: no visual PNG for
anomaly/climatology items, unclipped/unmasked rasters in clipping catalogs,
no ``imageUnscale`` extra fields, and no collection extent (which is what let
the item-detail map stretch their PNGs across the whole world).

This command replays materialization for each derived item from its stored
COG — no recipe recompute — bringing history up to the current contract:
clipped COG + visual PNG + JSON sidecar, honest Asset rows, item bounds
snapped to the clip window, and the derived collection's extent rebuilt.

It also reports, per variable, the observed data range across everything it
read next to the variable's configured ``value_min``/``value_max`` — the
operator's evidence for tuning display ranges (e.g. anomaly palettes).

Usage:
    georiva rematerialize_derived_assets                      # all derived items
    georiva rematerialize_derived_assets --catalog chirps
    georiva rematerialize_derived_assets --collection precip-anomaly
    georiva rematerialize_derived_assets --dry-run            # report only
"""
import logging

import numpy as np
from django.core.management.base import BaseCommand

logger = logging.getLogger(__name__)


def _read_cog(href):
    """Read a stored COG into ``(data, bounds, crs)`` — north-up, nodata→NaN."""
    import rasterio
    from rasterio.io import MemoryFile

    from georiva.core.storage import storage

    raw = storage.assets.read_bytes(href)
    with MemoryFile(raw) as memfile, memfile.open() as src:
        data = src.read(1).astype("float32")
        if src.nodata is not None:
            data = np.where(data == src.nodata, np.nan, data)
        if src.transform.e > 0:
            data = np.flipud(data)
        crs = src.crs.to_string() if src.crs else "EPSG:4326"
        return data, list(src.bounds), crs


class Command(BaseCommand):
    help = (
        "Re-run the shared asset materialization for existing derived items "
        "(clip + visual PNG + extra fields + collection extent) and report "
        "observed data ranges per variable."
    )

    def add_arguments(self, parser):
        parser.add_argument("--catalog", default=None, help="Catalog slug filter")
        parser.add_argument("--collection", default=None, help="Collection slug filter")
        parser.add_argument(
            "--dry-run", action="store_true",
            help="Read and report only — write nothing.",
        )

    def handle(self, *args, **options):
        from georiva.core.models import Asset, Item
        from georiva.core.storage import storage
        from georiva.ingestion.asset_writer import AssetWriter
        from georiva.ingestion.materialization import AssetMaterializer
        from georiva.processing.engine import _catalog_clipper

        items = (
            Item.objects
            .filter(properties__has_key="derivation")
            .select_related("collection__catalog")
            .order_by("collection_id", "time")
        )
        if options["catalog"]:
            items = items.filter(collection__catalog__slug=options["catalog"])
        if options["collection"]:
            items = items.filter(collection__slug=options["collection"])

        dry_run = options["dry_run"]
        materializer = AssetMaterializer(AssetWriter(storage.assets))
        clippers = {}          # collection_id -> BoundaryClipper | None
        observed = {}          # variable pk -> {"variable", "min", "max", "n"}
        done = failed = 0

        for item in items.iterator():
            collection = item.collection
            if collection.pk not in clippers:
                clippers[collection.pk] = _catalog_clipper(collection)
            clipper = clippers[collection.pk]

            cogs = list(
                item.assets.filter(format=Asset.Format.COG).select_related(
                    "variable", "variable__unit",
                )
            )
            if not cogs:
                continue

            for asset in cogs:
                label = f"{collection.slug}/{asset.variable.slug} @ {item.time:%Y-%m-%d}"
                try:
                    data, bounds, crs = _read_cog(asset.href)
                    if clipper is not None:
                        data, bounds = materializer.clip_array(data, bounds, clipper)

                    self._track_range(observed, asset.variable, data)

                    if not dry_run:
                        materializer.materialize_variable(
                            item=item,
                            variable=asset.variable,
                            data=data,
                            bounds=bounds,
                            crs=crs,
                            timestamp=item.time,
                            clipper=clipper,
                            checksum=asset.checksum,
                        )
                        height, width = data.shape
                        item.bounds = list(bounds)
                        item.width = width
                        item.height = height
                        item.save(update_fields=["bounds", "width", "height"])
                    done += 1
                    self.stdout.write(f"{'would rematerialize' if dry_run else 'rematerialized'}: {label}")
                except Exception as e:
                    failed += 1
                    self.stderr.write(self.style.ERROR(f"failed: {label} — {e}"))

        self.stdout.write(self.style.SUCCESS(
            f"{'Scanned' if dry_run else 'Rematerialized'} {done} asset(s), {failed} failure(s)."
        ))
        self._report_ranges(observed)

    def _track_range(self, observed, variable, data):
        finite = data[np.isfinite(data)]
        if finite.size == 0:
            return
        rec = observed.setdefault(
            variable.pk,
            {"variable": variable, "min": np.inf, "max": -np.inf, "n": 0},
        )
        rec["min"] = min(rec["min"], float(finite.min()))
        rec["max"] = max(rec["max"], float(finite.max()))
        rec["n"] += 1

    def _report_ranges(self, observed):
        if not observed:
            return
        self.stdout.write("")
        self.stdout.write("Observed data ranges vs configured display ranges:")
        for rec in observed.values():
            v = rec["variable"]
            self.stdout.write(
                f"  {v.collection.slug}/{v.slug}: observed "
                f"[{rec['min']:.2f}, {rec['max']:.2f}] over {rec['n']} raster(s); "
                f"configured value_min/max [{v.value_min}, {v.value_max}]"
            )
        self.stdout.write(
            "Adjust a variable's value_min/value_max in the admin if the "
            "observed range clips or washes out, then re-run this command."
        )
