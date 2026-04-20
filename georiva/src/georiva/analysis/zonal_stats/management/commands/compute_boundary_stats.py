"""
Management command: compute_boundary_stats

Backfills BoundaryZonalStats for existing COG assets.

Usage
-----
# Backfill all variables in a collection (async via Celery)
python manage.py compute_boundary_stats --collection chirps/chirps-monthly

# Single variable, synchronous
python manage.py compute_boundary_stats \\
    --collection chirps/chirps-monthly \\
    --variable precipitation \\
    --sync

# Specific time range
python manage.py compute_boundary_stats \\
    --collection chirps/chirps-monthly \\
    --time-start 2020-01-01 \\
    --time-end 2024-12-31 \\
    --sync

# All collections with boundary_stats_level configured
python manage.py compute_boundary_stats --all --sync
"""

import time
from datetime import datetime

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction


class Command(BaseCommand):
    help = "Backfill zonal statistics for existing COG assets"
    
    def add_arguments(self, parser):
        parser.add_argument(
            "--collection",
            help="catalog_slug/collection_slug, e.g. chirps/chirps-monthly",
        )
        parser.add_argument(
            "--variable",
            help="Variable slug. If omitted, all active variables are processed.",
        )
        parser.add_argument(
            "--all",
            action="store_true",
            help="Process all collections with boundary_stats_levels configured.",
        )
        parser.add_argument(
            "--time-start",
            help="Start of time range (ISO format, e.g. 2020-01-01).",
        )
        parser.add_argument(
            "--time-end",
            help="End of time range (ISO format, e.g. 2024-12-31).",
        )
        parser.add_argument(
            "--sync",
            action="store_true",
            help="Run synchronously (blocking). Default: dispatch to Celery.",
        )
        parser.add_argument(
            "--overwrite",
            action="store_true",
            help="Recompute stats even if rows already exist.",
        )
    
    def handle(self, *args, **options):
        from georiva.core.models import Asset
        from georiva.core.storage import storage
        from georiva.analysis.zonal_stats.service import (
            compute_stats_from_cog_bytes,
            get_boundaries_for_collection,
            persist_stats,
        )
        from georiva.analysis.zonal_stats.tasks import compute_boundary_zonal_stats
        
        collections = self._resolve_collections(options)
        
        time_start = self._parse_date(options.get("time_start"))
        time_end = self._parse_date(options.get("time_end"))
        
        total_written = 0
        total_skipped = 0
        
        for collection in collections:
            boundaries_by_level = get_boundaries_for_collection(collection)
            if not boundaries_by_level:
                self.stdout.write(
                    self.style.WARNING(
                        f"  {collection} — no boundary_stats_levels set, skipping"
                    )
                )
                continue
            
            total_boundaries = sum(len(b) for b in boundaries_by_level.values())
            self.stdout.write(
                f"\n{collection} — {total_boundaries} boundary/ies "
                f"at level(s) {collection.boundary_stats_levels}"
            )
            
            assets_qs = (
                Asset.objects
                .filter(
                    item__collection=collection,
                    format=Asset.Format.COG,
                )
                .select_related("item", "variable")
                .order_by("item__time")
            )
            
            if options.get("variable"):
                assets_qs = assets_qs.filter(variable__slug=options["variable"])
            
            if time_start:
                assets_qs = assets_qs.filter(item__time__gte=time_start)
            if time_end:
                assets_qs = assets_qs.filter(item__time__lte=time_end)
            
            total = assets_qs.count()
            self.stdout.write(f"  {total} COG asset(s) to process")
            
            with transaction.atomic():
                for i, asset in enumerate(assets_qs.iterator(chunk_size=100), 1):
                    if options["sync"]:
                        t0 = time.perf_counter()
                        try:
                            with transaction.atomic():
                                cog_bytes = storage.assets.read_bytes(asset.href)
                                total_written_asset = 0
                                for level, boundaries in boundaries_by_level.items():
                                    stats_rows = compute_stats_from_cog_bytes(
                                        cog_bytes, boundaries
                                    )
                                    written = persist_stats(
                                        item=asset.item,
                                        variable=asset.variable,
                                        stats_rows=stats_rows,
                                    )
                                    total_written += written
                                    total_written_asset += written
                            elapsed = time.perf_counter() - t0
                            self.stdout.write(
                                f"  [{i}/{total}] {asset.variable.slug} "
                                f"@ {asset.item.time.date()} "
                                f"→ {total_written_asset} row(s) ({elapsed:.2f}s)"
                            )
                        except Exception as exc:
                            self.stdout.write(
                                self.style.ERROR(
                                    f"  [{i}/{total}] FAILED {asset.variable.slug} "
                                    f"@ {asset.item.time}: {exc}"
                                )
                            )
                    else:
                        compute_boundary_zonal_stats.apply_async(
                            args=[asset.pk],
                            queue="georiva-ingestion",
                        )
                        total_written += 1
                        if i % 50 == 0:
                            self.stdout.write(f"  Dispatched {i}/{total}…")
        
        action = "written" if options["sync"] else "dispatched"
        self.stdout.write(
            self.style.SUCCESS(f"\nDone. {total_written} row(s) {action}.")
        )
    
    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------
    
    def _resolve_collections(self, options):
        from georiva.core.models import Collection
        
        if options["all"] and options.get("collection"):
            raise CommandError("Pass either --all or --collection, not both.")
        
        if options["all"]:
            return list(
                Collection.objects.filter(
                    is_active=True,
                    boundary_stats_levels__isnull=False,
                ).exclude(
                    boundary_stats_levels=[],
                ).select_related("catalog")
            )
        
        if options.get("collection"):
            parts = options["collection"].split("/")
            if len(parts) != 2:
                raise CommandError(
                    "--collection must be catalog_slug/collection_slug"
                )
            catalog_slug, collection_slug = parts
            try:
                return [
                    Collection.objects.select_related("catalog").get(
                        catalog__slug=catalog_slug,
                        slug=collection_slug,
                    )
                ]
            except Collection.DoesNotExist:
                raise CommandError(
                    f"Collection not found: {options['collection']}"
                )
        
        raise CommandError("Pass --collection <catalog/collection> or --all.")
    
    @staticmethod
    def _parse_date(value: str | None):
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                import pytz
                dt = pytz.utc.localize(dt)
            return dt
        except ValueError:
            raise CommandError(
                f"Invalid date format: {value!r}. Use ISO format, e.g. 2020-01-01"
            )
