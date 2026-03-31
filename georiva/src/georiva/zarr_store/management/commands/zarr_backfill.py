"""
Management command: zarr_backfill

Queues Zarr sync for existing COG assets that have no corresponding Zarr asset.
Use this to populate the Zarr store from already-ingested data.

Usage:
    georiva zarr_backfill
    georiva zarr_backfill --catalog chirps
    georiva zarr_backfill --catalog chirps --collection chirps-daily
    georiva zarr_backfill --dry-run
"""

from django.core.management.base import BaseCommand, CommandError

from georiva.core.models import Collection
from georiva.zarr_store.utils import rebuild_zarr_for_collection


class Command(BaseCommand):
    help = (
        "Backfill Zarr stores from existing COG assets. "
        "Queues ZarrSyncLog PENDING records for (item, variable) pairs "
        "that have a COG asset but no Zarr asset yet."
    )
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--catalog',
            type=str,
            default=None,
            help='Scope to a single catalog slug.',
        )
        parser.add_argument(
            '--collection',
            type=str,
            default=None,
            help='Scope to a single collection slug.',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            default=False,
            help='Print what would be queued without creating any records.',
        )
    
    def handle(self, *args, **options):
        
        catalog_slug = options['catalog']
        collection_slug = options['collection']
        dry_run = options['dry_run']
        
        qs = Collection.objects.select_related('catalog').filter(is_active=True)
        
        if catalog_slug:
            qs = qs.filter(catalog__slug=catalog_slug)
        if collection_slug:
            qs = qs.filter(slug=collection_slug)
        
        collections = list(qs)
        if not collections:
            raise CommandError(
                "No active collections found for the given filters. "
                "Check --catalog and --collection arguments."
            )
        
        if dry_run:
            self.stdout.write(self.style.WARNING("Dry-run mode — no records will be written."))
        
        total = 0
        for collection in collections:
            label = f"{collection.catalog.slug}/{collection.slug}"
            count = rebuild_zarr_for_collection(collection, dry_run=dry_run)
            verb = "Would queue" if dry_run else "Queued"
            self.stdout.write(f"  {verb} {count:>6} record(s) for {label}")
            total += count
        
        style = self.style.WARNING if dry_run else self.style.SUCCESS
        verb = "Would queue" if dry_run else "Queued"
        self.stdout.write(style(f"\n{verb} {total} total record(s) across {len(collections)} collection(s)."))
