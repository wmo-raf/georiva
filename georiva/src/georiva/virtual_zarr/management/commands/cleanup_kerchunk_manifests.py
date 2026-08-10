from django.core.management.base import BaseCommand

from georiva.core.storage import storage
from georiva.virtual_zarr.models import VirtualZarrManifest


class Command(BaseCommand):
    help = (
        "Delete the retired kerchunk JSON manifests from the zarr bucket. "
        "One-off cleanup after the Icechunk migration: the old keys were "
        "{org}/{catalog}/{collection}/{variable}.json, derived here from the "
        "existing manifest records."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="List the keys that would be deleted without deleting them.",
        )

    def handle(self, *args, **options):
        manifests = VirtualZarrManifest.objects.select_related(
            "variable",
            "variable__collection",
            "variable__collection__catalog",
        )

        deleted = missing = 0
        for manifest in manifests:
            collection = manifest.variable.collection
            key = (
                f"{collection.catalog.storage_prefix}/"
                f"{collection.slug}/{manifest.variable.slug}.json"
            )
            if options["dry_run"]:
                exists = storage.zarr.exists(key)
                marker = "would delete" if exists else "not found"
                self.stdout.write(f"  [{marker}] {key}")
                continue

            if storage.zarr.delete(key):
                deleted += 1
                self.stdout.write(f"  deleted: {key}")
            else:
                missing += 1

        if not options["dry_run"]:
            self.stdout.write(self.style.SUCCESS(
                f"Deleted {deleted} kerchunk manifest(s); "
                f"{missing} already absent."
            ))
