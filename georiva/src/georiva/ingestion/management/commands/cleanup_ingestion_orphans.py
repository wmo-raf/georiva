from django.core.management.base import BaseCommand

from georiva.ingestion.models import FileIngestion


class Command(BaseCommand):
    help = (
        "Delete FileIngestion records no longer referenced by any Item. "
        "Use --dry-run to preview counts without deleting."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Print counts without deleting anything.",
        )
        parser.add_argument(
            "--collection",
            type=str,
            default=None,
            metavar="SLUG",
            help="Scope cleanup to a single collection slug.",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        collection_slug = options["collection"]

        # Pass 1: FileIngestions with no Items
        orphan_logs = FileIngestion.objects.filter(items__isnull=True)
        if collection_slug:
            orphan_logs = orphan_logs.filter(collection_slug=collection_slug)
        log_count = orphan_logs.count()
        if not dry_run:
            orphan_logs.delete()

        prefix = "[DRY RUN] " if dry_run else ""
        self.stdout.write(f"{prefix}Deleted {log_count} orphan FileIngestion(s).")
