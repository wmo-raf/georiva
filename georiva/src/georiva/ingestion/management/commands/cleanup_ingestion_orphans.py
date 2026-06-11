from django.core.management.base import BaseCommand
from django.db.models import F, Value
from django.db.models.functions import Concat

from georiva.core.models import Item
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
            help="Scope cleanup to a single collection slug (uses FileIngestion.collections M2M).",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        collection_slug = options["collection"]

        # Orphan: a FileIngestion whose "{bucket}:{file_path}" key matches no
        # Item.source_file. Collect live keys first (management command — not
        # a hot path, so a Python set is acceptable).
        live_source_files = set(
            Item.objects.values_list("source_file", flat=True).exclude(source_file="")
        )

        orphan_logs = (
            FileIngestion.objects
            .annotate(_sf=Concat(F("bucket"), Value(":"), F("file_path")))
            .exclude(_sf__in=live_source_files)
        )

        if collection_slug:
            orphan_logs = orphan_logs.filter(collections__slug=collection_slug)

        log_count = orphan_logs.count()
        if not dry_run:
            orphan_logs.delete()

        prefix = "[DRY RUN] " if dry_run else ""
        self.stdout.write(f"{prefix}Deleted {log_count} orphan FileIngestion(s).")
