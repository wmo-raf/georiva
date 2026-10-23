"""
Recompute ``Collection.item_count`` from the items that actually exist.

Ingestion counts items in and, since ADR 0032, deleting an item counts it out —
but every delete before that left the count behind, so existing collections can
overstate what they hold. Run once after deploying, and whenever a count is in
doubt; it is idempotent.

Examples::

    georiva recount_items
    georiva recount_items --org kenya
"""

from django.core.management.base import BaseCommand
from django.db.models import Count, IntegerField, OuterRef, Subquery, Value
from django.db.models.functions import Coalesce

from georiva.core.models import Collection, Item


class Command(BaseCommand):
    help = "Recompute Collection.item_count from the items that exist."

    def add_arguments(self, parser):
        parser.add_argument(
            "--org",
            metavar="SLUG",
            help="Scope to one organisation (by slug).",
        )

    def handle(self, *args, **options):
        collections = Collection.objects.select_related("catalog__organisation").annotate(live=Count("items"))
        if options["org"]:
            collections = collections.filter(catalog__organisation__slug=options["org"])

        fixed = 0
        for coll in collections:
            if coll.item_count == coll.live:
                continue
            self.stdout.write(f"{coll.catalog.storage_prefix}/{coll.slug}: {coll.item_count} -> {coll.live}")
            # Counted again in the UPDATE itself, so an item ingested since the
            # listing above is not counted out.
            Collection.objects.filter(pk=coll.pk).update(item_count=_live_count())
            fixed += 1

        self.stdout.write(self.style.SUCCESS(f"Corrected {fixed} collection count(s)."))


def _live_count():
    counted = (
        Item.objects.filter(collection=OuterRef("pk"))
        .order_by()
        .values("collection")
        .annotate(n=Count("*"))
        .values("n")
    )
    return Coalesce(Subquery(counted, output_field=IntegerField()), Value(0))
