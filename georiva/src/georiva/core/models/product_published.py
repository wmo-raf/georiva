"""``ProductPublished``: new data became servable in a collection (ADR 0030).

One per (collection × publishing act): once per file-and-collection when that
file's per-collection processing finishes, and once per completed Derivation
unit. Never one per Item. Recorded for ``internal`` Intermediate products too,
with the visibility tier snapshotted so the record says what the tier was when
it happened.

References to Published Items are plain id columns: Item is a hypertable and is
removed by retention, so a foreign key would either block that or dangle.
"""

from django.db import models

from georiva.events.models import DomainEvent


class ProductPublished(DomainEvent):
    label = "Product published"

    class Origin(models.TextChoices):
        INGESTED = "ingested", "Ingested from a file"
        DERIVED = "derived", "Derived by a recipe"

    collection = models.ForeignKey(
        "georivacore.Collection", null=True, blank=True, on_delete=models.SET_NULL, related_name="+"
    )
    collection_slug = models.CharField(max_length=255)
    catalog_slug = models.CharField(max_length=255)
    visibility = models.CharField(max_length=20, help_text="The collection's tier when this was published.")
    origin = models.CharField(max_length=10, choices=Origin.choices, default=Origin.INGESTED)
    item_count = models.PositiveIntegerField()
    valid_time_start = models.DateTimeField(null=True, blank=True)
    valid_time_end = models.DateTimeField(null=True, blank=True)
    source_file = models.CharField(max_length=500, blank=True, help_text="``bucket:path`` for ingested products.")
    produced_item_id = models.BigIntegerField(null=True, blank=True, help_text="Derived products: the Item written.")

    class Meta(DomainEvent.Meta):
        indexes = [models.Index(fields=["collection", "-occurred_at"], name="idx_prodpub_collection")]

    def summary(self):
        noun = "item" if self.item_count == 1 else "items"
        return f"{self.item_count} {noun} published to {self.catalog_slug}/{self.collection_slug}"
