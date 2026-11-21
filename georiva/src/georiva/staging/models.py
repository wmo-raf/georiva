from django.db import models
from django.utils.translation import gettext_lazy as _
from django_extensions.db.models import TimeStampedModel
from modelcluster.fields import ParentalKey
from modelcluster.models import ClusterableModel
from wagtail.models import Orderable
from wagtail.snippets.models import register_snippet

from georiva.core.models.base import (
    AbstractAsset,
    AbstractCollection,
    AbstractSpatialItem,
)


@register_snippet
class StagingCollection(AbstractCollection, TimeStampedModel, ClusterableModel):
    # Machine-generated data, not editorial content — kept out of Wagtail's
    # reference index (see core/tests/test_reference_index_exclusion.py).
    wagtail_reference_index_ignore = True

    ORGANISATION_LOOKUP = "catalog__organisation"

    catalog = models.ForeignKey(
        "georivacore.Catalog",
        on_delete=models.CASCADE,
        related_name="staging_collections",
    )

    # Nullable: a staged file whose core Collection isn't provisioned yet still
    # registers; SET_NULL keeps staging data if the core Collection is later deleted.
    collection = models.ForeignKey(
        "georivacore.Collection",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="staging_collections",
    )

    is_active = models.BooleanField(default=True)

    class Meta:
        unique_together = ["catalog", "slug"]
        ordering = ["catalog", "name"]

    def __str__(self):
        return f"{self.catalog.slug}/{self.slug} (staging)"


@register_snippet
class StagingItem(AbstractSpatialItem, TimeStampedModel, ClusterableModel):
    # Machine-generated data written on every staged file — kept out of Wagtail's
    # reference index (see core/tests/test_reference_index_exclusion.py).
    wagtail_reference_index_ignore = True

    ORGANISATION_LOOKUP = "collection__catalog__organisation"

    collection = models.ForeignKey(
        StagingCollection,
        on_delete=models.CASCADE,
        related_name="items",
    )

    datetime = models.DateTimeField(
        null=True,
        blank=True,
        db_index=True,
        help_text=_("Single valid time, when the file represents one slice"),
    )
    start_datetime = models.DateTimeField(
        null=True,
        blank=True,
        db_index=True,
        help_text=_("Start of the temporal extent for a multi-temporal file"),
    )
    end_datetime = models.DateTimeField(
        null=True,
        blank=True,
        db_index=True,
        help_text=_("End of the temporal extent for a multi-temporal file"),
    )
    reference_time = models.DateTimeField(
        null=True,
        blank=True,
        db_index=True,
        help_text=_("Model run time, for forecast inputs"),
    )

    class Meta:
        ordering = ["collection", "-start_datetime", "-datetime"]
        indexes = [
            models.Index(fields=["collection", "start_datetime"]),
            models.Index(fields=["collection", "datetime"]),
        ]

    def __str__(self):
        when = self.datetime or self.start_datetime
        return f"{self.collection.slug} @ {when} (staging)"


class StagingAsset(AbstractAsset, TimeStampedModel, Orderable):
    """
    A stored raw artifact for a StagingItem.

    Usually carries the ``source`` role. The ``variable`` link is optional —
    a raw multi-variable file (e.g. a NetCDF holding several variables) does
    not map to a single Variable.
    """

    ORGANISATION_LOOKUP = "item__collection__catalog__organisation"

    item = ParentalKey(
        StagingItem,
        on_delete=models.CASCADE,
        related_name="assets",
        db_constraint=False,
    )

    variable = models.ForeignKey(
        "georivacore.Variable",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="staging_assets",
    )

    class Meta:
        ordering = ["sort_order"]
        indexes = [
            models.Index(fields=["item"]),
        ]

    def __str__(self):
        return f"{self.item} / {self.format or 'raw'}"


class DerivationLink(models.Model):
    ORGANISATION_LOOKUP = "derived_item__collection__catalog__organisation"

    # FKs to Item use db_constraint=False because Item is a TimescaleDB
    # hypertable with no simple unique PK to reference (same as Asset.item).
    derived_item = models.ForeignKey(
        "georivacore.Item",
        on_delete=models.CASCADE,
        related_name="derivation_links",
        db_constraint=False,
        help_text=_("The Published item this lineage edge describes"),
    )

    source_staging_item = models.ForeignKey(
        StagingItem,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="derived_into",
    )
    source_published_item = models.ForeignKey(
        "georivacore.Item",
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="derived_into",
        db_constraint=False,
    )

    recipe_id = models.CharField(max_length=100)
    recipe_version = models.CharField(max_length=50)
    input_hash = models.CharField(max_length=64)

    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=["derived_item"]),
            models.Index(fields=["source_staging_item"]),
            models.Index(fields=["source_published_item"]),
        ]
        constraints = [
            models.CheckConstraint(
                name="derivationlink_exactly_one_source",
                condition=(
                    models.Q(
                        source_staging_item__isnull=False,
                        source_published_item__isnull=True,
                    )
                    | models.Q(
                        source_staging_item__isnull=True,
                        source_published_item__isnull=False,
                    )
                ),
            ),
        ]

    def __str__(self):
        src = self.source_staging_item or self.source_published_item
        return f"{self.derived_item} ⟵ {src} ({self.recipe_id}@{self.recipe_version})"
