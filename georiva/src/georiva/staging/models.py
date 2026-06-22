"""
Staging data tier — source-grained, not-served STAC models.

Mirrors the Published STAC spec (Collection/Item/Asset) but follows the
source/acquisition shape: one ``StagingItem`` per raw file (no timestep
shredding), a flexible temporal extent (no TimescaleDB hypertable), and assets
carrying the ``source`` role.

See docs/adr/0004-staging-tier-and-abstract-stac-models.md.
"""

from django.db import models
from django.utils.translation import gettext_lazy as _
from django_extensions.db.models import TimeStampedModel
from modelcluster.fields import ParentalKey
from modelcluster.models import ClusterableModel
from wagtail.models import Orderable

from georiva.core.models.base import (
    AbstractAsset,
    AbstractCollection,
    AbstractSpatialItem,
)


class StagingCollection(AbstractCollection, TimeStampedModel, ClusterableModel):
    """A source-grained grouping of staged raw artifacts."""
    
    catalog = models.ForeignKey(
        'georivacore.Catalog',
        on_delete=models.CASCADE,
        related_name='staging_collections',
    )
    
    is_active = models.BooleanField(default=True)
    
    class Meta:
        unique_together = ['catalog', 'slug']
        ordering = ['catalog', 'name']
    
    def __str__(self):
        return f"{self.catalog.slug}/{self.slug} (staging)"


class StagingItem(AbstractSpatialItem, TimeStampedModel, ClusterableModel):
    """
    One staged raw file held as a STAC-shaped item.

    Not a TimescaleDB hypertable. Carries a flexible STAC temporal extent:
    a nullable single ``datetime`` (one slice) and/or a ``start_datetime`` /
    ``end_datetime`` range (multi-temporal file). These are approximate
    Gregorian index bounds for selection only — the authoritative time and
    calendar are read from file content at derivation time.
    """
    
    collection = models.ForeignKey(
        StagingCollection,
        on_delete=models.CASCADE,
        related_name='items',
    )
    
    # Flexible STAC temporal extent (all nullable).
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
        ordering = ['collection', '-start_datetime', '-datetime']
        indexes = [
            models.Index(fields=['collection', 'start_datetime']),
            models.Index(fields=['collection', 'datetime']),
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
    
    item = ParentalKey(
        StagingItem,
        on_delete=models.CASCADE,
        related_name='assets',
        db_constraint=False,
    )
    
    variable = models.ForeignKey(
        'georivacore.Variable',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='staging_assets',
    )
    
    class Meta:
        ordering = ['sort_order']
        indexes = [
            models.Index(fields=['item']),
        ]
    
    def __str__(self):
        return f"{self.item} / {self.format or 'raw'}"
