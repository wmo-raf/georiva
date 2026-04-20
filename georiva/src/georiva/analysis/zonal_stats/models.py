from django.db import models
from django.utils.translation import gettext_lazy as _
from django_extensions.db.models import TimeStampedModel
from timescale.db.models.models import TimescaleModel


class BoundaryZonalStats(TimescaleModel, TimeStampedModel):
    """
    Zonal statistics for one (Item, Variable, AdminBoundary) triple.

    TimescaleDB hypertable partitioned by ``time`` (valid_time).

    Unique constraint: (item, variable, boundary) — one row per
    boundary per variable per timestep.  Safe to re-compute with
    bulk_create(update_conflicts=True).
    """
    
    # -------------------------------------------------------------------------
    # Identity
    # -------------------------------------------------------------------------
    
    item = models.ForeignKey(
        "georivacore.Item",
        on_delete=models.CASCADE,
        related_name="zonal_stats",
        db_constraint=False,  # hypertable FK — same pattern as Asset
        help_text=_("Source item (carries valid_time and reference_time)."),
    )
    variable = models.ForeignKey(
        "georivacore.Variable",
        on_delete=models.CASCADE,
        related_name="zonal_stats",
        help_text=_("Variable these stats describe."),
    )
    boundary = models.ForeignKey(
        "adminboundarymanager.AdminBoundary",
        on_delete=models.CASCADE,
        related_name="zonal_stats",
        help_text=_("Administrative boundary region."),
    )
    
    # -------------------------------------------------------------------------
    # Statistics
    # -------------------------------------------------------------------------
    
    mean = models.FloatField(null=True, blank=True)
    min = models.FloatField(null=True, blank=True)
    max = models.FloatField(null=True, blank=True)
    sum = models.FloatField(null=True, blank=True)
    std = models.FloatField(null=True, blank=True)
    count = models.IntegerField(
        null=True,
        blank=True,
        help_text=_("Number of valid (non-NaN) pixels within the boundary."),
    )
    
    # -------------------------------------------------------------------------
    # Meta
    # -------------------------------------------------------------------------
    
    class Meta:
        # TimescaleModel provides the 'time' field (= valid_time).
        # We add ordering and indexes but NOT unique_together because
        # TimescaleDB hypertables do not support unique constraints that
        # don't include the partition column.  Uniqueness is enforced at
        # the application layer via bulk_create(update_conflicts=True).
        ordering = ["-time"]
        constraints = [
            models.UniqueConstraint(
                fields=["time", "item", "variable", "boundary"],
                name="uq_bzs_time_item_variable_boundary",
            ),
        ]
        indexes = [
            # Primary query: all stats for a boundary × variable over time
            models.Index(
                fields=["boundary", "variable", "time"],
                name="idx_bzs_boundary_variable_time",
            ),
            # Lookup by item — used during ingestion to check for dupes
            models.Index(
                fields=["boundary", "variable", "item"],
                name="idx_bzs_boundary_variable_item",
            ),
        ]
    
    def __str__(self):
        return (
            f"{self.variable.slug} @ {self.boundary} [{self.time}]"
        )
    
    # -------------------------------------------------------------------------
    # Convenience accessors
    # -------------------------------------------------------------------------
    
    @property
    def valid_time(self):
        """Alias for TimescaleModel.time — matches Item.valid_time semantics."""
        return self.time
    
    @property
    def reference_time(self):
        """Forecast reference time from the parent Item."""
        return self.item.reference_time
    
    @property
    def is_forecast(self):
        return self.item.reference_time is not None
