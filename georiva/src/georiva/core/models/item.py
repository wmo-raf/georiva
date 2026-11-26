"""
GeoRiva Item and Asset Models

Item: a spatiotemporal entry in a Collection — a plain table (ADR 0047)
Asset: Individual files associated with an Item, linked to Variables
"""

from django.db import models, transaction
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _
from django_extensions.db.models import TimeStampedModel
from modelcluster.fields import ParentalKey
from modelcluster.models import ClusterableModel
from wagtail.models import Orderable

from georiva.core.utils import iso_utc_z

from .base import AbstractAsset, AbstractSpatialItem, HarvestProvenance

# =============================================================================
# Custom Manager
# =============================================================================


class ItemQuerySet(models.QuerySet):
    def delete_locked(self):
        """Delete these Items once no Asset is being recorded for them (#704).

        Django deletes an Item by collecting its Assets first, then deleting
        them, then the Item; an Asset committed in between names no Item and
        fails the delete at commit. Locking the Items before collecting waits
        out the writer, which holds the Item's row while it records
        (``AssetMaterializer.record_cog``), so its Asset is collected too.
        The filter is asked again by the delete, of the rows locked.
        """
        with transaction.atomic(using=self.db):
            locked = list(self.select_for_update(of=("self",)).values_list("pk", flat=True))
            return self.filter(pk__in=locked).delete()


class ItemManager(models.Manager.from_queryset(ItemQuerySet)):
    """Custom manager for Item with common query patterns."""

    def for_collection(self, collection):
        """Get items for a collection, ordered by time descending."""
        return self.filter(collection=collection).order_by("-time")

    def latest(self, collection=None):
        """Get the most recent item."""
        qs = self.all()
        if collection:
            qs = qs.filter(collection=collection)
        return qs.order_by("-time").first()

    def recent_reference_times(self, collection, limit):
        """The newest ``limit`` run reference times in ``collection``, newest first.

        The first half of answering "which runs may I show?" without reading a
        row per lead time: there is one item per ``(reference_time, time)``, so
        a caller that wants runs must ask for runs (issue #600).
        """
        return list(
            self.filter(collection=collection, reference_time__isnull=False)
            .order_by("-reference_time")
            .values_list("reference_time", flat=True)
            .distinct()[:limit]
        )

    def valid_times_by_run(self, collection, reference_times):
        """Each named run's valid times as ISO strings, oldest first, keyed by run.

        One query for a caller that has already decided which runs it will
        answer for — the EDR document's run window and the runs route's opt-in
        valid times are the same question asked of two routes (issue #600). A
        run whose items have not arrived yet maps to an empty list rather than
        going missing, so a caller can serve every key it promised.
        """
        times = {reference_time: [] for reference_time in reference_times}
        if not reference_times:
            return times
        rows = (
            self.filter(collection=collection, reference_time__in=reference_times)
            .order_by("reference_time", "time")
            .values_list("reference_time", "time")
        )
        for reference_time, time in rows:
            times.setdefault(reference_time, []).append(time.isoformat())
        return times

    def latest_for_variable(self, variable):
        """The item a preview of ``variable`` should show: newest run, earliest
        horizon.

        Plain ``-time`` ordering answers the wrong question on a forecast feed.
        Its newest valid time is the *furthest* horizon — a ten-day-out field —
        so an operator judging a style against it is judging a guess, not the
        weather. The newest ``reference_time`` therefore decides first and the
        earliest valid time within that run decides second; a collection whose
        items carry no reference time degenerates to the newest valid time,
        which for observations is the same question correctly answered.

        Only items carrying an asset Titiler can draw for this variable are
        candidates — a COG, or a Store asset read from its store: the encoded
        texture is derived from it and nothing else can stand in.
        """
        from georiva.core.storage.asset_location import drawable_q

        candidates = self.filter(
            drawable_q("assets__"),
            collection_id=variable.collection_id,
            assets__variable=variable,
        )
        latest_ref = (
            candidates.filter(reference_time__isnull=False)
            .order_by("-reference_time")
            .values_list("reference_time", flat=True)
            .first()
        )
        if latest_ref is not None:
            return candidates.filter(reference_time=latest_ref).order_by("time").first()
        return candidates.order_by("-time").first()

    def neighbour_for_variable(self, variable, item, direction):
        """The item before or after ``item`` in a preview of ``variable``, or None.

        The same candidates :meth:`latest_for_variable` draws from, ordered by
        valid time and held to ``item``'s own run: a forecast feed repeats a
        valid time once per run that predicted it, so stepping across runs would
        repeat times and jump between forecasts. A feed with no reference times
        is one long run in time order.
        """
        if direction not in ("prev", "next"):
            raise ValueError(f"direction must be 'prev' or 'next', not {direction!r}")
        from georiva.core.storage.asset_location import drawable_q

        candidates = self.filter(
            drawable_q("assets__"),
            collection_id=variable.collection_id,
            assets__variable=variable,
        ).distinct()
        if item.reference_time is not None:
            candidates = candidates.filter(reference_time=item.reference_time)
        else:
            candidates = candidates.filter(reference_time__isnull=True)
        if direction == "next":
            later = models.Q(time__gt=item.time) | models.Q(time=item.time, pk__gt=item.pk)
            return candidates.filter(later).order_by("time", "pk").first()
        earlier = models.Q(time__lt=item.time) | models.Q(time=item.time, pk__lt=item.pk)
        return candidates.filter(earlier).order_by("-time", "-pk").first()

    def latest_forecast_run(self, collection):
        """Get items from the latest forecast run."""
        latest_ref = (
            self.filter(collection=collection, reference_time__isnull=False)
            .order_by("-reference_time")
            .values("reference_time")
            .first()
        )
        if latest_ref:
            return self.filter(collection=collection, reference_time=latest_ref["reference_time"]).order_by("time")
        return self.none()

    def in_time_range(self, start, end, collection=None):
        """Get items within a time range."""
        qs = self.filter(time__gte=start, time__lte=end)
        if collection:
            qs = qs.filter(collection=collection)
        return qs.order_by("time")

    def valid_at(self, valid_time, collection=None):
        """Get items valid at a specific time."""
        qs = self.filter(time=valid_time)
        if collection:
            qs = qs.filter(collection=collection)
        return qs.order_by("-reference_time")

    def with_assets(self):
        """Prefetch assets for efficiency."""
        return self.prefetch_related("assets", "assets__variable")


class Item(AbstractSpatialItem, HarvestProvenance, TimeStampedModel, ClusterableModel):
    """
    A single spatiotemporal entry in a Collection.

    A plain table with a primary key, not a hypertable: its reads name a
    collection, not a time window, and a hypertable's planning cost on every
    query bought nothing (ADR 0047). ``time`` is the valid time.

    Spatial/raster fields (source_file, bounds, geometry, width, height,
    resolution_x/y, crs, properties) are inherited from AbstractSpatialItem.
    """

    # Excluded from Wagtail's reference index: an Item is machine-generated data
    # written on every ingestion/derivation, not editorial content whose usage or
    # delete-protection matters. Snippet registration would otherwise enrol it,
    # making every save run update_reference_index_task inline in the pipeline
    # workers. See core/test_reference_index_exclusion.py.
    wagtail_reference_index_ignore = True

    ORGANISATION_LOOKUP = "collection__catalog__organisation"

    objects = ItemManager()

    collection = models.ForeignKey(
        "georivacore.Collection",
        on_delete=models.CASCADE,
        related_name="items",
        db_index=False,  # (collection, time) leads with it
    )

    # Time dimensions
    # valid time: what time the data represents
    time = models.DateTimeField()
    reference_time = models.DateTimeField(
        null=True,
        blank=True,
        help_text=_("When data was produced (model run time for forecasts)"),
    )

    class Meta:
        ordering = ["-time"]
        constraints = [
            models.UniqueConstraint(
                fields=["collection", "time"],
                name="unique_time_per_collection",
                condition=models.Q(reference_time__isnull=True),
            ),
            models.UniqueConstraint(
                fields=["collection", "time", "reference_time"],
                name="unique_time_collection_reference",
                condition=models.Q(reference_time__isnull=False),
            ),
        ]
        indexes = [
            # Also serves ``collection = …`` alone, and ``-time`` by a backward scan.
            models.Index(fields=["collection", "time"]),
            models.Index(fields=["collection", "reference_time", "time"]),
        ]

    def __str__(self):
        return f"{self.collection.slug} @ {self.time}"

    # =========================================================================
    # Time Properties
    # =========================================================================

    @property
    def valid_time(self):
        """Alias for the ``time`` field."""
        return self.time

    @property
    def horizon(self):
        """Forecast horizon as timedelta."""
        if self.reference_time:
            return self.time - self.reference_time
        return None

    @property
    def horizon_hours(self):
        """Forecast horizon in hours."""
        if self.horizon:
            return self.horizon.total_seconds() / 3600
        return None

    @property
    def is_forecast(self):
        """True if this is forecast data (has reference_time)."""
        return self.reference_time is not None

    # =========================================================================
    # Asset Access Helpers
    # =========================================================================

    def get_asset(self, variable_slug: str) -> "Asset":
        """Get asset for a specific variable."""
        return self.assets.filter(variable__slug=variable_slug).first()

    def get_asset_by_role(self, role: str) -> "Asset":
        """Get asset by role."""
        return self.assets.filter(roles__contains=[role]).first()

    @property
    def data_assets(self):
        """Get all data assets."""
        return self.assets.filter(roles__contains=["data"])

    @property
    def time_iso(self) -> str:
        """ISO 8601 UTC string with Z suffix, for use in API URLs and templates."""
        return iso_utc_z(self.time)

    @property
    def reference_time_iso(self) -> str | None:
        """ISO 8601 UTC string with Z suffix for reference_time, or None."""
        if self.reference_time:
            return iso_utc_z(self.reference_time)
        return None

    def display_time(self, time_resolution: str = "") -> str:
        """
        Format valid_time according to the collection's time resolution.
        """
        formats = {
            "sub_hourly": "%d %b %Y %H:%M",
            "hourly": "%d %b %Y %H:%M",
            "3hourly": "%d %b %Y %H:%M",
            "6hourly": "%d %b %Y %H:%M",
            "12hourly": "%d %b %Y %H:%M",
            "daily": "%d %b %Y",
            "pentadal": "%d %b %Y",
            "dekadal": "%d %b %Y",
            "monthly": "%b %Y",
            "sub_seasonal": "%d %b %Y",
            "seasonal": "%b %Y",
            "annual": "%Y",
            "climatology": "%b %Y",
        }
        fmt = formats.get(time_resolution, "%d %b %Y %H:%M")
        return self.time.strftime(fmt)


class Asset(AbstractAsset, HarvestProvenance, TimeStampedModel, Orderable):
    """
    A stored data file for a specific Variable within an Item.

    Links Item (when/where) to Variable (what) with the actual file (href).

    File/classification/stats fields and the Role/Format enums are inherited
    from AbstractAsset; only the tier-specific relations live here.
    """

    ORGANISATION_LOOKUP = "item__collection__catalog__organisation"

    # Parent Item
    item = ParentalKey(
        Item,
        on_delete=models.CASCADE,
        related_name="assets",
    )

    # Link to Variable (carries units, styling, visualization config)
    variable = models.ForeignKey(
        "georivacore.Variable",
        on_delete=models.CASCADE,
        related_name="assets",
    )

    # Kept beside the harvest Provenance, and like it outlives the feed (ADR 0044 §3).
    store_array = models.CharField(
        max_length=255,
        blank=True,
        default="",
        editable=False,
        help_text=_(
            "For a Store asset: the full path, in the upstream store its href names, of the array it is a "
            "slice of. Which slice is its item's reference time and valid time."
        ),
    )
    store_selection = models.JSONField(
        default=dict,
        blank=True,
        editable=False,
        help_text=_(
            "For a Store asset whose array has extra axes — ensemble members, statistics, levels: the value of "
            "each it is read at, by the axis's name. Empty for an array with none."
        ),
    )

    class Meta:
        ordering = ["sort_order"]
        constraints = [
            models.UniqueConstraint(fields=["item", "variable", "format"], name="unique_format_per_variable_per_item"),
        ]
        indexes = [
            models.Index(fields=["item"]),
            models.Index(fields=["item", "variable"]),
        ]

    def __str__(self):
        return f"{self.item} / {self.variable.slug} - {self.format}"

    # =========================================================================
    # Properties from Variable (convenience accessors)
    # =========================================================================

    @property
    def name(self):
        return self.variable.name

    @property
    def unit(self):
        return self.variable.unit.symbol if self.variable.unit else ""

    # =========================================================================
    # Format checks
    # =========================================================================

    @property
    def is_visual(self) -> bool:
        return self.format in (self.Format.PNG, self.Format.WEBP, self.Format.JPEG)

    @property
    def is_data(self) -> bool:
        return self.format in (self.Format.COG, self.Format.GEOTIFF, self.Format.ZARR)

    @property
    def is_cog(self) -> bool:
        return self.format == self.Format.COG

    @property
    def is_zarr(self) -> bool:
        return self.format == self.Format.ZARR

    @property
    def is_drawable(self) -> bool:
        """Whether Titiler can draw this asset — a COG or a Store asset (#686).
        Its queryset half is ``storage.asset_location.drawable_q``."""
        return self.location.is_drawable

    # =========================================================================
    # Extra fields accessors
    # =========================================================================

    @property
    def overviews(self):
        """For COG: overview levels."""
        return self.extra_fields.get("overviews", [])

    @property
    def compression(self):
        """For COG: compression method."""
        return self.extra_fields.get("compression")

    @property
    def nodata(self):
        """Nodata value."""
        return self.extra_fields.get("nodata")

    # =========================================================================
    # URL Generation
    # =========================================================================
    @property
    def location(self):
        """Where this asset's bytes are: our bucket, or upstream (ADR 0043 §1)."""
        from georiva.core.storage.asset_location import AssetLocation

        return AssetLocation(self.href, self.format)

    @property
    def store_vector(self):
        """For a vector Variable's Store asset, the two arrays it is computed
        from and which quantity of them it is (a ``VectorComponents``, ADR 0048);
        ``None`` for any other asset. Such an asset names no array of its own."""
        if self.store_array or not self.location.is_store:
            return None
        return self.variable.vector_components

    @cached_property
    def url(self) -> str:
        """Public URL to this asset — the upstream href itself for a Remote asset."""
        return self.location.public_url()

    @property
    def preview_url(self) -> str:
        """TiTiler preview URL for this asset."""
        from georiva.core.machine_plane import titiler_preview_url

        return titiler_preview_url(self.item, self.variable)
