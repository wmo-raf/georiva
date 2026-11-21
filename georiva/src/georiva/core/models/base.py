from django.contrib.postgres.fields import ArrayField
from django.db import models
from django.utils.translation import gettext_lazy as _


class AbstractCollection(models.Model):
    slug = models.SlugField(max_length=100)
    name = models.CharField(max_length=200)
    description = models.TextField(blank=True)

    bounds = ArrayField(
        models.FloatField(),
        size=4,
        null=True,
        blank=True,
    )
    crs = models.CharField(max_length=50, default="EPSG:4326")

    class Meta:
        abstract = True

    def __str__(self):
        return self.slug


class AbstractSpatialItem(models.Model):
    # Source tracking — convention: "{bucket}:{file_path}".
    source_file = models.CharField(
        max_length=500,
        blank=True,
        db_index=True,
        help_text=_("Original source file path"),
    )

    bounds = ArrayField(
        models.FloatField(),
        size=4,
        null=True,
        blank=True,
        help_text=_("Bounding box [west, south, east, north]"),
    )
    geometry = models.JSONField(
        null=True,
        blank=True,
        help_text=_("GeoJSON geometry for non-rectangular footprints"),
    )

    width = models.IntegerField(null=True, blank=True)
    height = models.IntegerField(null=True, blank=True)
    resolution_x = models.FloatField(null=True, blank=True)
    resolution_y = models.FloatField(null=True, blank=True)
    crs = models.CharField(max_length=50, default="EPSG:4326")

    properties = models.JSONField(default=dict, blank=True)

    class Meta:
        abstract = True


class AbstractAsset(models.Model):
    class Role(models.TextChoices):
        DATA = "data", _("Data")
        VISUAL = "visual", _("Visual")
        THUMBNAIL = "thumbnail", _("Thumbnail")
        OVERVIEW = "overview", _("Overview")
        METADATA = "metadata", _("Metadata")
        SOURCE = "source", _("Source")  # raw acquisition artifact (staging)

    class Format(models.TextChoices):
        COG = "cog", _("Cloud-Optimized GeoTIFF")
        ZARR = "zarr", _("Zarr")
        GEOTIFF = "geotiff", _("GeoTIFF")
        NETCDF = "netcdf", _("NetCDF")
        GRIB2 = "grib2", _("GRIB2")
        PNG = "png", _("PNG")
        WEBP = "webp", _("WebP")
        JPEG = "jpeg", _("JPEG")
        JSON = "json", _("JSON")

    href = models.CharField(
        max_length=500,
        help_text=_("Storage path or URL"),
    )
    media_type = models.CharField(
        max_length=100,
        blank=True,
        help_text=_("MIME type"),
    )

    roles = ArrayField(
        models.CharField(max_length=20, choices=Role.choices),
        default=list,
    )
    format = models.CharField(
        max_length=20,
        choices=Format.choices,
        blank=True,
    )

    file_size = models.BigIntegerField(null=True, blank=True)
    checksum = models.CharField(max_length=64, blank=True)

    # Raster info (if different from Item)
    width = models.IntegerField(null=True, blank=True)
    height = models.IntegerField(null=True, blank=True)
    bands = models.IntegerField(null=True, blank=True, default=1)

    stats_min = models.FloatField(null=True, blank=True)
    stats_max = models.FloatField(null=True, blank=True)
    stats_mean = models.FloatField(null=True, blank=True)
    stats_std = models.FloatField(null=True, blank=True)

    extra_fields = models.JSONField(default=dict, blank=True)

    class Meta:
        abstract = True


class HarvestProvenance(models.Model):
    """The durable mark on a harvested row naming the Harvest feed that created it (ADR 0042 §7).

    Set once, when a harvest creates the row, and never rewritten: it survives
    Materialization as history, and it survives the feed. There is deliberately no
    database constraint and no cascade — deleting a feed leaves its rows with
    Provenance pointing nowhere, and they are then nobody's. So read
    ``harvested_by_id``; following the relation may find no feed.

    Published tier only. Null on every row a harvest did not create, and a row
    without it is in no Mirror's Jurisdiction, wherever its href points.
    """

    harvested_by = models.ForeignKey(
        "georivasources.DataFeed",
        null=True,
        blank=True,
        editable=False,
        on_delete=models.DO_NOTHING,
        db_constraint=False,
        related_name="+",
        help_text=_("The harvest feed that created this row, if one did."),
    )

    class Meta:
        abstract = True
