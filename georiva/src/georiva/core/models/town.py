"""Towns: the gazetteer of named places a forecast is told for.

A point list per country, imported from GeoNames ``cities1000`` into the
instance-wide tier (``import_towns``) and extended by each organisation with
the places GeoNames ranks poorly or misses — a station town, a market, a local
spelling. Tiered like color ramps (ADR 0011): a town with no organisation is
the instance-wide baseline every organisation reads and only the instance admin
writes; a town with one is that organisation's addition.

A town is a point and a label, nothing more. Which forecast is told for it is
the consumer's business — today the Forti publisher's places endpoint, which
joins a model's point answers onto the towns inside its area.
"""

from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from wagtail.admin.panels import FieldPanel, FieldRowPanel


class Town(models.Model):
    ORGANISATION_LOOKUP = "organisation"
    ORGANISATION_GLOBAL_TIER = True

    class Rank(models.IntegerChoices):
        """How prominent a town is, most prominent first.

        Derived from the GeoNames feature code, so a capital outranks a
        regional seat however small it is. Within a rank, population orders the
        towns (see :meth:`Town.importance`).
        """

        CAPITAL = 1, "Capital"
        ADMIN1_SEAT = 2, "Admin 1 seat"
        ADMIN2_SEAT = 3, "Admin 2 seat"
        TOWN = 4, "Town"

    #: GeoNames feature code → rank. Anything else is a plain town.
    RANK_BY_FEATURE_CODE = {
        "PPLC": Rank.CAPITAL,
        "PPLG": Rank.CAPITAL,
        "PPLA": Rank.ADMIN1_SEAT,
        "PPLA2": Rank.ADMIN2_SEAT,
    }

    organisation = models.ForeignKey(
        "organisations.Organisation",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="towns",
        help_text=(
            "The organisation this town belongs to. Left empty, it is part of "
            "the instance-wide gazetteer every organisation can use."
        ),
    )
    geonames_id = models.BigIntegerField(
        null=True,
        blank=True,
        help_text="The GeoNames id an imported town was matched on. Empty for a town added by hand.",
    )
    name = models.CharField(max_length=200)
    ascii_name = models.CharField(max_length=200, blank=True)
    country_code = models.CharField(max_length=2, help_text="ISO 3166-1 alpha-2, e.g. KE.")
    admin1_code = models.CharField(max_length=20, blank=True)
    feature_code = models.CharField(
        max_length=10,
        default="PPL",
        help_text="GeoNames feature code: PPLC capital, PPLA admin-1 seat, PPLA2 admin-2 seat, PPL populated place.",
    )
    latitude = models.FloatField(validators=[MinValueValidator(-90), MaxValueValidator(90)])
    longitude = models.FloatField(validators=[MinValueValidator(-180), MaxValueValidator(180)])
    population = models.PositiveBigIntegerField(default=0)
    rank = models.PositiveSmallIntegerField(choices=Rank.choices, default=Rank.TOWN, editable=False)
    is_active = models.BooleanField(
        default=True,
        help_text=(
            "An inactive town is kept, not shown. The importer deactivates a town "
            "GeoNames no longer lists rather than deleting it."
        ),
    )

    panels = [
        FieldPanel("name"),
        FieldPanel("ascii_name"),
        FieldRowPanel([FieldPanel("latitude"), FieldPanel("longitude")]),
        FieldRowPanel([FieldPanel("country_code"), FieldPanel("admin1_code")]),
        FieldRowPanel([FieldPanel("feature_code"), FieldPanel("population")]),
        FieldPanel("is_active"),
    ]

    class Meta:
        ordering = ["country_code", "rank", "-population", "name"]
        indexes = [models.Index(fields=["latitude", "longitude"])]
        constraints = [
            # One instance-wide row per GeoNames place, which is what makes a
            # re-import an upsert. Org additions may carry an id too (copied
            # from GeoNames) without colliding with the baseline.
            models.UniqueConstraint(
                fields=["geonames_id"],
                condition=models.Q(organisation__isnull=True, geonames_id__isnull=False),
                name="unique_global_town_geonames_id",
            ),
        ]

    def __str__(self):
        return f"{self.name} ({self.country_code})"

    def save(self, *args, **kwargs):
        self.rank = self.rank_for(self.feature_code)
        super().save(*args, **kwargs)

    @classmethod
    def rank_for(cls, feature_code: str) -> int:
        return cls.RANK_BY_FEATURE_CODE.get((feature_code or "").upper(), cls.Rank.TOWN)

    def importance(self) -> tuple[int, int]:
        """Sort key, most prominent first: rank, then larger population."""
        return (self.rank, -self.population)

    def owner_label(self):
        return self.organisation.name if self.organisation_id else "Instance-wide"

    owner_label.short_description = "Owner"
