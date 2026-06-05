from datetime import date

from django.db import models
from django_extensions.db.models import TimeStampedModel
from wagtail.admin.panels import FieldPanel
from wagtail.admin.panels import MultiFieldPanel
from wagtail.snippets.models import register_snippet

from georiva.sources.collection_definitions import CollectionDefinition, parse_collection_defs
from georiva.sources.models import DataFeed, DataFeedCollectionLink

PERIOD_CHOICES = [
    ("monthly", "Monthly"),
    ("pentadal", "Pentadal (5-day)"),
    ("dekadal", "Dekadal (10-day)"),
]

# ---------------------------------------------------------------------------
# Raw collection spec — the canonical source of truth for this plugin.
# Edit this dict to add/remove collections or change variable definitions.
# ---------------------------------------------------------------------------
COLLECTIONS = {
    "chirps-monthly": {
        "name": "CHIRPS Monthly",
        "time_resolution": "monthly",
        "default_interval_minutes": 43200,
        "variables": [
            {
                "key": "precip",
                "name": "Precipitation",
                "units": "mm",
                "source": "band_1",
                "value_range": (0.0, 300.0),
            }
        ],
    },
    "chirps-dekadal": {
        "name": "CHIRPS Dekadal",
        "time_resolution": "dekadal",
        "default_interval_minutes": 14400,
        "variables": [
            {
                "key": "precip",
                "name": "Precipitation",
                "units": "mm",
                "source": "band_1",
                "value_range": (0.0, 100.0),
            }
        ],
    },
    "chirps-pentadal": {
        "name": "CHIRPS Pentadal",
        "time_resolution": "pentadal",
        "default_interval_minutes": 7200,
        "variables": [
            {
                "key": "precip",
                "name": "Precipitation",
                "units": "mm",
                "source": "band_1",
                "value_range": (0.0, 60.0),
            }
        ],
    },
}


class CHIRPSDataFeedCollectionLink(DataFeedCollectionLink):
    """Per-collection config for a CHIRPS DataFeed."""
    
    # Baked in from definition_key — set automatically, never shown in forms
    period = models.CharField(max_length=10, choices=PERIOD_CHOICES)
    
    default_start_date = models.DateField(
        default=date(1981, 1, 1),
        help_text="Default backfill start date for this collection.",
    )
    
    class Meta:
        verbose_name = "CHIRPS Collection Link"
    
    @classmethod
    def get_panels(cls):
        # 'period' is baked in from definition_key — not operator-configurable
        return [
            FieldPanel("default_start_date"),
        ]
    
    @property
    def config(self) -> dict:
        return {
            "period": self.period,
            "default_start_date": self.default_start_date,
        }
    
    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        # Keep Collection.time_resolution in sync for code that reads it directly
        if self.collection_id:
            type(self.collection).objects.filter(pk=self.collection_id).update(
                time_resolution=self.period
            )


@register_snippet
class CHIRPSDataFeed(DataFeed, TimeStampedModel):
    """CHIRPS Loader profile. Period and start date are configured per-collection on the link."""
    
    head_timeout = models.IntegerField(
        default=20,
        help_text="HTTP HEAD timeout (seconds) used for URL existence checks.",
    )
    
    panels = [
        *DataFeed.base_panels,
        MultiFieldPanel(
            [FieldPanel("head_timeout")],
            heading="Advanced",
        ),
    ]
    
    class Meta:
        verbose_name = "CHIRPS Data Feed"
    
    # =========================================================================
    # Collection definitions (the exact set of collections this plugin creates)
    # =========================================================================
    
    @classmethod
    def get_collection_definitions(cls) -> list[CollectionDefinition]:
        return parse_collection_defs(COLLECTIONS)
    
    # =========================================================================
    # Catalog defaults (pre-fill wizard step 1)
    # =========================================================================
    
    @classmethod
    def get_catalog_defaults(cls) -> dict:
        return {
            "name": "CHIRPS",
            "file_format": "geotiff",
            "description": "CHIRPS rainfall estimates — 0.05° resolution.",
        }
    
    # =========================================================================
    # Collection link
    # =========================================================================
    
    @classmethod
    def get_collection_link_model(cls):
        return CHIRPSDataFeedCollectionLink
    
    @classmethod
    def get_link_config_for_definition(cls, definition) -> dict:
        """Derive period from the definition key so it's never shown as an editable field."""
        for period in ('monthly', 'pentadal', 'dekadal'):
            if period in definition.key:
                return {'period': period}
        return {}
    
    # =========================================================================
    # Runtime
    # =========================================================================
    
    @property
    def data_source_cls(self):
        from .source import CHIRPSDataSource
        return CHIRPSDataSource
    
    def get_loader_config(self):
        return {"head_timeout": self.head_timeout}
