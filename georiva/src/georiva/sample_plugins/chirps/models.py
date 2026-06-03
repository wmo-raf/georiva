from datetime import date

from django.db import models
from django_extensions.db.models import TimeStampedModel
from wagtail.admin.panels import FieldPanel, MultiFieldPanel

from georiva.sources.models import DataFeed

PERIOD_CHOICES = [
    ("monthly", "Monthly"),
    ("pentadal", "Pentadal (5-day)"),
]


class CHIRPSDataFeed(DataFeed, TimeStampedModel):
    """
    CHIRPS Loader profile:
      - Select monthly and/or pentadal
      - Optional time window defaults (if your loader uses profile-driven backfill)
    """
    
    period = models.CharField(
        max_length=10,
        choices=PERIOD_CHOICES,
    )
    
    # Optional: if you want to drive backfill defaults from the profile
    default_start_date = models.DateField(
        default=date(1981, 1, 1),
        help_text="Default backfill start date (if not supplied elsewhere).",
    )
    
    head_timeout = models.IntegerField(
        default=20,
        help_text="HTTP HEAD timeout (seconds) used for existence checks.",
    )
    
    panels = [
        *DataFeed.base_panels,
        FieldPanel("period"),
        MultiFieldPanel(
            [
                FieldPanel("default_start_date"),
                FieldPanel("head_timeout"),
            ],
            heading="Advanced",
        ),
    ]
    
    class Meta:
        verbose_name = "CHIRPS Data Feed"
    
    @classmethod
    def get_wizard_defaults(cls) -> dict:
        return {"period": "monthly"}

    @classmethod
    def get_catalog_defaults(cls) -> dict:
        return {
            "name": "CHIRPS",
            "file_format": "geotiff",
            "description": "CHIRPS rainfall estimates — 0.05° resolution.",
        }

    @property
    def data_source_cls(self):
        from .source import CHIRPSDataSource
        return CHIRPSDataSource
    
    def get_loader_config(self):
        return {
            "period": self.period,
            "default_start_date": self.default_start_date,
            "head_timeout": self.head_timeout,
        }
