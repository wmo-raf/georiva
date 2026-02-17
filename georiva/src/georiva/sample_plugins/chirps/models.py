from datetime import date

from django.db import models
from django_extensions.db.models import TimeStampedModel
from wagtail.admin.panels import FieldPanel, MultiFieldPanel
from wagtail.snippets.models import register_snippet

from georiva.sources.models import LoaderProfile

PERIOD_CHOICES = [
    ("monthly", "Monthly"),
    ("pentadal", "Pentadal (5-day)"),
]


@register_snippet
class CHIRPSLoaderProfile(LoaderProfile, TimeStampedModel):
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
        *LoaderProfile.base_panels,
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
        verbose_name = "CHIRPS Loader Profile"
    
    def get_loader_config(self):
        return {
            "period": self.period,
            "default_start_date": self.default_start_date,
            "head_timeout": self.head_timeout,
        }
