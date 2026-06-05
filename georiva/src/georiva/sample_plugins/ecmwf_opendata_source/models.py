from datetime import timedelta
from zoneinfo import ZoneInfo

from django import forms
from django.contrib.postgres.fields import ArrayField
from django.core.validators import MinValueValidator, MaxValueValidator
from django.db import models
from django.utils import timezone
from django_extensions.db.models import TimeStampedModel
from timezone_field import TimeZoneField
from wagtail.admin.forms import WagtailAdminModelForm
from wagtail.admin.panels import FieldPanel, MultiFieldPanel

from georiva.sources.collection_definitions import CollectionDefinition, parse_collection_defs
from georiva.sources.models import DataFeed

# ---------------------------------------------------------------------------
# ECMWF AIFS collection definitions
# ---------------------------------------------------------------------------

_PRESSURE_LEVELS = [1000, 925, 850, 700, 500, 300, 250, 200, 50]


def _height(name, value):
    """Source dict for a height-above-ground level."""
    return {
        "name": name,
        "level": {
            "type": "heightAboveGround",
            "value": value,
            "dimension": "heightAboveGround",
            "unit": "m"
        }
    }


def _pl_source(name, level):
    """Source dict for a pressure level."""
    return {
        "name": name,
        "level": {
            "type": "pressure",
            "value": level,
            "dimension": "isobaricInhPa",
            "unit": "hPa"
        }
    }


def _pl_vars(base_key, base_name, units, value_range=None):
    """Generate one variable dict per pressure level."""
    v = [
        {
            "key": f"{base_key}_{lv}",
            "name": f"{base_name} at {lv} hPa",
            "units": units,
            "source": _pl_source(base_key, lv),
            **({"value_range": value_range} if value_range else {}),
        }
        for lv in _PRESSURE_LEVELS
    ]
    return v


# ---------------------------------------------------------------------------
# Raw collection spec — the canonical source of truth for this plugin.
# ---------------------------------------------------------------------------
COLLECTIONS = {
    "ecmwf-aifs-surface": {
        "name": "Surface Variables",
        "time_resolution": "hourly",
        "is_forecast": True,
        "variables": [
            {
                "key": "2t",
                "name": "2m Temperature",
                "units": "K",
                "source": _height("2t", 2),
                "value_range": (213.0, 333.0),
            },
            {
                "key": "10u",
                "name": "10m U Wind Component",
                "units": "m/s",
                "source": _height("10u", 10),
                "value_range": (-80.0, 80.0),
            },
            {
                "key": "10v",
                "name": "10m V Wind Component",
                "units": "m/s",
                "source": _height("10v", 10),
                "value_range": (-80.0, 80.0),
            },
            {
                "key": "msl",
                "name": "Mean Sea Level Pressure",
                "units": "Pa",
                "source": "msl",
                "value_range": (87000.0, 108000.0),
            },
            {
                "key": "tp",
                "name": "Total Precipitation",
                "units": "m",
                "source": "tp",
                "value_range": (0.0, 0.5),
            },
            {
                "key": "sp",
                "name": "Surface Pressure",
                "units": "Pa",
                "source": "sp",
                "value_range": (47000.0, 108000.0),
            },
            {
                "key": "wind_speed_10m",
                "name": "10m Wind Speed",
                "units": "m/s",
                "transform": "vector_magnitude",
                "components": {"u": _height("10u", 10), "v": _height("10v", 10)},
                "value_range": (0.0, 80.0),
            },
            {
                "key": "wind_dir_10m",
                "name": "10m Wind Direction",
                "units": "deg",
                "transform": "vector_direction",
                "components": {"u": _height("10u", 10), "v": _height("10v", 10)},
                "value_range": (0.0, 360.0),
            },
        ],
        "groups": [
            {
                "key": "temp-pressure",
                "name": "Temperature & Pressure",
                "variable_keys": ["2t", "msl", "sp", "tp"],
            },
            {
                "key": "wind",
                "name": "10m Wind",
                "variable_keys": ["10u", "10v", "wind_speed_10m", "wind_dir_10m"],
            },
        ],
    },
    "ecmwf-aifs-pressure-levels": {
        "name": "Pressure Level Variables",
        "time_resolution": "hourly",
        "is_forecast": True,
        "variables": [
            *_pl_vars("t", "Temperature", "K", value_range=(173.0, 333.0)),
            *_pl_vars("u", "U Wind Component", "m/s", value_range=(-120.0, 120.0)),
            *_pl_vars("v", "V Wind Component", "m/s", value_range=(-120.0, 120.0)),
            *_pl_vars("z", "Geopotential", "m2 s-2"),
            *_pl_vars("q", "Specific Humidity", "kg kg-1", value_range=(0.0, 0.04)),
        ],
        "groups": [
            {
                "key": f"pl-{lv}",
                "name": f"{lv} hPa",
                "variable_keys": [
                    f"t_{lv}",
                    f"u_{lv}",
                    f"v_{lv}",
                    f"z_{lv}",
                    f"q_{lv}",
                ],
            }
            for lv in _PRESSURE_LEVELS
        ],
    },
}

RUN_HOUR_CHOICES = [
    (0, "00Z"),
    (6, "06Z"),
    (12, "12Z"),
    (18, "18Z"),
]


def default_run_hours():
    return [0, 12]


class ECMWFAIFSDataFeedForm(WagtailAdminModelForm):
    run_hours = forms.MultipleChoiceField(
        choices=RUN_HOUR_CHOICES,
        widget=forms.CheckboxSelectMultiple,
        required=False,
        initial=[0, 12],
    )
    
    def clean_run_hours(self):
        # Convert list of strings → list of ints for the ArrayField
        return [int(v) for v in self.cleaned_data.get("run_hours", [])]


class ECMWFAIFSDataFeed(DataFeed, TimeStampedModel):
    """
    ECMWF AIFS Loader profile:
      - Select which model runs to fetch from (00Z, 06Z, 12Z, 18Z)
      - Select forecast day range
      - Each day includes 4 timesteps: +0h, +6h, +12h, +18h
    """
    
    base_form_class = ECMWFAIFSDataFeedForm
    
    # Which runs to fetch from
    run_hours = ArrayField(
        models.IntegerField(choices=RUN_HOUR_CHOICES),
        default=default_run_hours,
        help_text="Which model runs to fetch from (e.g., 00Z and 12Z are most common)",
    )
    
    # Forecast range
    start_day = models.IntegerField(
        default=0,
        validators=[MinValueValidator(0), MaxValueValidator(15)],
        help_text="Forecast start day (0 = analysis time)",
    )
    
    end_day = models.IntegerField(
        default=5,
        validators=[MinValueValidator(0), MaxValueValidator(15)],
        help_text="Forecast end day (max 15)",
    )
    
    display_timezone = TimeZoneField(
        default="Africa/Nairobi",
    )
    
    panels = [
        *DataFeed.base_panels,
        MultiFieldPanel(
            [
                FieldPanel("run_hours"),
            ],
            heading="Model Runs",
        ),
        MultiFieldPanel(
            [
                FieldPanel("start_day"),
                FieldPanel("end_day"),
                FieldPanel("display_timezone"),
            ],
            heading="Forecast Range",
        ),
    ]
    
    class Meta:
        verbose_name = "ECMWF AIFS Data Feed"
    
    def clean(self):
        super().clean()
        if self.start_day > self.end_day:
            from django.core.exceptions import ValidationError
            raise ValidationError({
                "end_day": "End day must be greater than or equal to start day."
            })
    
    # ======================================================
    # Core logic
    # ======================================================
    
    MAX_STEP = 360
    STEP_INTERVAL = 6
    HOURS_IN_DAY = [0, 6, 12, 18]
    
    def get_run_hours(self):
        """Returns selected run hours, or all if none selected."""
        if self.run_hours:
            return sorted(self.run_hours)
        return [0, 6, 12, 18]  # Default to all runs
    
    def compute_steps(self):
        """
        Convert day range → list of forecast step hours, capped at 360h.
        
        Example:
            start_day=0, end_day=2 → [0, 6, 12, 18, 24, 30, 36, 42, 48, 54, 60, 66]
        """
        steps = []
        
        for day in range(self.start_day, self.end_day + 1):
            base_hour = day * 24
            for hour_offset in self.HOURS_IN_DAY:
                step = base_hour + hour_offset
                if step <= self.MAX_STEP:
                    steps.append(step)
        
        return steps
    
    def valid_times(self, run_utc=None):
        """
        Returns user-friendly timestamps for each step.
        """
        if run_utc is None:
            run_utc = timezone.now().replace(
                hour=0, minute=0, second=0, microsecond=0
            ).astimezone(ZoneInfo("UTC"))
        
        tz = ZoneInfo(self.display_timezone)
        
        output = []
        for step in self.compute_steps():
            valid_utc = run_utc + timedelta(hours=step)
            valid_local = valid_utc.astimezone(tz)
            
            output.append({
                "step": step,
                "valid_utc": valid_utc,
                "valid_local": valid_local,
                "label": f"{valid_local:%a %d %b %H:%M} (T+{step}h)",
            })
        
        return output
    
    def __str__(self):
        runs = ", ".join(f"{h:02d}Z" for h in self.get_run_hours())
        return f"{self.name} ({runs}, Day {self.start_day}–{self.end_day})"
    
    @classmethod
    def get_wizard_defaults(cls) -> dict:
        return {
            "run_hours": [0, 12],
            "start_day": 0,
            "end_day": 5,
        }
    
    @classmethod
    def get_catalog_defaults(cls) -> dict:
        return {
            "name": "ECMWF AIFS",
            "file_format": "grib2",
            "description": "ECMWF AIFS global forecast — 0.25° resolution, 6-hourly steps.",
        }
    
    @classmethod
    def get_collection_definitions(cls) -> list[CollectionDefinition]:
        return parse_collection_defs(COLLECTIONS)
    
    @property
    def data_source_cls(self):
        from .source import ECMWFAIFSDataSource
        return ECMWFAIFSDataSource
    
    def get_loader_config(self):
        """Get loader configuration dictionary."""
        return {
            "run_hours": self.get_run_hours(),
            "forecast_hours": self.compute_steps(),
        }
