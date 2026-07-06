"""
DataFeed for {{ cookiecutter.project_name }}.

A `DataFeed` is the operator-facing model (registered as a Wagtail snippet).
GeoRiva auto-discovers every `DataFeed` subclass and builds its admin form and
setup wizard — you do NOT register anything in apps.py.

The plugin contract a DataFeed must satisfy:
  * get_collection_definitions() — which collections and variables this plugin
    can create (declared below as the COLLECTIONS dict).
  * get_catalog_defaults() — pre-fills the wizard's catalog step.
  * data_source_cls — the BaseDataSource (source.py) that fetches the data.
  * get_loader_config() — feed-wide settings merged into the source's config.

Optionally, a feed can declare **derived products** — layers computed from its
collections by a registered recipe (anomaly, climatology, promotion, …):
  * get_derived_products() — the list of DerivedProductDefinitions this feed
    offers (ADR-0008/0009). It is an *instance* method (a product's inputs bind
    to this feed's actual collections). See the commented skeleton at the bottom
    of the DataFeed below, and docs/plugins/derived-products.md for the full
    contract. In the chain DAG, products are edges and collections are nodes.
"""

from django.db import models
from django_extensions.db.models import TimeStampedModel
from wagtail.admin.panels import FieldPanel, MultiFieldPanel
from wagtail.snippets.models import register_snippet

from georiva.sources.collection_definitions import CollectionDefinition, parse_collection_defs
from georiva.sources.models import DataFeed

# ---------------------------------------------------------------------------
# Collections this plugin can create — the canonical spec for the plugin.
# One entry per collection; each lists the variables it exposes.
#
#   time_resolution : one of sub_hourly, hourly, 3hourly, 6hourly, 12hourly,
#                     daily, pentadal, dekadal, monthly, seasonal, annual, ...
#   variables[].source : how the variable is located in the raw file, e.g. a
#                     GeoTIFF band name ("band_1"), or a GRIB short-name / level
#                     dict. See the georiva-source-* reference plugins.
# ---------------------------------------------------------------------------
COLLECTIONS = {
    "example-collection": {
        "name": "Example Collection",
        "time_resolution": "daily",
        # "default_interval_minutes": 1440,   # pre-fills the collection's schedule
        "variables": [
            {
                "key": "example_var",
                "name": "Example Variable",
                "source_units": "",          # raw unit of the source data (required)
                # "output_units": "",        # optional; exposed unit if converting
                "source_variable": "band_1",
                # "value_range": (0.0, 100.0),
            },
        ],
        # "groups": [   # optional UX grouping of variables in the wizard
        #     {"key": "group-a", "name": "Group A", "variable_keys": ["example_var"]},
        # ],
    },
}


@register_snippet
class {{ cookiecutter.project_module|replace('_', ' ')|title|replace(' ', '') }}DataFeed(DataFeed, TimeStampedModel):
    """Operator configuration for a {{ cookiecutter.project_name }} acquisition."""

    # Add feed-wide configuration fields here (credentials, timeouts, bbox...).
    # Example:
    # head_timeout = models.IntegerField(
    #     default=20,
    #     help_text="HTTP timeout (seconds) for source requests.",
    # )

    panels = [
        *DataFeed.base_panels,
        # MultiFieldPanel([FieldPanel("head_timeout")], heading="Advanced"),
    ]

    class Meta:
        verbose_name = "{{ cookiecutter.project_name }} Data Feed"

    # -- plugin contract ----------------------------------------------------

    @classmethod
    def get_collection_definitions(cls) -> list[CollectionDefinition]:
        return parse_collection_defs(COLLECTIONS)

    @classmethod
    def get_catalog_defaults(cls) -> dict:
        return {
            "name": "{{ cookiecutter.project_name }}",
            "file_format": "geotiff",   # geotiff | grib2 | netcdf
            "description": "{{ cookiecutter.project_description }}",
        }

    @property
    def data_source_cls(self):
        from .source import {{ cookiecutter.project_module|replace('_', ' ')|title|replace(' ', '') }}DataSource
        return {{ cookiecutter.project_module|replace('_', ' ')|title|replace(' ', '') }}DataSource

    def get_loader_config(self) -> dict:
        """Feed-wide settings merged into the data source's config dict."""
        return {}

    # -- derived products (optional) ----------------------------------------
    #
    # Uncomment and adapt to declare layers computed from this feed's
    # collections by a registered recipe. Delete this block if the plugin only
    # ingests raw data. Full guide: docs/plugins/derived-products.md.
    #
    # get_derived_products() is an *instance* method: a product's InputRef /
    # OutputRef bind to this feed's actual collection slugs. Return one
    # DerivedProductDefinition per product the feed offers.
    #
    # def get_derived_products(self):
    #     from georiva.core.derived_products import (
    #         ConfigField, DerivedProductDefinition, InputRef, OutputRef,
    #     )
    #     return [
    #         DerivedProductDefinition(
    #             key="example-anomaly",          # unique per feed; the origin/config key
    #             recipe_type="my-anomaly",       # a recipe registered via @recipe_registry.register
    #             label="Example anomaly",        # shown in the wizard + chain panel
    #             description="Departure from the climatological normal.",
    #             # Operator options -> the wizard form + validation. Types:
    #             # str | int | float | bool | choice (choice requires `choices`).
    #             config_schema=(
    #                 ConfigField(key="min_years", type="int", default=30),
    #                 ConfigField(key="quantity", type="choice",
    #                             choices=("anomaly", "value"), default="anomaly"),
    #             ),
    #             # Declared inputs (not buried in the recipe). tier is "staging"
    #             # (loader-fed, pre-publish) or "published". A required input at
    #             # the *published* tier that names another product's output creates
    #             # a dependency edge (anomaly -> climatology).
    #             inputs=(
    #                 InputRef(role="value", collection="example-collection", tier="staging"),
    #                 InputRef(role="baseline", collection="example-collection-climatology",
    #                          tier="published", required=True),
    #             ),
    #             # Output collections this product materialises. title/description/
    #             # visibility drive the catalog Collection created on enable
    #             # (get-or-create only, so operator renames survive). visibility is
    #             # "public" (served) or "internal" (a derivation intermediate).
    #             outputs=(
    #                 OutputRef(role="anomaly", collection="example-collection-anomaly",
    #                           title="Example anomaly",
    #                           description="Absolute departure from the normal.",
    #                           visibility="public"),
    #             ),
    #             # "event" (fire on each arriving input), "scheduled" (interval), or
    #             # "manual" (operator-triggered only).
    #             trigger_mode="event",
    #             # Pre-ticked in the wizard's opt-in step (default True).
    #             default_enabled=True,
    #             # Extra non-data-flow dependencies the tier rule can't infer
    #             # (usually unnecessary — a published-tier input gives the edge).
    #             depends_on=(),
    #         ),
    #     ]
