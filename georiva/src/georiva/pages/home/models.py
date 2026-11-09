from django.db import models
from modelcluster.fields import ParentalKey
from modelcluster.models import ClusterableModel
from wagtail.admin.forms import WagtailAdminModelForm
from wagtail.admin.panels import FieldPanel, HelpPanel, InlinePanel, MultiFieldPanel
from wagtail.contrib.settings.models import BaseSiteSetting, register_setting
from wagtail.models import Orderable, Page
from wagtail_color_panel.edit_handlers import NativeColorPanel
from wagtail_color_panel.fields import ColorField

from georiva.organisations.lookups import PAGE_TREE, via_related

from .widgets import PortalAppSelect


class GeoRivaSettingsForm(WagtailAdminModelForm):
    def is_valid(self):
        # After the formsets have been cleaned, not in clean(): the parent form
        # is cleaned first, when no row has cleaned_data yet.
        valid = super().is_valid()
        return self._one_row_per_app() and valid

    def _one_row_per_app(self):
        """One row per app: a second would leave the editor guessing which of two
        contradicting rows the portal obeys."""
        seen = set()
        unique = True
        for form in self.formsets["app_entries"].forms:
            data = getattr(form, "cleaned_data", None) or {}
            plugin = data.get("plugin")
            if not plugin or data.get("DELETE"):
                continue
            if plugin in seen:
                form.add_error("plugin", "This app already has a row above.")
                unique = False
            seen.add(plugin)
        return unique


@register_setting
class GeoRivaSettings(BaseSiteSetting, ClusterableModel):
    # Per-Site settings, and the Site *is* the organisation: the one-to-one on
    # Organisation gives these rows a real ORM path to their owner.
    ORGANISATION_LOOKUP = "site__organisation"

    site_name = models.CharField(max_length=255, default="GeoRiva", help_text="The name of your GeoRiva instance")
    site_tagline = models.CharField(
        max_length=500, blank=True, help_text="A short description or tagline for your GeoRiva instance"
    )
    org_logo = models.ForeignKey(
        "wagtailimages.Image",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="+",
        help_text="Logo for your organization (recommended size: 200x50px)",
        verbose_name="Organization Logo",
    )
    org_name = models.CharField(
        max_length=255, blank=True, help_text="Name of your organization", verbose_name="Organization Name"
    )
    org_website = models.URLField(
        blank=True, max_length=200, help_text="URL to your organization's website", verbose_name="Organization Website"
    )
    accent_color = ColorField(default="#00c9b1", help_text="Hex code for the accent color (e.g., #00c9b1)")
    map_viewer_url = models.CharField(max_length=200, default="/map/", help_text="URL to the map viewer (e.g., /map/)")
    stac_browser_url = models.CharField(
        max_length=200, default="/stac-browser/", help_text="URL to the STAC browser (e.g., /stac-browser/)"
    )

    panels = [
        MultiFieldPanel(
            [
                FieldPanel("site_name"),
                FieldPanel("site_tagline"),
            ],
            heading="Site Identity",
        ),
        MultiFieldPanel(
            [
                FieldPanel("org_name"),
                FieldPanel("org_logo"),
                FieldPanel("org_website"),
            ],
            heading="Organization Info",
        ),
        MultiFieldPanel(
            [
                FieldPanel("map_viewer_url"),
                FieldPanel("stac_browser_url"),
            ]
        ),
        MultiFieldPanel(
            [
                NativeColorPanel("accent_color"),
            ],
            heading="Appearance",
        ),
        MultiFieldPanel(
            [
                HelpPanel(
                    content=(
                        "Every app installed on this instance is listed in the navbar's Apps menu and on the "
                        "home page. Add a row only to change that for this site: to hide an app, reword it, "
                        "give it another icon, or move it up. Apps with a row come first, in row order."
                    )
                ),
                InlinePanel("app_entries", label="App"),
            ],
            heading="Apps",
        ),
    ]

    base_form_class = GeoRivaSettingsForm

    class Meta:
        verbose_name = "GeoRiva Settings"
        verbose_name_plural = "GeoRiva Settings"


class HomePage(Page):
    # No max_count: there is one HomePage per organisation, each the root of its
    # own portal, all siblings under the Wagtail root node.

    # Pages are org-owned through the Site → root-page link rather than a field
    # (decision #261), so the owner is decided by tree position.
    ORGANISATION_LOOKUP = PAGE_TREE

    subpage_types = ["datasets.DatasetsIndexPage"]

    # --- Hero Section ---
    hero_heading = models.CharField(max_length=255, default="GeoRiva")
    hero_background_image = models.ForeignKey(
        "wagtailimages.Image",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="+",
        help_text=(
            "Satellite or weather imagery used as the hero background. "
            "Recommended: 1920×1080px minimum, JPEG. "
            "If not set, a built-in fallback gradient is used."
        ),
    )
    hero_subheading = models.CharField(max_length=500, blank=True)
    hero_show_search = models.BooleanField(default=True)

    # --- Stats Bar ---
    stats_show_collection_count = models.BooleanField(default=True)
    stats_show_catalog_count = models.BooleanField(default=True)
    stats_show_last_updated = models.BooleanField(default=True)
    stats_custom_label = models.CharField(max_length=100, blank=True)
    stats_custom_value = models.CharField(max_length=100, blank=True)

    # --- Apps ---
    apps_heading = models.CharField(
        max_length=255,
        default="Apps",
        help_text="Which apps are listed, and how, is set in Settings → GeoRiva Settings → Apps.",
    )

    # --- Featured Catalogs ---
    featured_heading = models.CharField(max_length=255, default="Featured Datasets")

    content_panels = Page.content_panels + [
        MultiFieldPanel(
            [
                FieldPanel("hero_heading"),
                FieldPanel("hero_subheading"),
                FieldPanel("hero_background_image"),
                FieldPanel("hero_show_search"),
            ],
            heading="Hero",
        ),
        MultiFieldPanel(
            [
                FieldPanel("stats_show_collection_count"),
                FieldPanel("stats_show_catalog_count"),
                FieldPanel("stats_show_last_updated"),
                FieldPanel("stats_custom_label"),
                FieldPanel("stats_custom_value"),
            ],
            heading="Stats Bar",
        ),
        MultiFieldPanel([FieldPanel("apps_heading")], heading="Apps"),
        MultiFieldPanel(
            [
                FieldPanel("featured_heading"),
                InlinePanel("featured_catalogs", label="Featured Catalogs", max_num=6),
            ],
            heading="Featured Catalogs",
        ),
    ]


class FeaturedCatalog(Orderable):
    # Owned by its page's organisation, which pages reach by tree position
    # rather than by a field.
    ORGANISATION_LOOKUP = via_related("page")

    page = ParentalKey("home.HomePage", on_delete=models.CASCADE, related_name="featured_catalogs")
    catalog = models.ForeignKey("georivacore.Catalog", on_delete=models.CASCADE)
    override_description = models.TextField(blank=True)

    panels = [
        FieldPanel("catalog"),
        FieldPanel("override_description"),
    ]


class PortalAppEntry(Orderable):
    """One site's word on one installed app: hide it, restate it, or move it up.

    Only exceptions are stored. An app with no row is shown, as its plugin
    describes it, after the apps that have one — so installing a plugin needs
    nothing from any site's editors, and uninstalling one leaves a row the
    portal quietly skips.

    Keyed by plugin name rather than by the app's mount label, because the label
    is the operator's to rename (ADR 0039) and a rename should not detach an
    editor's choices from the app they were about.
    """

    ORGANISATION_LOOKUP = "settings__site__organisation"

    settings = ParentalKey(GeoRivaSettings, on_delete=models.CASCADE, related_name="app_entries")
    plugin = models.CharField(max_length=255, verbose_name="App")
    hidden = models.BooleanField(default=False, help_text="Leave this app out of the navbar and the home page.")
    title = models.CharField(max_length=100, blank=True, help_text="Leave blank to use the app's own name.")
    description = models.CharField(
        max_length=255, blank=True, help_text="Leave blank to use the app's own description."
    )
    icon = models.CharField(
        max_length=64,
        blank=True,
        help_text=(
            'A Bootstrap Icons name, such as "cloud-sun" (icons.getbootstrap.com). Leave blank for the app\'s own.'
        ),
    )

    panels = [
        FieldPanel("plugin", widget=PortalAppSelect),
        FieldPanel("hidden"),
        FieldPanel("title"),
        FieldPanel("description"),
        FieldPanel("icon"),
    ]

    class Meta(Orderable.Meta):
        verbose_name = "app"
