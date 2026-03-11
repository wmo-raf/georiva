from django.db import models
from modelcluster.fields import ParentalKey
from wagtail.admin.panels import FieldPanel, MultiFieldPanel, InlinePanel
from wagtail.contrib.settings.models import BaseSiteSetting, register_setting
from wagtail.models import Page, Orderable
from wagtail_color_panel.edit_handlers import NativeColorPanel
from wagtail_color_panel.fields import ColorField


@register_setting
class GeoRivaSettings(BaseSiteSetting):
    site_name = models.CharField(max_length=255, default="GeoRiva", help_text="The name of your GeoRiva instance")
    site_tagline = models.CharField(max_length=500, blank=True,
                                    help_text="A short description or tagline for your GeoRiva instance")
    org_logo = models.ForeignKey('wagtailimages.Image', null=True, blank=True, on_delete=models.SET_NULL,
                                 related_name='+', help_text="Logo for your organization (recommended size: 200x50px)",
                                 verbose_name="Organization Logo")
    org_name = models.CharField(max_length=255, blank=True, help_text="Name of your organization",
                                verbose_name="Organization Name")
    org_website = models.URLField(blank=True, max_length=200, help_text="URL to your organization's website",
                                  verbose_name="Organization Website")
    accent_color = ColorField(default="#00c9b1", help_text="Hex code for the accent color (e.g., #00c9b1)")
    map_viewer_url = models.CharField(max_length=200, default="/map/", help_text="URL to the map viewer (e.g., /map/)")
    stac_browser_url = models.CharField(max_length=200, default="/stac-browser/",
                                        help_text="URL to the STAC browser (e.g., /stac-browser/)")
    
    panels = [
        MultiFieldPanel([
            FieldPanel('site_name'),
            FieldPanel('site_tagline'),
        ], heading="Site Identity"),
        MultiFieldPanel([
            FieldPanel('org_name'),
            FieldPanel('org_logo'),
            FieldPanel('org_website'),
        ], heading="Organization Info"),
        MultiFieldPanel([
            FieldPanel('map_viewer_url'),
            FieldPanel('stac_browser_url'),
        ]),
        MultiFieldPanel([
            NativeColorPanel('accent_color'),
        ], heading="Appearance"),
    ]
    
    class Meta:
        verbose_name = "GeoRiva Settings"
        verbose_name_plural = "GeoRiva Settings"


class HomePage(Page):
    max_count = 1
    
    # --- Hero Section ---
    hero_heading = models.CharField(max_length=255, default="GeoRiva")
    hero_background_image = models.ForeignKey(
        'wagtailimages.Image',
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name='+',
        help_text=(
            "Satellite or weather imagery used as the hero background. "
            "Recommended: 1920×1080px minimum, JPEG. "
            "If not set, a built-in fallback gradient is used."
        )
    )
    hero_subheading = models.CharField(max_length=500, blank=True)
    hero_show_search = models.BooleanField(default=True)
    
    # --- Stats Bar ---
    stats_show_collection_count = models.BooleanField(default=True)
    stats_show_catalog_count = models.BooleanField(default=True)
    stats_show_last_updated = models.BooleanField(default=True)
    stats_custom_label = models.CharField(max_length=100, blank=True)
    stats_custom_value = models.CharField(max_length=100, blank=True)
    
    # --- Featured Catalogs ---
    featured_heading = models.CharField(max_length=255, default="Featured Datasets")
    
    content_panels = Page.content_panels + [
        MultiFieldPanel([
            FieldPanel('hero_heading'),
            FieldPanel('hero_subheading'),
            FieldPanel('hero_background_image'),
            FieldPanel('hero_show_search'),
        ], heading="Hero"),
        MultiFieldPanel([
            FieldPanel('stats_show_collection_count'),
            FieldPanel('stats_show_catalog_count'),
            FieldPanel('stats_show_last_updated'),
            FieldPanel('stats_custom_label'),
            FieldPanel('stats_custom_value'),
        ], heading="Stats Bar"),
        MultiFieldPanel([
            FieldPanel('featured_heading'),
            InlinePanel('featured_catalogs', label="Featured Catalogs", max_num=6),
        ], heading="Featured Catalogs"),
    ]


class FeaturedCatalog(Orderable):
    page = ParentalKey(
        'home.HomePage',
        on_delete=models.CASCADE,
        related_name='featured_catalogs'
    )
    catalog = models.ForeignKey(
        'georivacore.Catalog',
        on_delete=models.CASCADE
    )
    override_description = models.TextField(blank=True)
    
    panels = [
        FieldPanel('catalog'),
        FieldPanel('override_description'),
    ]
