from django.conf import settings
from django.urls import reverse
from django.utils.translation import gettext_lazy as _
from wagtail.admin.site_summary import SummaryItem

from georiva.core.models import Catalog, Collection


class _CountSummaryItem(SummaryItem):
    """Base for a dashboard tile showing a count, a label and a link.

    Subclasses set ``label_singular`` / ``label_plural`` and ``icon_name`` and
    implement ``get_count`` / ``get_link``."""

    template_name = "core/summary_count.html"
    icon_name = "doc-empty"
    label_singular = ""
    label_plural = ""

    def get_count(self):
        raise NotImplementedError

    def get_link(self):
        raise NotImplementedError

    def get_context_data(self, parent_context):
        return {
            "count": self.get_count(),
            "label_singular": self.label_singular,
            "label_plural": self.label_plural,
            "icon_name": self.icon_name,
            "link": self.get_link(),
        }


class CatalogSummaryItem(_CountSummaryItem):
    order = 100
    icon_name = "globe"
    label_singular = _("Catalog")
    label_plural = _("Catalogs")

    def get_count(self):
        return Catalog.objects.count()

    def get_link(self):
        return reverse("catalog:index")


class CollectionSummaryItem(_CountSummaryItem):
    order = 110
    icon_name = "folder-open-inverse"
    label_singular = _("Collection")
    label_plural = _("Collections")

    def get_count(self):
        return Collection.objects.count()

    def get_link(self):
        return reverse("catalog:index")


class PluginSummaryItem(_CountSummaryItem):
    order = 120
    icon_name = "puzzle-piece"
    label_singular = _("Plugin")
    label_plural = _("Plugins")

    def get_count(self):
        return len(settings.GEORIVA_PLUGIN_NAMES)

    def get_link(self):
        return reverse("plugin_list")
