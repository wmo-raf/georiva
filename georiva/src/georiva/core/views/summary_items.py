from django.conf import settings
from django.urls import reverse
from django.utils.translation import gettext_lazy as _
from wagtail.admin.site_summary import SummaryItem

from georiva.core.models import Catalog, Collection
from georiva.organisations.access import scoped_queryset


class _CountSummaryItem(SummaryItem):
    """Base for a dashboard tile showing a count, a label and a link.

    Subclasses set ``label_singular`` / ``label_plural`` and ``icon_name`` and
    implement ``get_count`` / ``get_link``.

    A tile is a headline for the listing it links to, so its number must be the
    number of rows that listing shows this organisation. Anything else quotes
    the neighbours' holdings back at an institution as if they were its own —
    see :meth:`scoped_count`."""

    template_name = "core/summary_count.html"
    icon_name = "doc-empty"
    label_singular = ""
    label_plural = ""

    def get_count(self):
        raise NotImplementedError

    def get_link(self):
        raise NotImplementedError

    def scoped_count(self, queryset):
        """``queryset`` narrowed to the organisation whose dashboard this is.

        Deliberately no ``is_active`` filter: the tiles link to the Catalog
        accordion, which lists active and inactive rows alike, and a headline
        that disagrees with the page under it is the bug this method exists to
        prevent.
        """
        return scoped_queryset(self.request, queryset).count()

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
        return self.scoped_count(Catalog.objects.all())

    def get_link(self):
        return reverse("catalog:index")


class CollectionSummaryItem(_CountSummaryItem):
    order = 110
    icon_name = "folder-open-inverse"
    label_singular = _("Collection")
    label_plural = _("Collections")

    def get_count(self):
        return self.scoped_count(Collection.objects.all())

    def get_link(self):
        return reverse("catalog:index")


class PluginSummaryItem(_CountSummaryItem):
    """Instance-wide on purpose, unlike its neighbours on the row.

    Plugins are installed into the container, not owned by anybody, and
    ``plugin_list`` offers every one of them to every organisation. The count
    therefore already equals what the page it links to shows; scoping it would
    make the tile disagree with its own listing to satisfy an aesthetic.
    """

    order = 120
    icon_name = "puzzle-piece"
    label_singular = _("Plugin")
    label_plural = _("Plugins")

    def get_count(self):
        return len(settings.GEORIVA_PLUGIN_NAMES)

    def get_link(self):
        return reverse("plugin_list")
