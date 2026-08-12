import json
from urllib.parse import urlencode

from django import template
from django.conf import settings

from georiva import __version__
from georiva.core.models import Catalog, Item, Collection
from georiva.core.topics import topics_of
from georiva.organisations.access import scoped_queryset

register = template.Library()


def require_request(context):
    """The request a portal tag reads its organisation from.

    Every tag using this lists tenant rows, and a portal shows exactly one
    organisation's — the one its hostname resolved to. Rendering without a
    request would mean guessing, and guessing wrong means one institution's
    portal advertising another's holdings. So it refuses instead, and says why:
    a bare ``KeyError`` from a template is a long afternoon.
    """
    request = context.get("request")
    if request is None:
        raise RuntimeError(
            "This tag lists one organisation's data and needs the request to "
            "know which. Render through a RequestContext (or add "
            "django.template.context_processors.request)."
        )
    return request


def org_catalogs(context):
    """The active catalogs of the organisation this portal serves."""
    return scoped_queryset(require_request(context), Catalog.objects.filter(is_active=True))


def org_collections(context):
    """The active collections of the organisation this portal serves."""
    return scoped_queryset(require_request(context), Collection.objects.filter(is_active=True))


@register.simple_tag(takes_context=True)
def datasets_index_url(context):
    """The URL of *this* portal's DatasetsIndexPage, or '/datasets/' as fallback.

    Page trees are per organisation, so the index is found by descending from
    the request Site's root rather than by taking the first one on the instance
    — which, on a second tenant's host, would link away to somebody else's
    portal.
    """
    from wagtail.models import Site

    from georiva.pages.datasets.models import DatasetsIndexPage

    request = context.get("request")
    site = Site.find_for_request(request) if request is not None else None
    pages = DatasetsIndexPage.objects.live()
    if site is not None:
        pages = pages.descendant_of(site.root_page, inclusive=True)
    page = pages.first()
    return page.url if page else "/datasets/"


@register.filter(is_safe=True)
def to_json(value):
    """Convert a Python object to JSON string."""
    if value is None:
        return ""
    return json.dumps(value)


@register.filter
def django_settings(value):
    return getattr(settings, value, None)


@register.simple_tag
def georiva_version():
    return __version__


@register.filter
def get_item(dictionary, key):
    return dictionary.get(key)


@register.simple_tag(takes_context=True)
def get_latest_collections(context, limit=6):
    """Latest active collections ordered by most recently updated item."""
    return (
        org_collections(context)
        .select_related("catalog")
        .prefetch_related("catalog__topics")
        .order_by("-time_end", "-modified")[:limit]
    )


@register.simple_tag(takes_context=True)
def get_latest_catalogs(context, limit=6):
    """Active catalogs ordered by most recently updated item across their collections."""
    from django.db.models import Max

    return (
        org_catalogs(context)
        .prefetch_related("topics")
        .annotate(latest_updated=Max("collections__time_end"))
        .order_by("-latest_updated", "name")[:limit]
    )


@register.simple_tag(takes_context=True)
def get_active_topics(context):
    """Topics this organisation has at least one active catalog under.

    Topics themselves are instance-global shared reference data; which of them
    a portal offers is not.
    """
    return topics_of(org_catalogs(context))


# Landing page stats — used in stats_bar.html
@register.simple_tag(takes_context=True)
def get_landing_stats(context):
    """
    Returns a dict of live stats from Django ORM for the stats bar.
    Called once per page render — cheap queries.

    Counts this organisation's holdings: a portal that boasts the instance's
    totals is quoting its neighbours' numbers.
    """
    latest_item = (
        scoped_queryset(require_request(context), Item.objects.all()).order_by("-created").values("created").first()
    )

    return {
        "catalog_count": org_catalogs(context).count(),
        "collection_count": org_collections(context).count(),
        "last_updated": latest_item["created"] if latest_item else None,
    }


# -----------------------------------------------------------------------------
# All collections
# -----------------------------------------------------------------------------


@register.simple_tag(takes_context=True)
def get_all_collections(context):
    return (
        org_collections(context)
        .select_related("catalog")
        .prefetch_related("variables", "catalog__topics")
        .order_by("catalog__name", "sort_order", "name")
    )


# -----------------------------------------------------------------------------
# Catalog icon — maps file format to Bootstrap Icon class
# -----------------------------------------------------------------------------

FORMAT_ICONS = {
    "grib2": "bi-wind",
    "netcdf": "bi-grid-3x3",
    "geotiff": "bi-image",
    "zarr": "bi-database",
}


@register.simple_tag
def get_catalog_icon(file_format):
    """
    Returns a Bootstrap Icon class string for the given file format.
    Falls back to a generic layers icon.
    """
    return FORMAT_ICONS.get(file_format, "bi-layers")


# -----------------------------------------------------------------------------
# Active collection count for a catalog — used in featured_catalogs.html
# -----------------------------------------------------------------------------


@register.simple_tag
def active_collection_count(catalog):
    """Returns the number of active collections in a catalog."""
    return catalog.collections.filter(is_active=True).count()


@register.simple_tag(takes_context=True)
def get_active_time_resolutions(context):
    """Only resolutions used by at least one of this organisation's collections."""
    from georiva.core.models import Collection

    active_values = (
        org_collections(context).exclude(time_resolution="").values_list("time_resolution", flat=True).distinct()
    )
    # Return as (value, label) tuples preserving TimeResolution order
    choices = dict(Collection.TimeResolution.choices)
    return [(value, choices[value]) for value in Collection.TimeResolution.values if value in active_values]


@register.simple_tag(takes_context=False)
def query_params(filters, **kwargs):
    """
    Build a query string from the current filters dict,
    overriding with any kwargs passed in.
    Drops empty values and always resets page to 1 when
    a filter changes (unless page is explicitly passed).
    """
    from urllib.parse import urlencode

    params = {k: v for k, v in filters.items() if v}
    params.update({k: v for k, v in kwargs.items() if v != ""})
    # reset to page 1 when any filter other than page changes
    if "page" not in kwargs:
        params.pop("page", None)
    return urlencode(params)


@register.simple_tag(takes_context=True)
def query_string_replace(context, key, value):
    """
    Return the current query string with `key` set to `value`.
    All other parameters are preserved.

    Usage:
        <a href="?{% query_string_replace 'page' 3 %}">Page 3</a>
    """
    request = context.get("request")
    params = request.GET.copy() if request else {}
    params[key] = value
    return params.urlencode()


@register.simple_tag(takes_context=True)
def query_string_drop(context, *keys):
    """
    Return the current query string with the given keys removed.
    All other parameters are preserved.

    Usage:
        <a href="?{% query_string_drop 'date' 'page' %}">Clear date</a>
    """
    request = context.get("request")
    params = request.GET.copy() if request else {}
    for key in keys:
        params.pop(key, None)
    return params.urlencode()


@register.simple_tag(name="titiler_preview_url")
def titiler_preview_url_tag(item, variable_slug):
    """
    Build a TiTiler preview.webp URL for a given item and variable.

    Catalog, collection and organisation all come from the item rather than from
    the template's context variables: a page that had picked up one of the three
    from somewhere else could address one organisation's catalog under another's
    org segment, and the thumbnail would quietly render the wrong tenant's data.

    Usage:
        {% titiler_preview_url item active_var_slug as thumb_url %}
        <img src="{{ thumb_url }}">
    """
    from georiva.core.machine_plane import titiler_preview_url

    return titiler_preview_url(item, variable_slug)


@register.filter
def item_display_time(item, time_resolution):
    return item.display_time(time_resolution)
