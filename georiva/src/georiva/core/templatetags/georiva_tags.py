import json

from django import template
from django.conf import settings

from georiva import __version__
from georiva.core.models import Catalog, Item, Collection

register = template.Library()


@register.filter(is_safe=True)
def to_json(value):
    """Convert a Python object to JSON string."""
    if value is None:
        return ''
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


@register.simple_tag
def get_active_topics():
    """Topics that have at least one active catalog."""
    from georiva.core.models import Topic
    return (
        Topic.objects
        .filter(catalogs__is_active=True)
        .distinct()
        .order_by('sort_order', 'name')
    )


# Landing page stats — used in stats_bar.html
@register.simple_tag
def get_landing_stats():
    """
    Returns a dict of live stats from Django ORM for the stats bar.
    Called once per page render — cheap queries.
    """
    catalog_count = Catalog.objects.filter(is_active=True).count()
    collection_count = Collection.objects.filter(is_active=True).count()
    
    latest_item = (
        Item.objects.order_by('-created')
        .values('created')
        .first()
    )
    
    return {
        'catalog_count': catalog_count,
        'collection_count': collection_count,
        'last_updated': latest_item['created'] if latest_item else None,
    }


# -----------------------------------------------------------------------------
# All collections
# -----------------------------------------------------------------------------

@register.simple_tag
def get_all_collections():
    return (
        Collection.objects
        .filter(is_active=True)
        .select_related('catalog')
        .prefetch_related('variables', 'catalog__topics')
        .order_by('catalog__name', 'sort_order', 'name')
    )


# -----------------------------------------------------------------------------
# Catalog icon — maps file format to Bootstrap Icon class
# -----------------------------------------------------------------------------

FORMAT_ICONS = {
    'grib2': 'bi-wind',
    'netcdf': 'bi-grid-3x3',
    'geotiff': 'bi-image',
    'zarr': 'bi-database',
}


@register.simple_tag
def get_catalog_icon(file_format):
    """
    Returns a Bootstrap Icon class string for the given file format.
    Falls back to a generic layers icon.
    """
    return FORMAT_ICONS.get(file_format, 'bi-layers')


# -----------------------------------------------------------------------------
# Active collection count for a catalog — used in featured_catalogs.html
# -----------------------------------------------------------------------------

@register.simple_tag
def active_collection_count(catalog):
    """Returns the number of active collections in a catalog."""
    return catalog.collections.filter(is_active=True).count()


@register.simple_tag
def get_active_time_resolutions():
    """Only resolutions used by at least one active collection."""
    from georiva.core.models import Collection
    active_values = (
        Collection.objects
        .filter(is_active=True)
        .exclude(time_resolution='')
        .values_list('time_resolution', flat=True)
        .distinct()
    )
    # Return as (value, label) tuples preserving TimeResolution order
    choices = dict(Collection.TimeResolution.choices)
    return [
        (value, choices[value])
        for value in Collection.TimeResolution.values
        if value in active_values
    ]
