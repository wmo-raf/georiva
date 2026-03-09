import json

from django import template
from django.conf import settings

from georiva import __version__

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
def adl_version():
    return __version__


@register.filter
def get_item(dictionary, key):
    return dictionary.get(key)
