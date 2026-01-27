import json

from django import template

register = template.Library()


@register.filter(is_safe=True)
def to_json(value):
    """Convert a Python object to JSON string."""
    if value is None:
        return ''
    return json.dumps(value)
