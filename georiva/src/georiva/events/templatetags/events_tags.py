from django import template
from django.utils import timezone
from django.utils.dateformat import format as date_format

register = template.Library()


@register.filter
def recent_time(value):
    """``14:05`` for today, ``10-23 08:15`` before it — a dashboard line has no room for the year."""
    local = timezone.localtime(value)
    return date_format(local, "H:i" if local.date() == timezone.localdate() else "m-d H:i")


@register.inclusion_tag("georivaevents/_history_panel.html")
def history_panel(history):
    """Render a :class:`georiva.events.history.History` as expandable rows."""
    return {"history": history}
