from django import template

register = template.Library()


@register.inclusion_tag("georivaevents/_history_panel.html")
def history_panel(history):
    """Render a :class:`georiva.events.history.History` as expandable rows."""
    return {"history": history}
