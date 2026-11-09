from django import template

from georiva.pages.home.portal_apps import portal_apps as resolve_portal_apps

register = template.Library()


@register.simple_tag(takes_context=True)
def portal_apps(context):
    """The apps this portal links to — ``{% portal_apps as apps %}``.

    Empty without a request: which apps a portal lists is its site's choice, and
    a template rendered outside a request has no site to ask.
    """
    request = context.get("request")
    if request is None:
        return []
    return resolve_portal_apps(request)
