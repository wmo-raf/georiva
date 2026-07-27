"""Reading a request's organisation, without ever guessing one.

That is all this module does today. It is the module row scoping
(`scoped_queryset`, `get_org_object_or_404`, `require_org_admin`) will be built
on, but none of that lives here yet — writers that must stamp an organisation on
what they create need only the helper below.
"""
from django.http import Http404


def require_active_org(request):
    """The organisation serving this request, or 404.

    ``OrganisationMiddleware`` already 404s unknown hosts, so a missing
    ``active_org`` here means an exempt (infrastructure) path reached a view that
    owns tenant data. That is a bug, and the request must not be answered from a
    guessed organisation — data written under the wrong ``{org}/`` prefix is
    misfiled national data.
    """
    organisation = getattr(request, "active_org", None)
    if organisation is None:
        raise Http404("This request is not served for any organisation.")
    return organisation
