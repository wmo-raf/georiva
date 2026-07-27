"""The single place a request's organisation decides what it may see.

Row-level tenancy on this instance is one rule applied everywhere: a request
serves exactly one organisation — the one its Host resolved to — and may only
read and write rows belonging to it. This module is that rule, and every admin
surface owning tenant data is expected to reach it through one of these helpers
rather than filter by hand. One implementation means one place to audit, and a
guard test that walks the admin can hold the whole instance to it.

Three properties are deliberate:

*Deny by default.* A model becomes scopable by declaring ``ORGANISATION_LOOKUP``
— the ORM path from itself to ``Organisation``. A model that has not declared
one cannot be scoped at all: :func:`scoped_queryset` raises rather than quietly
returning every row. Forgetting to think about a model is therefore a loud
error, not a silent leak.

*The host, not the session.* ``request.active_org`` is set by
``OrganisationMiddleware`` from the hostname on every request; nothing here
trusts a session value or a submitted id.

*Superusers skip the membership gate, not the host.* The instance admin may
enter any organisation's admin — that is what ``is_superuser`` buys, and the
middleware already grants it the ADMIN role everywhere. It does not turn the
admin into a cross-tenant view: on Kenya's host a superuser reads and writes
Kenya's rows, so nothing they create there can be filed under another
institution's storage prefix. Reaching another organisation's data means
visiting its host.
"""
from django.core.exceptions import ImproperlyConfigured, PermissionDenied
from django.db.models import Model, QuerySet
from django.http import Http404
from django.shortcuts import get_object_or_404

from .models import OrganisationMembership

#: Class attribute a model declares to name the ORM path from itself to the
#: ``Organisation`` that owns it — ``"organisation"`` on the tenancy root,
#: ``"catalog__organisation"`` one step down, and so on.
LOOKUP_ATTR = "ORGANISATION_LOOKUP"


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


def organisation_lookup(model):
    """The ORM path from ``model`` to its owning organisation.

    Raises ``ImproperlyConfigured`` for a model that has not declared one. That
    is the deny-by-default hinge: a new org-owned model is unusable with these
    helpers until somebody writes down how it reaches its organisation.
    """
    lookup = getattr(model, LOOKUP_ATTR, None)
    if not lookup:
        raise ImproperlyConfigured(
            f"{model._meta.label} cannot be scoped to an organisation: it declares no "
            f"{LOOKUP_ATTR}. Add one naming the ORM path to Organisation, or keep the "
            f"model off org-scoped admin surfaces."
        )
    return lookup


def organisation_of(obj):
    """The organisation owning ``obj``, or ``None`` if the route is broken.

    A ``None`` anywhere along the declared path (a nullable FK left unset) means
    the row belongs to no organisation, and callers treat that as "nobody's" —
    never as "everybody's".
    """
    value = obj
    for step in organisation_lookup(type(obj)).split("__"):
        value = getattr(value, step, None)
        if value is None:
            return None
    return value


def scoped_queryset(request, queryset):
    """``queryset`` narrowed to the organisation serving ``request``."""
    organisation = require_active_org(request)
    return queryset.filter(**{organisation_lookup(queryset.model): organisation})


def require_org_object(request, obj):
    """Return ``obj`` if this request's organisation owns it, else 404.

    The check for objects already in hand — an object resolved before the
    organisation was known, or one carried across from an earlier request (a
    second browser tab left open on another organisation's host, a wizard
    session that outlived a switch). A foreign row is reported as absent rather
    than forbidden: which catalogs another institution runs is not this
    organisation's business.
    """
    organisation = require_active_org(request)
    if organisation_of(obj) != organisation:
        raise Http404("No such object in this organisation.")
    return obj


def get_org_object_or_404(request, klass, *args, **kwargs):
    """``get_object_or_404`` that can only ever find this organisation's rows.

    ``klass`` is a model or a queryset, exactly as with Django's own helper, so
    a call site keeps any narrowing it already had (``is_active=True``, a
    ``select_related``) and gains the organisation filter.
    """
    queryset = klass if isinstance(klass, QuerySet) else klass._default_manager.all()
    return get_object_or_404(scoped_queryset(request, queryset), *args, **kwargs)


def get_via_scoped_parent_or_404(queryset, *args, **kwargs):
    """Resolve a row whose only route to an organisation is a parent already scoped.

    A few records — a derivation run, a file-ingestion row — are pipeline
    bookkeeping with no usable FK chain to an organisation of their own (the one
    link they have is nullable, so scoping on it would hide rows that simply have
    not produced anything yet). They are only ever reached *through* an object
    this request has already resolved under :func:`get_org_object_or_404`, and
    the filter that reaches them descends from it.

    This is that case, named. It is deliberately not a synonym for Django's
    ``get_object_or_404``: writing it asserts the parent was scoped, and the
    guard test that forbids bare ``get_object_or_404`` in org-owned apps allows
    exactly this exception.
    """
    return get_object_or_404(queryset, *args, **kwargs)


def org_role(request):
    """The requesting user's role in the organisation serving the request."""
    return getattr(request, "active_org_role", None)


def is_org_admin(request):
    return org_role(request) == OrganisationMembership.Role.ADMIN


def require_org_member(request):
    """Deny the request unless its user belongs to the organisation it dials.

    The middleware re-reads the membership row on every request, so a revoked
    membership fails here on the very next one rather than at logout.
    """
    if org_role(request) is None:
        raise PermissionDenied("You are not a member of this organisation.")


def require_org_admin(request):
    """Deny the request unless its user administers the organisation it dials.

    The gate on org-management surfaces (member accounts, roles, organisation
    settings) as opposed to data work, which every member does.
    """
    if not is_org_admin(request):
        raise PermissionDenied("This action is restricted to organisation admins.")


def scope_form_fields(request, form):
    """Narrow every org-owned choice on ``form`` to this organisation's rows.

    Covers both halves of a relation field: an operator is only offered their own
    organisation's objects, and a posted id for anyone else's fails validation
    rather than being saved. Fields over models that declare no route to an
    organisation (shared reference data — topics, units, boundaries) are left
    alone.
    """
    for field in form.fields.values():
        queryset = getattr(field, "queryset", None)
        if queryset is None or not isinstance(queryset, QuerySet):
            continue
        if not getattr(queryset.model, LOOKUP_ATTR, None):
            continue
        field.queryset = scoped_queryset(request, queryset)
    return form


def is_org_owned(model):
    """Whether ``model`` has declared a route to an owning organisation."""
    return isinstance(model, type) and issubclass(model, Model) and bool(
        getattr(model, LOOKUP_ATTR, None)
    )
