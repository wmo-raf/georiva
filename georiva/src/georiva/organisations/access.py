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

This module holds the ORM-path half of that vocabulary — the common case, and
the only one a single ``filter()`` can express. The kinds that need more than
one (a page judged by its tree, a row delegating to a related object, a
polymorphic subject) are read by the dispatcher in ``ownership``, which lands
back here for everything below.

Which half a caller wants is not a judgement call. **A surface calls the
dispatcher**, always: it is the only thing that can answer for every model, and
a helper here handed a page-shaped queryset refuses it. These are what the
dispatcher calls in turn, and what code already holding an ORM-path model — a
STAC view over collections, a template tag over catalogs — may call directly.
Nothing here decides *which* kind a model is; that question has one answer, in
``lookups.kind_of``, and one reader.

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
from django.db.models import Q, QuerySet
from django.http import Http404
from django.shortcuts import get_object_or_404

from .lookups import (  # re-exported so enforcement code has one import
    GLOBAL_TIER_ATTR,
    LOOKUP_ATTR,
    NO_ROUTE,
    NOT_ORM_SCOPABLE,
    ORGANISATION_SELF,
    ORM_PATH,
    OWN_MODULE_PREFIX,
    PAGE_TREE,
    SENTINELS,
    SHARED_REFERENCE_DATA,
    VIA_CONTENT_OBJECT,
    VIA_RELATED,
    content_object_fields,
    declared_lookup,
    has_global_tier,
    is_orm_path,
    kind_of,
    related_path,
    via_content_object,
    via_related,
)
from .models import OrganisationMembership


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

    Raises ``ImproperlyConfigured`` for anything that is not a path — including
    every declaration that is a decision rather than a route, since none of them
    describes something the ORM can walk in one filter. That is the
    deny-by-default hinge: a model is unusable with *these* helpers until
    somebody writes down how it reaches its organisation, or writes down that it
    cannot. The declarations refused here are not necessarily unscopable; the
    dispatcher in ``ownership`` knows what to do with several of them, and it is
    what a surface should call.
    """
    declared = declared_lookup(model)
    if not is_orm_path(declared):
        raise ImproperlyConfigured(
            f"{model._meta.label} cannot be scoped by an ORM filter: it declares "
            f"{LOOKUP_ATTR} = {declared!r}. Give it the path to Organisation, or reach "
            f"its rows through a parent that has one."
        )
    return declared


def organisation_of(obj):
    """The organisation owning ``obj``, or ``None`` if the route is broken.

    A ``None`` anywhere along the declared path (a nullable FK left unset) means
    the row belongs to no organisation, and callers treat that as "nobody's" —
    never as "everybody's". The one model where nobody's *is* everybody's says so
    explicitly, with :data:`GLOBAL_TIER_ATTR`, and the callers below read that
    declaration rather than inferring anything from the ``None`` itself.
    """
    if declared_lookup(type(obj)) == ORGANISATION_SELF:
        return obj
    value = obj
    for step in organisation_lookup(type(obj)).split("__"):
        value = getattr(value, step, None)
        if value is None:
            return None
    return value


def scoped_queryset(request, queryset):
    """``queryset`` narrowed to the organisation serving ``request``.

    A model with a global tier (:data:`GLOBAL_TIER_ATTR`) is narrowed to this
    organisation's rows *plus* the ownerless ones — the shipped palettes an
    institution draws on alongside its own. Reading is where the two tiers meet;
    writing is not, and :func:`require_writable_org_object` keeps them apart.
    """
    organisation = require_active_org(request)
    model = queryset.model
    if declared_lookup(model) == ORGANISATION_SELF:
        return queryset.filter(pk=organisation.pk)
    lookup = organisation_lookup(model)
    if has_global_tier(model):
        return queryset.filter(Q(**{lookup: organisation}) | Q(**{f"{lookup}__isnull": True}))
    return queryset.filter(**{lookup: organisation})


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
    owner = organisation_of(obj)
    if owner is None and has_global_tier(type(obj)):
        # The global tier: owned by nobody, readable by everybody. See
        # :func:`scoped_queryset`.
        return obj
    if owner != organisation:
        raise Http404("No such object in this organisation.")
    return obj


def require_writable_org_object(request, obj):
    """Return ``obj`` if this request may *change* it, else 404.

    The same rule as :func:`require_org_object` for everything an organisation
    owns, and the tighter half of the global tier: a shipped palette is instance
    data, so an organisation reads it and only the instance admin edits it. A
    member is told it is not there to edit rather than forbidden, which is what
    "read-only tier" means from inside one organisation's admin.
    """
    if organisation_of(obj) is None and has_global_tier(type(obj)):
        user = getattr(request, "user", None)
        if user is not None and user.is_authenticated and user.is_superuser:
            return obj
        raise Http404("This is instance-wide reference data; it is read-only here.")
    return require_org_object(request, obj)


def may_change_org_object(request, obj):
    """Whether :func:`require_writable_org_object` would admit ``obj``.

    For listings, which would rather not offer an edit button that 404s. The
    answer comes from the enforcing helper itself, so what is shown and what is
    allowed cannot drift apart.
    """
    try:
        require_writable_org_object(request, obj)
    except Http404:
        return False
    return True


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


def resolve_org_role(organisation, user):
    """``user``'s live role in ``organisation``, read from the database, or ``None``.

    The membership rule itself, in one function, so the middleware and the
    serving planes cannot answer it differently. Never cached: a revoked
    membership must fail on the very next request rather than at logout.

    Superusers are the instance admin and hold the ADMIN role in every
    organisation without a membership row — see this module's docstring for why
    that is not a cross-tenant view.
    """
    if organisation is None:
        return None
    if user is None or not user.is_authenticated or not user.is_active:
        return None
    if user.is_superuser:
        return OrganisationMembership.Role.ADMIN
    membership = organisation.membership_for(user)
    return membership.role if membership else None


def org_role(request):
    """The requesting user's role in the organisation serving the request.

    Reads what ``OrganisationMiddleware`` resolved. That is the right source on
    the admin plane, where the identity is a session cookie and is therefore
    known before any middleware after ``AuthenticationMiddleware`` runs.
    """
    return getattr(request, "active_org_role", None)


def may_see_private(request):
    """Whether this request may be served the active organisation's private data.

    The serving-plane counterpart of :func:`require_org_member`, and the reason
    ``private`` collections can be filtered rather than forbidden: a caller this
    returns ``False`` for is simply not shown them, and a fetch by name 404s.

    It deliberately re-resolves the role from ``request.user`` instead of
    reading :func:`org_role`. On the public plane the identity is not always
    settled when the middleware runs: an API-key request arrives anonymous and
    only acquires its user when DRF authenticates it, inside the view. Reading
    the middleware's answer there would deny every key holder. Same rule, same
    :func:`resolve_org_role`, read at the moment the answer is used.
    """
    organisation = require_active_org(request)
    return resolve_org_role(organisation, getattr(request, "user", None)) is not None


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
    rather than being saved. Fields over shared reference data (topics, units,
    boundaries) are left alone; a field over a model that has declared neither
    raises, because a chooser is exactly where an undeclared model leaks.

    A Wagtail edit form is more than its own fields: an inline panel carries a
    formset whose rows are forms in their own right, with relation fields of
    their own — the variables edited inside a collection, and the palette each
    one picks. Those are narrowed too, by :func:`scope_formset_fields`.

    The dispatcher is imported here rather than at module level: ``ownership``
    reads this module and the page-tree module, so a module-level import would
    close the circle. These form helpers are the one place in ``access`` that has
    to reach the whole vocabulary — a relation field over pages is exactly where
    a page-shaped model would otherwise leak.
    """
    from .ownership import scope_rows

    for field in form.fields.values():
        queryset = getattr(field, "queryset", None)
        if queryset is None or not isinstance(queryset, QuerySet):
            continue
        field.queryset = scope_rows(request, queryset)
    for formset in getattr(form, "formsets", {}).values():
        scope_formset_fields(request, formset)
    return form


def scope_formset_fields(request, formset):
    """Narrow the choices in every row of an inline panel — present and future.

    Two halves, because a formset renders two kinds of row. The rows it has built
    are ordinary forms, and scoping them is what refuses a foreign id: on a POST
    those are the forms that validate what was submitted. The row the "add
    another" button clones is built fresh from the formset's form *class*, so
    narrowing it means shadowing that class per request — a subclass carrying its
    own copy of the fields, assigned to this formset instance only, because the
    class itself is shared by every request in the process.
    """
    for child_form in formset.forms:
        scope_form_fields(request, child_form)
    formset.form = _scoped_form_class(request, formset.form)


def _scoped_form_class(request, form_class):
    """``form_class`` with its relation choices narrowed, as a throwaway subclass.

    The fields are copied before being narrowed, and the copies are attached
    *after* the class exists: a form metaclass rebuilds ``base_fields`` from the
    declared fields and the model, so anything passed in the class body is
    discarded on the way through.
    """
    from copy import deepcopy

    from .ownership import scope_rows  # see :func:`scope_form_fields`

    scoped = type(form_class)(form_class.__name__, (form_class,), {})
    fields = {name: deepcopy(field) for name, field in form_class.base_fields.items()}
    for field in fields.values():
        queryset = getattr(field, "queryset", None)
        if isinstance(queryset, QuerySet):
            field.queryset = scope_rows(request, queryset)
    scoped.base_fields = fields
    return scoped
