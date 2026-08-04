"""The dispatcher: one place that turns a tenancy declaration into an answer.

Every model in this codebase says where it stands on tenancy (ADR 0011). This
module is what reads those declarations — all of them, including the two that no
single ``.filter()`` can express — and it offers exactly two entry points:

* :func:`scope_rows`, for rows the ORM has yet to fetch;
* :func:`belongs_to_active_org`, for an object already in hand.

Both walk the same vocabulary, so a surface cannot be scoped one way in its
listing and another way in its detail view, and a model that gains a declaration
becomes scopable on every surface at once rather than on the ones somebody
remembers. Both — and :func:`is_scopable` beside them — switch exactly once, on
``lookups.kind_of``, so a kind added to the vocabulary is a branch to add in one
place rather than a cascade three functions have to keep agreeing about.

Where the two halves *could* legitimately differ, they are made to agree
deliberately, and each of those places says so: a null link is nobody's on both
sides, and a subject whose model belongs everywhere is admitted on both sides
without either half resolving the row.

The kinds it dispatches on, and what each resolves to:

* an **ORM path** to Organisation — the common case, handed to
  ``access.scoped_queryset`` / ``access.require_org_object``, global tier and
  all;
* :data:`~.lookups.ORGANISATION_SELF` — the organisation itself, matched by
  identity, likewise ``access``'s;
* :data:`~.lookups.PAGE_TREE` — a Wagtail page, judged by the tree it sits in
  rather than by a field, because that is how a page is owned (ADR 0016);
* :func:`~.lookups.via_related` — the row's owner is whoever owns the object at
  the end of an FK path. Resolved by asking *that* model's declaration, which is
  why a page log entry and a page-child orderable both come out as tree tests
  without either of them naming a tree;
* :func:`~.lookups.via_content_object` — the row's subject is polymorphic, named
  by a content type and an object id. Resolved by splitting the rows by content
  type and scoping each part by that part's own declaration, recursively;
* :data:`~.lookups.SHARED_REFERENCE_DATA` — belongs everywhere, passed through.
  Every model from outside this codebase reads as shared without having said so
  (``lookups.OWN_MODULE_PREFIX``), so a workflow over some third-party package's
  snippet is admitted rather than scoped;
* :data:`~.lookups.NOT_ORM_SCOPABLE`, or **no declaration at all** — **refused**,
  loudly. Silence is not consent: a dispatcher that guessed "admit" would be an
  invisible leak, and one that guessed "refuse" would hide an institution's own
  work from it. The sweep in ``tests/test_fail_closed.py`` is what makes
  refusing safe — no model in this codebase reaches production undeclared.

Models we did not write cannot carry a declaration, and four of Wagtail's
matter: the two audit logs and the two workflow tables are exactly the rows the
admin's reports and dashboard panels list. :data:`EXTERNAL_DECLARATIONS` speaks
for them, in one table, rather than each surface narrowing them by hand.

This module imports both the access helpers and the page-tree helpers, which is
why it is neither of them: ``pages`` already imports ``access``, and folding this
into ``access`` would close that circle.
"""
from functools import reduce
from operator import or_

from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import (
    FieldDoesNotExist,
    ImproperlyConfigured,
    ObjectDoesNotExist,
)
from django.db.models import Model, Q
from django.http import Http404
from wagtail.models import Page

from .access import (
    LOOKUP_ATTR,
    NO_ROUTE,
    ORGANISATION_SELF,
    ORM_PATH,
    PAGE_TREE,
    SHARED_REFERENCE_DATA,
    VIA_CONTENT_OBJECT,
    VIA_RELATED,
    content_object_fields,
    declared_lookup,
    kind_of,
    related_path,
    require_org_object,
    require_writable_org_object,
    scoped_queryset,
    via_content_object,
    via_related,
)
from .pages import org_root_page, page_is_in_org_tree, scope_pages

#: Tenancy declarations for models we did not write and therefore cannot annotate.
#:
#: Only models whose rows this instance's admin actually lists belong here. Each
#: is a decision with the same weight as one written on a model, and the sweep
#: checks these against the real fields exactly as it checks our own.
#:
#: Wagtail's two workflow tables disagree about which content-type column names
#: the subject — ``WorkflowState`` keys its generic relation on
#: ``base_content_type`` so that a workflow over a specific page type is found by
#: the base type — which is why the field names are part of the declaration.
EXTERNAL_DECLARATIONS = {
    # A page audit-log entry carries a real foreign key to its page.
    "wagtailcore.PageLogEntry": via_related("page"),
    # A snippet audit-log entry names its subject generically.
    "wagtailcore.ModelLogEntry": via_content_object("content_type", "object_id"),
    # Workflow state: the subject under moderation, page or snippet.
    "wagtailcore.WorkflowState": via_content_object("base_content_type", "object_id"),
    # One task within that workflow — owned by whatever the state is owned by.
    "wagtailcore.TaskState": via_related("workflow_state"),
}


def declaration_of(model):
    """The tenancy declaration governing ``model``.

    What the model declared, what :data:`EXTERNAL_DECLARATIONS` declares on its
    behalf, or — for a page class from Wagtail or a third-party package, which
    could not have declared anything and would otherwise read as shared — the
    tree rule that governs every page on this instance regardless of who wrote
    the class.
    """
    declared = getattr(model, LOOKUP_ATTR, None)
    if declared:
        return declared
    external = EXTERNAL_DECLARATIONS.get(model._meta.label)
    if external:
        return external
    if issubclass(model, Page):
        return PAGE_TREE
    return declared_lookup(model)


def scope_rows(request, queryset, _seen=frozenset()):
    """``queryset`` narrowed to the rows the organisation serving ``request`` owns.

    The queryset-level entry point, and the one every admin surface should call:
    it understands everything the vocabulary can express, where
    ``access.scope_or_pass`` understands only the ORM-path half.

    ``_seen`` guards the recursion. A declaration may delegate to another model's
    declaration, so a pair of models pointing at each other would otherwise
    recurse forever; the second visit to a model raises rather than looping.
    """
    model = queryset.model
    declared = declaration_of(model)
    kind = kind_of(declared)

    if kind == SHARED_REFERENCE_DATA:
        return queryset
    if kind == PAGE_TREE:
        return scope_pages(request, queryset)
    if kind in (ORM_PATH, ORGANISATION_SELF):
        return scoped_queryset(request, queryset)
    if kind == NO_ROUTE:
        raise _no_route(
            model, declared, "Its rows cannot be listed on a scoped surface."
        )

    seen = _visit(model, _seen)
    if kind == VIA_RELATED:
        return _scope_via_related(request, queryset, related_path(declared), seen)
    return _scope_via_content_object(
        request, queryset, *content_object_fields(declared), seen
    )


def _scope_via_related(request, queryset, path, seen):
    """Rows whose related object at ``path`` survived that object's own rule.

    One subquery. A row whose relation is null is dropped: a null means the row
    belongs to nobody, which is never everybody — the same reading
    ``access.organisation_of`` gives a null along a declared path.
    """
    target = _related_model(queryset.model, path)
    if kind_of(declaration_of(target)) == SHARED_REFERENCE_DATA:
        # Every row of the target belongs everywhere, so no subquery is needed —
        # but a null link is still nobody's, and dropping those here is what
        # keeps this agreeing with the object half, which reads a null as False.
        return queryset.filter(**{f"{path}__isnull": False})
    scoped = scope_rows(request, target._default_manager.all(), seen)
    return queryset.filter(**{f"{path}__in": scoped})


def _scope_via_content_object(request, queryset, content_type_field, object_id_field, seen):
    """Rows split by content type, each part scoped by that type's own rule.

    The one place the dispatcher materialises anything. A generic key stores the
    subject's id in a character column, so it cannot be joined against the
    subject's own primary key in the database — the surviving ids have to be
    fetched and sent back as literals. The cost is bounded by how much of a
    *subject* type one organisation owns rather than by the size of the table
    being scoped, and the surfaces that use this list moderation queues and audit
    trails, which are small. If that stops being true the fix is a stored
    denormalised owner, not a cleverer filter.

    A content type whose model has been uninstalled resolves to nothing, and its
    rows are dropped: they name a subject nobody can produce, so no organisation
    can be shown to own them.
    """
    content_type_ids = set(
        queryset.order_by().values_list(f"{content_type_field}_id", flat=True).distinct()
    )
    clauses = []
    for content_type in ContentType.objects.filter(pk__in=content_type_ids):
        subject = content_type.model_class()
        if subject is None:
            continue
        here = Q(**{f"{content_type_field}_id": content_type.pk})
        if kind_of(declaration_of(subject)) == SHARED_REFERENCE_DATA:
            # Admitted whole, without an id list: every row of the subject
            # belongs everywhere, and listing its primary keys would be the one
            # place this scales with a *shared* table rather than an owned one.
            # The object half admits the same rows on the same terms.
            clauses.append(here)
            continue
        allowed = scope_rows(request, subject._default_manager.all(), seen)
        clauses.append(
            here & Q(**{
                f"{object_id_field}__in": [
                    str(pk) for pk in allowed.order_by().values_list("pk", flat=True)
                ]
            })
        )
    if not clauses:
        return queryset.none()
    return queryset.filter(reduce(or_, clauses))


def _visit(model, seen):
    """``seen`` with ``model`` added, unless it is already there.

    A declaration may delegate to another model's declaration, so two models
    pointing at each other would recurse until the stack ran out. The second
    visit raises instead, naming the model somebody has to fix.
    """
    if model._meta.label in seen:
        raise ImproperlyConfigured(
            f"{model._meta.label} reaches its organisation through a cycle of "
            f"{LOOKUP_ATTR} declarations; one of them has to name a real owner."
        )
    return seen | {model._meta.label}


def _related_model(model, path):
    """The model at the end of a ``via_related`` path, or a configuration error."""
    for step in path.split("__"):
        try:
            field = model._meta.get_field(step)
        except FieldDoesNotExist:
            field = None
        if field is None or field.related_model is None:
            raise ImproperlyConfigured(
                f"{model._meta.label} declares a relation to {path!r}, but {step!r} is not "
                f"a relation on {model._meta.label}."
            )
        model = field.related_model
    return model


def belongs_to_active_org(request, obj, _seen=frozenset()):
    """Whether ``obj`` belongs to the organisation serving ``request``.

    The object-level entry point, for surfaces that hand over objects rather than
    a queryset — the dashboard's workflow panels materialise their results before
    anybody can filter them, and what they hold is reached through a generic
    foreign key that no ORM path crosses.

    Raises ``ImproperlyConfigured`` for a model that has declared no route to an
    organisation — see this module's docstring for why that is not a ``False``.

    The answer for a model with an ORM path comes from the enforcing helper
    itself, for the reason ``access.may_change_org_object`` gives: what a surface
    shows and what a surface admits cannot then drift apart. The global tier of a
    nullable-owner model is admitted there, so it is admitted here.
    """
    model = type(obj)
    declared = declaration_of(model)
    kind = kind_of(declared)

    if kind == SHARED_REFERENCE_DATA:
        return True
    if kind == PAGE_TREE:
        return page_is_in_org_tree(org_root_page(request), obj.path, obj.depth)
    if kind in (ORM_PATH, ORGANISATION_SELF):
        try:
            require_org_object(request, obj)
        except Http404:
            return False
        return True
    if kind == NO_ROUTE:
        raise _no_route(model, declared, "Whether this row is yours cannot be answered.")

    seen = _visit(model, _seen)
    if kind == VIA_RELATED:
        path = related_path(declared)
        _related_model(model, path)  # a mistyped path is a loud error, not a False
        related = _walk(obj, path)
        # A null anywhere along the path, or a row that has gone: nobody's,
        # never everybody's — the same reading the queryset half gives, where
        # such a row matches no subquery and is excluded outright.
        return False if related is None else belongs_to_active_org(request, related, seen)

    content_type_field, object_id_field = content_object_fields(declared)
    subject_model = _subject_model(obj, content_type_field)
    if subject_model is None:
        # A content type whose model has gone: the subject names nothing.
        return False
    if kind_of(declaration_of(subject_model)) == SHARED_REFERENCE_DATA:
        # Admitted without resolving the row at all, which is exactly what the
        # queryset half does with the whole content type. Resolving it here
        # would make the two halves disagree about a dangling id — and would
        # also be the one place this module fetches a row it does not need.
        return True
    subject = subject_model._default_manager.filter(
        pk=getattr(obj, object_id_field, None)
    ).first()
    return False if subject is None else belongs_to_active_org(request, subject, seen)


def _walk(obj, path):
    """The object at the end of ``path``, or ``None`` if the route breaks.

    Both ways it can break count as broken: a null link, and a foreign key whose
    row is not there. The second is not hypothetical — Wagtail's page log entry
    carries its page under ``db_constraint=False`` and ``DO_NOTHING``, so an
    entry can outlive the page it describes. The caller has already checked that
    every step is a real relation, so nothing here can be a typo.
    """
    for step in path.split("__"):
        try:
            obj = getattr(obj, step)
        except ObjectDoesNotExist:
            return None
        if obj is None:
            return None
    return obj


def _subject_model(obj, content_type_field):
    """The model a generic key points at, or ``None`` if it points at nothing.

    Read from the content-type column the declaration names rather than through a
    generic-foreign-key accessor: Wagtail calls one ``content_object`` on the
    workflow tables and defines none at all on the model log, so the columns are
    the only reliable route. An uninstalled model resolves to ``None``.
    """
    content_type = getattr(obj, content_type_field, None)
    return content_type.model_class() if content_type is not None else None


def _no_route(model, declared, consequence):
    """The refusal both entry points give a model that reaches no organisation.

    One function so the two cannot describe the same condition differently; the
    ``consequence`` is the half that legitimately differs — a listing cannot be
    filtered, a single row cannot be judged.
    """
    return ImproperlyConfigured(
        f"{model._meta.label} declares {LOOKUP_ATTR} = {declared!r}, which names no way "
        f"to reach an organisation. {consequence}"
    )


def require_active_org_object(request, obj, *, writable=False):
    """Return ``obj`` if this request's organisation owns it, else 404.

    The enforcing form of :func:`belongs_to_active_org`, so a view that resolved
    an object by pk can refuse it. ``writable`` selects the tighter rule for the
    one model family that has two: a global-tier row is readable by every
    organisation and writable only by the instance admin. Nothing outside the
    ORM-path kinds has a global tier, so for everything else the two agree.
    """
    if writable and kind_of(declaration_of(type(obj))) in (ORM_PATH, ORGANISATION_SELF):
        return require_writable_org_object(request, obj)
    if not belongs_to_active_org(request, obj):
        raise Http404("No such object in this organisation.")
    return obj


def is_scopable(model):
    """Whether the dispatcher knows how to narrow ``model``'s rows.

    For the sweep, and for a caller that would rather ask than catch. It reads
    the same ``kind_of`` the two entry points switch on, and names the one kind
    they both refuse — so it cannot come to answer a question they answer
    differently.
    """
    if not (isinstance(model, type) and issubclass(model, Model)):
        return False
    return kind_of(declaration_of(model)) != NO_ROUTE
