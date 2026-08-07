"""Wiring the access rule into Wagtail's admin, one seam per kind of surface.

Wagtail resolves rows in four places, and all four have to be closed or the
others are decoration:

* listings (``get_base_queryset`` / ``get_queryset``) — what an operator sees;
* single-object views (``get_base_object_queryset``) — edit, delete, inspect,
  history, usage, reorder, all of which take a pk straight from the URL;
* choosers, in *both* halves — the modal's result list and the ``chosen/<pk>/``
  endpoint that turns a pk back into an object without ever consulting the list;
* forms — the related-object fields an operator picks from, which are also what
  validates a posted id.

The mixins below are deliberately blunt: they wrap every view class a viewset
exposes rather than an enumerated few, so a Wagtail upgrade that adds a view, or
a viewset that overrides one, is scoped by default rather than by remembering.

Every seam here goes through the dispatcher in ``organisations.ownership``, so a
viewset over pages, over a page's children, or over a workflow's rows is scoped
by the same declarations as a viewset over a catalog. Scoping is skipped only
for models that declare themselves shared reference data (topics, units,
administrative boundaries): instance-global on purpose. A model that has
declared nothing is refused, not passed through.
"""
from django.core.exceptions import ImproperlyConfigured
from django.db.models import QuerySet

from .access import scope_form_fields
from .ownership import is_scopable, require_active_org_object, scope_rows

#: Suffix of every attribute on a viewset naming a view class.
VIEW_CLASS_SUFFIX = "_view_class"

#: The view classes that only ever read. Everything else a viewset exposes is
#: treated as a write, so a view added by a Wagtail upgrade lands on the stricter
#: side by default.
#:
#: The distinction is invisible for everything an organisation owns outright —
#: both mixins scope to the same rows. It decides one thing: whether the global
#: tier of a model that has one (colour ramps) is readable here but not
#: editable. See ``access.require_writable_org_object``.
READ_ONLY_VIEW_CLASS_ATTRS = frozenset({
    "index_view_class",
    "index_results_view_class",
    "inspect_view_class",
    "history_view_class",
    "usage_view_class",
    "revisions_compare_view_class",
    "revisions_view_class",
    "workflow_history_view_class",
    "workflow_history_detail_view_class",
})


class OrgScopedViewMixin:
    """Narrows every queryset and form a Wagtail generic view resolves.

    Mixed in ahead of the view class, so each override delegates to Wagtail's own
    implementation and then filters — nothing about how a view builds its
    queryset (annotations, ``select_related``, custom base querysets) is
    reimplemented here.
    """

    def scope(self, queryset):
        """Narrowed by whatever the model declared, refused if it declared
        nothing the dispatcher understands — see ``ownership.scope_rows``."""
        if queryset is None:
            return queryset
        return scope_rows(self.request, queryset)

    # -- listings ----------------------------------------------------------

    def get_base_queryset(self):
        return self.scope(super().get_base_queryset())

    def get_queryset(self):
        queryset = super().get_queryset()
        if isinstance(queryset, QuerySet):
            return self.scope(queryset)
        # A listing that is searching returns the backend's results object, not
        # a queryset — the search already ran, over the base queryset this mixin
        # scoped on the way in, and filtering its output would ask the search
        # backend to filter on a field it has no reason to index. Scoping is
        # therefore upstream here, and only here: a view that reached search
        # results without a base queryset was never scoped at all.
        if not hasattr(super(), "get_base_queryset"):
            raise ImproperlyConfigured(
                f"{type(self).__name__} returns search results from get_queryset() but has no "
                f"get_base_queryset() for OrgScopedViewMixin to scope; its rows are unfiltered."
            )
        return queryset

    # -- single-object views -----------------------------------------------

    def get_base_object_queryset(self):
        return self.scope(super().get_base_object_queryset())

    def get_object(self, *args, **kwargs):
        """The backstop, for views that resolve a pk without a queryset.

        Wagtail's copy view is the live example: it calls Django's
        ``get_object_or_404(self.model, pk=…)`` directly and never consults the
        base queryset, so scoping that queryset does nothing for it. Checking the
        object every view actually returns closes that class of gap once, rather
        than per view as each is discovered.
        """
        obj = super().get_object(*args, **kwargs)
        if obj is None or not is_scopable(type(obj)):
            return obj
        return self.require_object(obj)

    def require_object(self, obj):
        """The check a resolved object must pass on this kind of view."""
        return require_active_org_object(self.request, obj)

    # -- forms -------------------------------------------------------------

    def get_form(self, *args, **kwargs):
        return scope_form_fields(self.request, super().get_form(*args, **kwargs))


class OrgScopedWriteViewMixin(OrgScopedViewMixin):
    """The same, for a view that changes what it resolves.

    Only the global tier tells the two apart: an organisation reads instance-wide
    reference data and does not edit it.
    """

    def require_object(self, obj):
        return require_active_org_object(self.request, obj, writable=True)


class OrgScopedChooseMixin:
    """The chooser modal's result list."""

    def get_object_list(self):
        return scope_rows(self.request, super().get_object_list())


class OrgScopedChosenMixin:
    """``chosen/<pk>/`` — the half of a chooser that never reads the list.

    Without this a scoped modal is cosmetic: the pk endpoint is a plain URL, and
    one from another organisation's admin resolves happily.
    """

    def get_object(self, pk):
        return require_active_org_object(self.request, super().get_object(pk))


class OrgScopedChosenMultipleMixin:
    def get_objects(self, pks):
        return scope_rows(self.request, super().get_objects(pks))


def view_class_attrs(viewset):
    """Every attribute on ``viewset`` that names a view class, declared or passed."""
    return sorted(
        attr
        for attr in set(dir(type(viewset))) | set(viewset.__dict__)
        if attr.endswith(VIEW_CLASS_SUFFIX)
    )


def mixin_for(attr):
    """The scoping mixin a view class named ``attr`` gets — read, or write."""
    return (
        OrgScopedViewMixin if attr in READ_ONLY_VIEW_CLASS_ATTRS
        else OrgScopedWriteViewMixin
    )


def _subclass(view_class, mixin):
    return type(f"OrgScoped{view_class.__name__}", (mixin, view_class), {})


def org_scoped_view(view_class, mixin=OrgScopedViewMixin):
    """``view_class`` with row scoping mixed in, built once per class."""
    if view_class is None or issubclass(view_class, mixin):
        return view_class
    return _subclass(view_class, mixin)


class OrgScopedViewSetMixinBase:
    """Rebinds a viewset's view classes to scoped subclasses at construction.

    Writing onto the instance rather than the class means a viewset constructed
    as ``SomeViewSet(index_view_class=…)`` has its override scoped too — the
    kwarg has already landed on ``__dict__`` by the time this runs.
    """

    #: attr name → mixin. Subclasses that leave this ``None`` scope every
    #: ``*_view_class`` the viewset exposes with the general view mixin.
    SCOPED_VIEW_CLASSES = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        wanted = self.SCOPED_VIEW_CLASSES or {
            attr: mixin_for(attr) for attr in view_class_attrs(self)
        }
        for attr, mixin in wanted.items():
            # Read the raw attribute rather than `getattr(self, …)`: some view
            # classes are cached properties whose evaluation needs a configured
            # model, and construction is too early for that. Those resolve to a
            # descriptor here and are left alone.
            view_class = self.__dict__.get(attr, getattr(type(self), attr, None))
            if isinstance(view_class, type):
                self.__dict__[attr] = org_scoped_view(view_class, mixin)


class OrgScopedViewSetMixin(OrgScopedViewSetMixinBase):
    """Scopes every view a ``ModelViewSet``/``SnippetViewSet`` exposes.

    Every one, rather than an enumerated few: a view added by a Wagtail upgrade
    is then scoped by default instead of by somebody remembering.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # A registered snippet gets a chooser of its own, built from a class
        # attribute and registered separately — so scoping the viewset's own
        # views leaves `/admin/snippets/choose/<app>/<model>/chosen/<pk>/` wide
        # open. Rebind the class the chooser is built from.
        chooser_class = getattr(self, "chooser_viewset_class", None)
        if isinstance(chooser_class, type):
            self.__dict__["chooser_viewset_class"] = org_scoped_chooser_viewset(chooser_class)


def org_scoped_chooser_viewset(viewset_class):
    """``viewset_class`` with chooser scoping mixed in, built once per class."""
    if issubclass(viewset_class, OrgScopedChooserViewSetMixin):
        return viewset_class
    return type(
        f"OrgScoped{viewset_class.__name__}",
        (OrgScopedChooserViewSetMixin, viewset_class),
        {},
    )


class OrgScopedChooserViewSetMixin(OrgScopedViewSetMixinBase):
    """Scopes both halves of a ``ChooserViewSet``.

    Named explicitly here because a chooser's seams are not the generic view's:
    its list and its ``chosen/<pk>/`` endpoint resolve rows by different methods.
    """

    SCOPED_VIEW_CLASSES = {
        "choose_view_class": OrgScopedChooseMixin,
        "choose_results_view_class": OrgScopedChooseMixin,
        "chosen_view_class": OrgScopedChosenMixin,
        "chosen_multiple_view_class": OrgScopedChosenMultipleMixin,
        "create_view_class": OrgScopedWriteViewMixin,
    }
