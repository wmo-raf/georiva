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

Scoping is skipped for models that declare no route to an organisation
(``organisations.access.is_org_owned``): shared reference data such as topics,
units and administrative boundaries is instance-global on purpose.
"""
from django.core.exceptions import ImproperlyConfigured
from django.db.models import QuerySet

from .access import (
    is_org_owned,
    require_org_object,
    scope_form_fields,
    scoped_queryset,
)

#: Suffix of every attribute on a viewset naming a view class.
VIEW_CLASS_SUFFIX = "_view_class"


class OrgScopedViewMixin:
    """Narrows every queryset and form a Wagtail generic view resolves.

    Mixed in ahead of the view class, so each override delegates to Wagtail's own
    implementation and then filters — nothing about how a view builds its
    queryset (annotations, ``select_related``, custom base querysets) is
    reimplemented here.
    """

    def scope(self, queryset):
        if queryset is None or not is_org_owned(queryset.model):
            return queryset
        return scoped_queryset(self.request, queryset)

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
        if obj is None or not is_org_owned(type(obj)):
            return obj
        return require_org_object(self.request, obj)

    # -- forms -------------------------------------------------------------

    def get_form(self, *args, **kwargs):
        return scope_form_fields(self.request, super().get_form(*args, **kwargs))


class OrgScopedChooseMixin:
    """The chooser modal's result list."""

    def get_object_list(self):
        return scoped_queryset(self.request, super().get_object_list())


class OrgScopedChosenMixin:
    """``chosen/<pk>/`` — the half of a chooser that never reads the list.

    Without this a scoped modal is cosmetic: the pk endpoint is a plain URL, and
    one from another organisation's admin resolves happily.
    """

    def get_object(self, pk):
        return require_org_object(self.request, super().get_object(pk))


class OrgScopedChosenMultipleMixin:
    def get_objects(self, pks):
        return scoped_queryset(self.request, super().get_objects(pks))


def view_class_attrs(viewset):
    """Every attribute on ``viewset`` that names a view class, declared or passed."""
    return sorted(
        attr
        for attr in set(dir(type(viewset))) | set(viewset.__dict__)
        if attr.endswith(VIEW_CLASS_SUFFIX)
    )


def _subclass(view_class, mixin):
    return type(f"OrgScoped{view_class.__name__}", (mixin, view_class), {})


def org_scoped_view(view_class, mixin=OrgScopedViewMixin):
    """``view_class`` with row scoping mixed in, built once per class."""
    if view_class is None or issubclass(view_class, mixin):
        return view_class
    return _subclass(view_class, mixin)


class OrgScopedViewSetMixin:
    """Scopes every view a ``ModelViewSet``/``SnippetViewSet`` exposes.

    Wrapping happens at construction and writes onto the instance, so a viewset
    constructed with ``SomeViewSet(index_view_class=…)`` has its override scoped
    too — the kwarg has already landed on ``__dict__`` by then.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for attr in view_class_attrs(self):
            # Read the raw attribute rather than `getattr(self, …)`: some view
            # classes are cached properties whose evaluation needs a configured
            # model, and construction is too early for that. Those resolve to a
            # descriptor here and are left alone.
            view_class = self.__dict__.get(attr, getattr(type(self), attr, None))
            if isinstance(view_class, type):
                self.__dict__[attr] = org_scoped_view(view_class)


class OrgScopedChooserViewSetMixin:
    """Scopes both halves of a ``ChooserViewSet``."""

    CHOOSER_MIXINS = {
        "choose_view_class": OrgScopedChooseMixin,
        "choose_results_view_class": OrgScopedChooseMixin,
        "chosen_view_class": OrgScopedChosenMixin,
        "chosen_multiple_view_class": OrgScopedChosenMultipleMixin,
        "create_view_class": OrgScopedViewMixin,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for attr, mixin in self.CHOOSER_MIXINS.items():
            view_class = getattr(self, attr, None)
            if isinstance(view_class, type):
                self.__dict__[attr] = org_scoped_view(view_class, mixin)
