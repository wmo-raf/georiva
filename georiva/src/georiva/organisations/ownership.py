"""One question, asked of an object already in hand: is it this organisation's?

``access.scoped_queryset`` answers the same question for rows the ORM has yet to
fetch, and that is the right seam almost everywhere. It cannot help where a
surface hands over objects rather than a queryset — the admin dashboard's
workflow panels materialise their results before anybody can filter them, and
what they hold is reached through a generic foreign key that no ORM path
crosses. So the object-level half is named here, once, rather than hand-written
per surface.

It dispatches on how the object's model declares itself, and it is deliberately
the same vocabulary the queryset half reads:

* a Wagtail page is judged by the tree it sits in, because that — not a field —
  is how a page is owned (ADR 0016);
* a model naming an ORM path to Organisation is judged by walking it, with the
  global tier of a nullable-owner model counting as everybody's, exactly as
  ``access.require_org_object`` has it;
* a model declaring itself shared reference data belongs everywhere — including
  every model from outside this codebase, which ``lookups.OWN_MODULE_PREFIX``
  reads as shared without its having said so. A workflow over a snippet some
  third-party package defines is therefore admitted, not scoped;
* a model that has declared no ownership rule is **refused**, loudly. Silence is
  not consent here any more than it is in ``access.scope_or_pass``: a predicate
  that guessed ``True`` would be an invisible leak, and one that guessed
  ``False`` would hide an institution's own work from it.

This module imports both the access helpers and the page-tree helpers, which is
why it is neither of them: ``pages`` already imports ``access``, and folding this
into ``access`` would close that circle.

It is also where the general dispatcher belongs — the one that will let a model
declare a path to a page, or to a generic content object, so that a future
surface is scoped by declaration rather than by remembering (#296). That work
grows this file rather than relocating this function.
"""
from django.http import Http404
from wagtail.models import Page

from .access import is_shared_reference, require_org_object
from .pages import org_root_page, page_is_in_org_tree


def belongs_to_active_org(request, obj):
    """Whether ``obj`` belongs to the organisation serving ``request``.

    Raises ``ImproperlyConfigured`` for a model that has declared no route to an
    organisation — see this module's docstring for why that is not a ``False``.

    The answer for everything that is not a page comes from the enforcing helper
    itself, for the reason ``access.may_change_org_object`` gives: what a surface
    shows and what a surface admits cannot then drift apart. The global tier of a
    nullable-owner model is admitted there, so it is admitted here.
    """
    if isinstance(obj, Page):
        page = obj
        return page_is_in_org_tree(org_root_page(request), page.path, page.depth)
    if is_shared_reference(type(obj)):
        return True
    try:
        require_org_object(request, obj)
    except Http404:
        return False
    return True
