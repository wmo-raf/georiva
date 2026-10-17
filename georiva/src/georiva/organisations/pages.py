"""Per-org page trees: an organisation's admin sees its own portal and no other.

Pages are org-owned, but not through a field. Each organisation is provisioned
with a Site and a root page of its own (see ``provisioning``), and everything it
authors lives under that root — so the ownership question for a page is a tree
question, not an FK lookup, and pages declare ``PAGE_TREE`` rather than a path to
Organisation.

:func:`scope_pages` is what that declaration resolves to: the dispatcher in
``ownership`` calls it for every page-shaped model and for everything that
delegates to one. What the dispatcher cannot reach is Wagtail's own page views,
which take a pk from the URL and expose no queryset to narrow — so this module
stays the pages-shaped equivalent for four of the five seams:

* the explorer listing and the sidebar's page explorer, via Wagtail's
  ``construct_explorer_page_queryset`` hook;
* the page chooser, via ``construct_page_chooser_queryset``;
* page search, whose view is scoped by subclassing (Wagtail offers no hook);
* every admin URL naming a page by id — edit, delete, move, copy, history,
  add-child and the rest — via :class:`OrgPageTreeMiddleware`, because Wagtail's
  page views resolve their pk directly and expose no queryset to narrow.

The fifth is the admin dashboard, in ``dashboard.py``. It is separate because it
is the one page surface where Wagtail resolves pages with neither a queryset to
hook nor a page id in the URL — a panel does it inside a context method — which
is exactly why all four mechanisms above missed it.

Wagtail's own page permissions (each org's group holds page permissions over its
own root, granted at provisioning) already stop a member editing a tree they have
no rights in. This is the tenancy layer over that: a user who belongs to two
institutions, and a superuser who belongs to all of them, still see exactly one
organisation's pages on one organisation's host.
"""

from django.conf import settings
from django.contrib.auth import get_user_model
from django.http import Http404
from django.urls import Resolver404, resolve
from wagtail.admin.views.pages.search import SearchView
from wagtail.models import Page, PageLogEntry, Site

from .access import require_active_org

#: URL kwargs by which Wagtail's admin names a page. Every page view takes one of
#: these (``wagtail.admin.urls.pages`` plus the explore and choose-page routes),
#: which is what makes one check at the request boundary enough.
PAGE_ID_URL_KWARGS = frozenset(
    {
        "page_id",
        "parent_page_id",
        "page_to_move_id",
        "destination_id",
    }
)

#: The bulk-action route, the one page surface that names its pages in the query
#: string instead of the path (``/admin/bulk/wagtailcore/page/delete/?id=…``).
BULK_ACTION_URL_NAME = "wagtail_bulk_action"
BULK_ACTION_ID_PARAM = "id"

#: Routes that may name the tree's own root node. Listing it is how a superuser's
#: explorer starts and how the chooser browses in — and what it *contains* is
#: filtered by the hooks below. Everything else (editing it, adding a child under
#: it) would author outside every organisation's tree, so it is refused.
ROOT_NODE_URL_NAMES = frozenset(
    {
        "wagtailadmin_explore",
        "wagtailadmin_explore_results",
        "wagtailadmin_choose_page_child",
        "wagtailadmin_choose_page_search",
    }
)


def org_root_page(request):
    """The page that is the whole of this organisation's tree."""
    return require_active_org(request).root_page


def scope_pages(request, queryset):
    """``queryset`` narrowed to the pages under this organisation's root."""
    return queryset.descendant_of(org_root_page(request), inclusive=True)


#: The audit-log action a page edit is recorded under. Named because two
#: surfaces read it — the page-listing filter panel and the dashboard's
#: recent-edits panel — and a string that means something is worth one spelling.
PAGE_EDIT_ACTION = "wagtail.edit"


def scope_page_log_entries(request, queryset):
    """``queryset`` of page log entries narrowed to this organisation's tree.

    A page log entry carries a real foreign key to its page, so the tree
    narrowing reaches it through that — which is what lets the dashboard's
    recent-edits panel apply its row limit to this organisation's rows rather
    than to the instance's.

    This is what ``ownership``'s dispatcher resolves ``PageLogEntry`` to as well,
    from the ``via_related("page")`` it declares on Wagtail's behalf. It stays
    spelled out here because it is one layer down — ``ownership`` reads this
    module, not the other way round — and because the two callers that reach for
    it by name are building queries, not scoping listings.
    """
    return queryset.filter(page__in=scope_pages(request, Page.objects.all()))


def page_is_in_org_tree(root, path, depth, *, root_node_allowed=False):
    """Whether a page at treebeard ``path``/``depth`` belongs to ``root``'s tree.

    Compares materialised paths rather than issuing a query per page: treebeard
    encodes ancestry in the path itself, so a prefix test *is* the ancestor test.

    The tree's own root node (depth 1) passes only where ``root_node_allowed`` —
    the listing routes, because that is where a superuser's explorer starts and
    what it *contains* is filtered by the hooks below. It is nobody's page, so
    everywhere else (editing it, adding a child under it) is refused: a page
    authored there would sit outside every organisation's tree, belonging to
    nobody and served by no portal.
    """
    if depth == 1:
        return root_node_allowed
    return path.startswith(root.path)


def filter_organisation(request):
    """The organisation a filter panel is being built for, or ``None``.

    The filter helpers below degrade to empty choices instead of raising, unlike
    everything else here: django-filter builds a filterset in paths that carry no
    request, and a filter panel is not the place to 404. Nothing is exposed by
    degrading — an empty choice list filters nothing open.
    """
    return getattr(request, "active_org", None) if request is not None else None


def _org_sites(request):
    organisation = filter_organisation(request)
    if organisation is None:
        return Site.objects.none()
    return Site.objects.filter(pk=organisation.site_id)


def _org_page_owners(request):
    users = get_user_model().objects
    if filter_organisation(request) is None:
        return users.none()
    return users.filter(
        pk__in=scope_pages(request, Page.objects.all()).order_by().values_list("owner_id", flat=True).distinct()
    )


def _org_page_editors(request):
    users = get_user_model().objects
    if filter_organisation(request) is None:
        return users.none()
    return users.filter(
        pk__in=scope_page_log_entries(request, PageLogEntry.objects.filter(action=PAGE_EDIT_ACTION))
        .order_by()
        .values_list("user_id", flat=True)
        .distinct()
    )


#: Filters on Wagtail's page-listing filter panel whose choices are drawn from
#: the whole instance: the Site list is the roster of organisations, and the
#: owner and editor lists the roster of their staff.
PAGE_FILTER_QUERYSETS = {
    "site": _org_sites,
    "owner": _org_page_owners,
    "edited_by": _org_page_editors,
}


def scope_page_filters():
    """Narrow the page-listing filter panel's choices to this organisation.

    The panel is built from a filterset Wagtail declares as a class, with the
    Site choices fixed at import time as ``Site.objects.all()`` — every
    organisation's hostname, listed beside the pages of exactly one of them. No
    hook reaches it, so its filters' querysets are replaced here with the
    request-aware callables django-filter already supports.

    Called once from ``OrganisationsConfig.ready``.
    """
    from wagtail.admin.views.pages.listing import GenericPageFilterSet, PageFilterSet

    for filterset in (PageFilterSet, GenericPageFilterSet):
        for name, queryset in PAGE_FILTER_QUERYSETS.items():
            page_filter = filterset.base_filters.get(name)
            if page_filter is not None:
                page_filter.queryset = queryset


class OrgScopedPageSearchView(SearchView):
    """Page search, narrowed to this organisation's tree.

    Wagtail's page search runs no hook, and its results are a search-backend
    object that cannot be filtered after the fact — so the narrowing has to
    happen while the view still holds a queryset. ``annotate_queryset`` is the
    last place it does, for both the queryset being searched and ``all_pages``,
    which feeds the content-type facets beside the results.

    Wired ahead of Wagtail's own route in ``config/urls.py``: the
    ``register_admin_urls`` hook appends, so it cannot replace a URL, and this
    view has to *be* ``/admin/pages/search/`` rather than sit next to it.
    """

    def annotate_queryset(self, pages):
        self.all_pages = scope_pages(self.request, self.all_pages)
        return super().annotate_queryset(scope_pages(self.request, pages))


class OrgPageTreeMiddleware:
    """Refuses an admin URL that names a page outside this organisation's tree.

    Must run after :class:`OrganisationMiddleware`, which is what put
    ``active_org`` on the request.

    A middleware rather than a view mixin because the views are Wagtail's: they
    call ``get_object_or_404(Page, id=page_id)`` themselves, with no base
    queryset to narrow and no hook to intercept. Resolving the URL here and
    checking the ids it carries closes all of them at once — including the ones a
    future Wagtail release adds, as long as it keeps naming pages the same way.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if getattr(request, "active_org", None) is not None and request.path.startswith(
            settings.GEORIVA_ADMIN_PATH_PREFIX
        ):
            self._guard(request)
        return self.get_response(request)

    @staticmethod
    def _guard(request):
        try:
            match = resolve(request.path_info)
        except Resolver404:
            return
        ids = {value for name, value in match.kwargs.items() if name in PAGE_ID_URL_KWARGS and value is not None}
        ids |= _bulk_action_page_ids(request, match)
        if not ids:
            return

        root = org_root_page(request)
        root_node_allowed = match.url_name in ROOT_NODE_URL_NAMES
        found = Page.objects.filter(pk__in=ids).values_list("pk", "path", "depth")
        for pk, path, depth in found:
            if not page_is_in_org_tree(root, path, depth, root_node_allowed=root_node_allowed):
                raise Http404(f"Page {pk} is not in this organisation's tree.")
        # Ids that matched no page are left to the view, which 404s them itself.


def _bulk_action_page_ids(request, match):
    """The pages a bulk action was asked to act on, named in its query string.

    The one page route that does not carry its subjects in the path — deleting,
    moving or publishing several pages at once posts to
    ``/admin/bulk/wagtailcore/page/<action>/`` with ``?id=`` repeated. Without
    this the whole bulk surface would sit outside the guard, and outside the URL
    sweep that generates its targets from path kwargs.
    """
    if match.url_name != BULK_ACTION_URL_NAME:
        return set()
    if match.kwargs.get("model_name", "").lower() != "page":
        return set()
    return {value for value in request.GET.getlist(BULK_ACTION_ID_PARAM) if value.isdigit()}
