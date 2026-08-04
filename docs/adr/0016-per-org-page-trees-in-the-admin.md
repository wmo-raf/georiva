# Per-org page trees in the Wagtail admin

## Status

accepted

## Context

Each Organisation is provisioned with a Wagtail Site, a root page of its own and
a group holding page permissions over that root (#266). That makes pages
org-owned — but through the Site → root-page link, not through a field, so at the
time they declared `NOT_ORM_SCOPABLE` and the choke point in ADR 0011 could not
scope them. (They now declare `PAGE_TREE`, and the dispatcher #296 added does
scope them; the four mechanisms below are still what closes Wagtail's own page
views, which take a pk and expose no queryset to narrow.)
Nothing else did either: the page explorer, the page chooser, page search and
every `/admin/pages/<id>/…` view resolved pages with no reference to the host's
organisation.

For a member of one institution this is invisible, because Wagtail's own page
permissions already confine them to the tree their group covers. It is visible
for exactly the users this tenancy exists for: a regional-centre operator who
belongs to two institutions saw both portals from either host, and a superuser
saw every portal from every host. Host-scoped admin means the subdomain *is* the
organisation (ADR 0011); a page surface that ignores the subdomain contradicts
it.

Wagtail's page views are not the generic model views the scoping mixins wrap.
They take a page id from the URL and call `get_object_or_404(Page, …)`
themselves: no base queryset to narrow, no viewset to mix into.

## Decision

**The org's root page is the whole of its admin's page world**, and five seams
enforce it. They are five because Wagtail resolves pages in five different ways,
and each needs its own:

- **Listings** — the `construct_explorer_page_queryset` hook, which both the
  explorer and the sidebar's page browser (through the admin API) run every
  listing through. The explorer started at the tree root — where a superuser
  lands, and where every organisation's root sits side by side — then lists one
  root: the host's.
- **Choosers** — the `construct_page_chooser_queryset` hook.
- **Search** — a `SearchView` subclass, wired at `/admin/pages/search/` in
  `config/urls.py` *ahead of* Wagtail's own route. Page search runs no hook, and
  its results are a search-backend object that cannot be filtered afterwards, so
  the narrowing happens in `annotate_queryset` — the last point the view holds a
  queryset. The `register_admin_urls` hook appends and therefore cannot replace a
  URL, which is why this one route is claimed in the project's URLconf.
- **Every page-id URL** — `OrgPageTreeMiddleware`, which resolves the request,
  reads the ids it carries (`page_id`, `parent_page_id`, `page_to_move_id`,
  `destination_id`) and 404s any page outside the tree. One check at the request
  boundary covers edit, delete, move, copy, history, add-child and the rest,
  including views a future Wagtail adds, as long as it keeps naming pages the
  same way. Bulk actions are the one page route that names its subjects in the
  query string rather than the path, so the same guard reads `?id=` there.
- **The admin dashboard** — the `construct_homepage_panels` hook, which swaps
  each of Wagtail's four page-resolving panels (recent edits, locked pages and
  the two workflow moderation queues) for a scoped subclass, in place. This one
  was added later (#295), and the reason it was missed is the reason it needs its
  own seam: a panel resolves its pages *inside a context method*, with no
  queryset to hook and no page id in the URL, so it was unreachable by all four
  mechanisms above. The panels filter by who the signed-in user is and never by
  which host they dialled, so a user belonging to two institutions was shown one
  institution's page titles on the other's dashboard.

  Two narrowing strategies, chosen by whether a panel's result is sliced. Recent
  edits takes its handful of rows in the database, so it is narrowed *while the
  query is built* — narrowing afterwards would let another organisation's edits
  consume the limit and silently return fewer rows. The other three are unsliced
  and are narrowed after calling Wagtail's own implementation, which duplicates
  nothing. The object-level question the last two ask — "does this belong to the
  active organisation?" — lives in `organisations/ownership.py`, because their
  results are reached through a generic foreign key that no ORM path crosses.

  The hook receives the assembled list and each entry is replaced by class,
  never rebuilt: other GeoRiva modules append panels of their own and this app
  is registered after them, so its hook runs last. A test asserts the finished
  dashboard holds only known panels, so a Wagtail release that adds or renames
  one fails the suite rather than appearing unscoped.

Membership of the tree is a **materialised-path prefix test**, not a query per
page: treebeard encodes ancestry in the path, so the prefix *is* the ancestor
test. The tree's own root node is admitted on the listing routes only — that is
where a superuser's explorer starts, and what it contains is filtered by the
hooks above — and refused everywhere else, since a page authored directly under
it would belong to no organisation and be served by no portal.

**The filter panel's choices are narrowed too.** Wagtail's page filterset lists
every Site on the instance — the roster of organisations, beside the pages of one
of them — and every user who has owned or edited a page. No hook reaches it, so
`scope_page_filters()` replaces those three filters' querysets at startup with
the request-aware callables django-filter already supports.

## Consequences

- Page tenancy is host-scoped, not user-scoped: the same user sees Kenya's tree
  on Kenya's host and Uganda's on Uganda's. That is the affordance the org-hopper
  (#270) exists to make navigable.
- Wagtail's per-org page-permission group stays the capability layer, untouched.
  This is the second, independent tenancy layer over it — the same shape as
  ADR 0011's two layers.
- The fail-closed sweep now puts a page id into every admin URL that names a
  page, so a page view added later is covered on the day it is registered.
- One residual is accepted: a superuser exploring `/admin/pages/<tree-root>/`
  sees the tree root itself — nobody's page, whose children are filtered to the
  host's organisation.
- The search route is the one place the project's URLconf shadows a Wagtail URL.
  A Wagtail upgrade that moves or renames `/admin/pages/search/` leaves the
  override serving a stale view; the page-tree tests exercise the real route by
  its Wagtail URL name, so that shows up as a failure rather than as a silent
  un-scoping.
