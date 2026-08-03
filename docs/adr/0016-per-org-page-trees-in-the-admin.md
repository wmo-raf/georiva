# Per-org page trees in the Wagtail admin

## Status

accepted

## Context

Each Organisation is provisioned with a Wagtail Site, a root page of its own and
a group holding page permissions over that root (#266). That makes pages
org-owned — but through the Site → root-page link, not through a field, so they
declare `NOT_ORM_SCOPABLE` and the choke point in ADR 0011 cannot scope them.
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

**The org's root page is the whole of its admin's page world**, and four seams
enforce it. They are four because Wagtail resolves pages in four different ways,
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
