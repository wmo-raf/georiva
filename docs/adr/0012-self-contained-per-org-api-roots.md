# Self-contained per-org API roots, addressed by host

## Status

accepted

## Context

ADR 0011 closed row-level tenancy on the admin: every listing, chooser and object
view reaches tenant rows through `organisations/access.py`, and a sweep over the
registered admin URLs holds new views to it. It explicitly left the public plane
alone.

That plane had the same class of defect and a worse audience. STAC, EDR, the
timeseries analysis endpoints and the dataset pages all resolved catalogs,
collections and variables by bare slug — and since #267 a slug is unique only
*within* an organisation. Two failure modes followed, depending on whether the
slug happened to collide:

- **Collision.** `get_object_or_404(Catalog, slug="forecast")` with two
  organisations running a `forecast` catalog raises `MultipleObjectsReturned`
  (a 500) or, where the code used `.filter().first()`, quietly wins with
  whichever row the database returned first. That second outcome is the
  dangerous one: one national met service's data served under another's
  identity, with a 200 and no trace.
- **No collision.** `Catalog.objects.filter(is_active=True)` in the STAC root,
  the collection listing and the datasets index enumerated *every* organisation's
  holdings to any anonymous caller — names, structure, and the fact that those
  institutions are on this instance at all.

The callers here are anonymous and address rows by slug, so neither the admin
sweep nor the membership gate touches any of it.

The shape of the fix was the real decision. Three options:

- **An org path segment** — `/api/{org}/stac/`. Explicit and self-describing,
  but it breaks every existing client URL, contradicts the host-first tenancy the
  rest of the system already commits to (ADR 0011, `OrganisationMiddleware`), and
  gives every request two sources of truth for the organisation that can
  disagree.
- **Org-qualified ids** — `kenya:forecast` as the STAC collection id. Keeps one
  root serving everyone, at the cost of ids that are ugly, that change meaning if
  an org is renamed, and that leak the tenant list through search.
- **Self-contained roots on the host.** Each hostname is one organisation's whole
  service. Ids stay bare.

## Decision

**A host is a service, not a view onto a shared one.** `<org-host>/api/stac/` is
that organisation's entire STAC catalog: its conformance declaration, its search,
its queryables, and every self/root/child link resolving back onto the same host.
EDR, `/api/analysis/` and `/api/datasets/` are the same. No root names, links to,
or can be made to return another organisation's rows.

**Ids stay bare.** `Catalog.slug` and `Collection.slug` go out unqualified and
may collide across organisations — `forecast` is Kenya's on Kenya's host and
Uganda's on Uganda's. The two roots never meet, so the collision is harmless, and
a client that only ever talks to one host cannot tell this instance from a
single-tenant one. The one id that *does* change is the root document's own: it
becomes the organisation slug (title and description follow), because a root *is*
the organisation and calling every institution's root `georiva` would be the one
place the API still pretended there was a single shared catalog.

**The organisation comes from the same choke point.** Public views call
`scoped_queryset`, `get_org_object_or_404` and `require_active_org` — the
identical helpers the admin uses, reading the `active_org` the middleware
resolved from the Host. There is no second resolution path to keep in step, and
the deny-by-default property (an undeclared model raises rather than returning
everything) covers the public plane for free.

**"Public" is defined once.** `Collection.objects.public()` holds the three
conditions that travel together everywhere — collection active, catalog active,
visibility public. Getting one wrong publishes a derivation intermediate or a
retired dataset, and it was previously written out in seven places that could
drift. It says nothing about tenancy: that stays the caller's, applied by
wrapping it in `scoped_queryset` so the organisation filter is visible at every
call site (ADR 0011).

**Tenant rows are reached through a named per-module seam.** `stac/views.py` has
`_org_catalogs` / `_org_variables` / `_org_items`, `edr/views.py` has
`_org_collections`, `DatasetsIndexPage` has `_org_catalog` / `_org_collection`.
A view that wants a Catalog goes through one of those or it is wrong — which is
what makes "no naked slug lookups" something a reader can check by eye.

**`/api/jobs/` is deliberately exempt.** task_ferry is mounted on every host and
is org-agnostic: jobs are guarded by unguessable ids, not by tenancy, and coupling
them to an organisation would buy nothing (PRD #265, story 26).

## Consequences

- Existing single-tenant clients keep the same URLs and the same catalog,
  collection and item ids; the set of rows behind them narrows, and on a
  one-organisation instance it narrows to everything. The one visible break is
  the root document: `id` was the constant `georiva` and is now the
  organisation's slug (`central` on a bootstrapped single-tenant install), with
  the title and description following. A client that pins the root id has to be
  updated; one that follows links does not notice.
- There is no cross-org public view, and adding one would be a new explicit
  endpoint rather than something that falls out of an unscoped queryset. This is
  a decision, not an oversight: a combined discovery surface across institutions
  is a product question nobody has asked for.
- The guarantee is tested by sweep, mirroring ADR 0011: two organisations with a
  colliding catalog slug plus a slug only one of them owns, every `/api/` route
  taking slugs dialled with the foreign one asserting 404, and every parameterless
  `/api/` route asserting the other organisation's name appears nowhere in the
  response. A counter-test dials the same URLs on their *own* host and asserts
  200, so a scoping bug that 404s everything cannot pass the sweep.
- The static rule from ADR 0011 — no bare `get_object_or_404` in org-owned apps —
  now covers `stac`, `edr` and `pages` too.
- A STAC search naming a collection id that resolves to nothing now returns no
  items rather than every item in the root. The old fall-through answered
  "collection you may not see" with "here is everything", which reads as success.
- The portal template tags (`get_latest_catalogs`, `get_landing_stats`, …) take
  the template context so they can scope. A tag rendered without a request now
  raises instead of listing the instance.
- Titiler's `tile-config` endpoint is **not** covered by the host-first rule. It
  is called server-to-server over an internal hostname that belongs to no
  organisation, so the Host cannot name one. It takes an explicit org path
  segment instead — the one place on the instance where a path names the tenant,
  and for the one reason that justifies it: there is no Host to disagree with.
  See ADR 0013.
