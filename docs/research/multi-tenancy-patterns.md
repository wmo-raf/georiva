# Row-Level Multi-Tenancy in Django/Wagtail: Prior Art & Pitfalls for GeoRiva

**Researched:** 2026-07-21
**Author:** research agent (primary-source pass)
**Scope:** GeoRiva's planned org model — `Organisation` with M2M membership (Admin/Member),
a session-scoped active-org switcher, and an org-FK on `Catalog` with transitive scoping to
`Collection`/`Item`/`Variable`/`Asset`. Schema-per-tenant is explicitly **out of scope**.

**Versions checked:** Django 5.x, Wagtail 7.x (verified against `main` branch source, which is
7.x-line at time of research), django-organizations 2.x (docs 2.6–2.8), django-tenants (docs
`dev`/master), wagtail-tenants 0.2.0 (last release 2023-09-05), Baserow (master), DRF (behaviour
of default settings).

---

## Question

What prior art exists for row-level multi-tenancy in Django/Wagtail, and which pitfalls apply to
GeoRiva's plan (session-scoped active org, Wagtail `ModelViewSet.get_queryset` filtering, choosers,
snippets, reports/dashboards)? Specifically: (1) adopt vs hand-roll — django-organizations,
django-tenants, wagtail-tenants, Baserow-style hand-rolling; (2) how Wagtail permission policies,
choosers, snippets, and search interact with row-level filtering and where the leak points are;
(3) known failure modes — leaks via choosers, reports, admin search, hook-registered API endpoints,
and session-scoped tenant context pitfalls.

## Verdict / Recommendation

**Hand-roll row-level scoping. Adopt django-organizations only for the membership/invitation
plumbing, not for isolation.** There is no drop-in package that does row-level (shared-schema)
tenant isolation for Wagtail's admin. The schema-per-tenant options (django-tenants, and
wagtail-tenants which is a thin layer over it) are a genuine non-fit for GeoRiva because GeoRiva's
whole point is a **shared, cross-org, partly-public STAC/EDR catalog** over shared TimescaleDB and a
single MinIO layout — schema isolation fights all three. django-organizations gives you Organization
/ OrganizationUser / OrganizationOwner models plus invitation backends but explicitly does **no**
automatic queryset scoping — you filter yourself. The Baserow model (explicit per-request
`CoreHandler.check_permission` calls at the handler layer, not a global queryset manager) is the
realistic template.

The dominant risk is **not** the index lists you will remember to filter — it is the surfaces that
resolve objects by PK independently of any index queryset: **Wagtail choosers' `chosen/<pk>/`
endpoint, edit/delete views at `/edit/<pk>/`, admin search, reference index, and — most severe in
GeoRiva today — the STAC/EDR/analysis DRF APIs, which currently run with DRF's default `AllowAny`
and no queryset scoping at all** (`config/settings` defines no `REST_FRAMEWORK` block; the STAC/EDR
views subclass plain `APIView`/`APIView`-derived bases with no `permission_classes`). Wagtail's
`ModelPermissionPolicy` is **model/group-level, never row-level**, so it will not save you.

---

## 1. Packages & prior art

### 1.1 django-organizations — adopt for membership, NOT for isolation

Provides three models: an Organization (the group), an OrganizationUser (a custom **through** model
on the Organization↔User M2M storing per-org info), and an OrganizationOwner; plus invitation and
registration backends. Users can belong to and own multiple orgs. The default invitation backend
takes an email and returns/creates the matching user, leaving the view to attach them to the org.
([django-organizations usage docs](https://django-organizations.readthedocs.io/en/latest/usage.html);
[custom usage](https://django-organizations.readthedocs.io/en/latest/custom_usage.html);
[backends](https://django-organizations.readthedocs.io/en/latest/reference/backends.html);
[repo](https://github.com/bennylope/django-organizations))

What it does **not** do: there is no automatic, framework-level queryset scoping. Its own cookbook
shows scoping as *something you write yourself* — a custom `QuerySet` with a `for_user`/`for_org`
method that filters "documents belonging to any organization the user is a member of." Nothing
intercepts arbitrary model queries or Wagtail admin views.
([cookbook](https://django-organizations.readthedocs.io/en/latest/cookbook.html))

**Fit for GeoRiva:** good match for the `Organisation` + M2M-membership + Admin/Member + invitations
requirement. It maps cleanly onto the planned model. Treat it as the membership substrate and layer
your own scoping on top. (You can also skip it and hand-roll the two models — it is small — but its
invitation backends are worth having.)

### 1.2 django-tenants (schema-per-tenant) — genuine non-fit; explain why

django-tenants gives each tenant its own PostgreSQL **schema**; on each request it matches a tenant
by hostname and rewrites PostgreSQL's `search_path` to `tenant_schema, public`, so every query runs
inside the tenant schema while `public` holds SHARED_APPS. `migrate_schemas` migrates SHARED_APPS to
`public` and TENANT_APPS per-schema. The `search_path` is reset in the DB wrapper's `_cursor()` on
every operation (mitigated by `TENANT_LIMIT_SET_CALLS`).
([repo](https://github.com/django-tenants/django-tenants);
[docs](https://django-tenants.readthedocs.io/en/latest/use.html))

Why it conflicts with GeoRiva specifically:

- **Shared, cross-org and partly-public catalog.** GeoRiva's STAC/EDR API is meant to serve a
  unified catalog including public data across orgs. Schema-per-tenant hides each org's rows inside
  a separate schema keyed by hostname, which is the opposite of "one shared catalog with an org FK
  and a public flag." You would need cross-schema unions to render the public catalog — defeating
  the model.
- **TimescaleDB hypertables.** GeoRiva runs TimescaleDB (per `CLAUDE.md`). Hypertables, continuous
  aggregates and chunk management are per-schema objects; multiplying them per tenant multiplies
  operational surface and breaks the "one time-series store" assumption.
- **Single MinIO layout.** Storage paths are already time-partitioned
  `{catalog}/{collection}/{variable}/{y}/{m}/{d}/` in one bucket set. Schema isolation buys nothing
  at the object-store layer, which stays shared regardless — so DB-schema isolation and storage
  isolation would diverge.
- **Hostname routing.** django-tenants keys tenants by hostname; GeoRiva's plan is a
  **session-scoped active-org switcher** within one host, which is architecturally incompatible with
  schema-by-hostname.

### 1.3 wagtail-tenants — thin wrapper over django-tenants; effectively unmaintained

`wagtail_tenants` is a Wagtail app that "provides multitenancy" by using **django-tenants to slice
the database at the Postgres-schema level"; the author explicitly chose the schema path. You run a
public Wagtail site that hosts per-tenant Wagtail sites, configure SHARED_APPS/TENANT_APPS, add
`WagtailTenantMainMiddleware`, and need a superuser per tenant.
([repo](https://github.com/borisbrue/wagtail-tenants);
[docs](https://github.com/borisbrue/wagtail-tenants/blob/main/docs/source/index.md))

Maturity: latest release **0.2.0 on 2023-09-05**; prior releases trail back to 2022. `requires-python
>=3.9,<4.0`, classifiers list Python 3.9–3.11, and it pins `django-tenants>=3.3.4,<4.0`. It does
**not** declare a Wagtail version bound. ([PyPI JSON](https://pypi.org/pypi/wagtail-tenants/json))

**Fit:** No. It inherits every django-tenants conflict above, it is ~3 years stale with no evidence
of Wagtail 7 support, and it is schema-based. Not a candidate.

> Note: Wagtail's own docs discuss "multi-site, multi-instance, multi-tenancy" but Wagtail multi-site
> is about serving multiple `Site` roots from one instance, **not** row-level authorization, and the
> project points at django-tenants for hard isolation.
> ([Wagtail docs](https://github.com/wagtail/wagtail/blob/main/docs/advanced_topics/multi_site_multi_instance_multi_tenancy.md);
> [discussion #12653](https://github.com/wagtail/wagtail/discussions/12653))

### 1.4 Baserow-style hand-rolling — the realistic template

Baserow (a mature Django multi-tenant SaaS) does **row-level** tenancy with a shared schema and a
`Workspace` (formerly Group) FK, enforcing access via **explicit per-request permission checks in
its handler layer**, not a magic global manager. Backend code calls
`CoreHandler().check_permission(actor, operation, workspace=..., context=...)`, which runs registered
permission managers in order; each may allow, disallow (raising `PermissionException`), or pass
through, and **default-deny** applies if none allow. The workspace is the context that scopes the
check. ([permissions guide](https://baserow.io/docs/technical/permissions-guide);
[source](https://github.com/baserow/baserow/blob/master/docs/technical/permissions-guide.md))

The lesson for GeoRiva: scoping is enforced at a **choke point you control on every mutating/reading
path** (a service/handler function), and every entry point routes through it. A global queryset
manager helps for the common read path but is not sufficient on its own because object-by-PK paths
bypass list querysets (see §2).

### 1.5 Hand-rolling aids worth knowing

- **django-scopes** — adds `ScopedManager`; models raise `ScopeError` if you query them without
  first entering an explicit `scope(organization=...)` context. Its value is **fail-closed**: an
  unscoped query is an error, not a silent full-table leak. Strong fit for GeoRiva's core models,
  but note it does **not** auto-wire Wagtail admin views — you must open the scope in middleware and
  be careful with admin code paths that expect unscoped access.
  ([repo](https://github.com/raphaelm/django-scopes))
- **django-multitenant** (Citus) — adds a `TenantManager`/`TenantModel` that auto-injects a
  `tenant_id` filter, designed for Citus distribution. Useful pattern reference (implicit tenant
  filter via manager + thread-local current tenant) but Citus-oriented and still leaves admin/API
  paths to you. ([repo](https://github.com/citusdata/django-multitenant))

---

## 2. Wagtail internals & where row-level filtering leaks

The core hazard: **filtering an index queryset does not filter object resolution.** Wagtail's
generic index, chooser, edit, delete, inspect, and usage views resolve a single object by PK
independently of the index list. Guarding the list alone leaves `/edit/<pk>/`, `/delete/<pk>/`, and
`chosen/<pk>/` wide open.

### 2.1 Permission policies are model-level, not row-level

`ModelPermissionPolicy` "enforces permissions at the **model level**, by consulting the standard
`django.contrib.auth` permission model directly." `user_has_permission(user, action)` answers a
model-wide yes/no; there is no per-row check. (`OwnershipPermissionPolicy` adds per-instance logic
but only via an `owner` field — not an org FK.)
([wagtail/permission_policies/base.py](https://github.com/wagtail/wagtail/blob/main/wagtail/permission_policies/base.py))

Consequence: GeoRiva's viewsets are already permission-gated at the **model** level — e.g.
`CatalogViewSet` menu visibility is gated by "groups need a Catalog model permission" (see the
comment in `core/viewsets.py`). That gates *whether the menu shows*, not *which rows*. Two members
of different orgs with the same "view Catalog" group permission will both pass the policy and, absent
your own row filter, both see all catalogs.

### 2.2 Choosers — two distinct leak points

`ChooserViewSet` builds its modal listing through an injectable `get_object_list` method (Wagtail
injects `get_object_list` into the choose/results view classes), which is the override point for
filtering the **list** you see in the modal. But the ViewSet also registers
`path("chosen/<str:pk>/", self.chosen_view, ...)` whose `ChosenView` **resolves the object by the PK
in the URL** — independently of `get_object_list`. So overriding the list is necessary but not
sufficient: a user can POST/GET `chosen/<pk>/` for an out-of-org PK.
([wagtail/admin/viewsets/chooser.py](https://github.com/wagtail/wagtail/blob/main/wagtail/admin/viewsets/chooser.py))

GeoRiva choosers to guard (each must filter both the list **and** the chosen/PK resolution):
- `CatalogChooserViewSet` (`core/viewsets.py`) — directly org-scoped.
- `BoundaryChooserViewSet` (`core/viewsets.py`, over `adminboundarymanager.AdminBoundary`) — decide
  if boundaries are org-scoped or shared reference data.
- `DataFeedChooserViewSet` (`sources/viewsets.py`) — DataFeed hangs off Catalog, so transitively org
  data.

### 2.3 Snippets — override `get_queryset`, but per-view

`SnippetViewSet`/`ModelViewSet` build on Wagtail's generic `IndexView`; the scoping override is
`get_queryset` (or `get_base_queryset`) on the index view class. But the **edit**, **delete**,
**inspect**, and **history** views each have their own `get_object`/`get_queryset` that fetch by PK
and must be overridden too — otherwise `/snippets/<app>/<model>/edit/<pk>/` reaches any row. There is
no single viewset-wide "scope everything" switch; you override per member view (or subclass a mixin
you apply to all of them).

GeoRiva snippet/model viewsets to guard: `ItemViewSet` and `AssetViewSet` (registered via
`register_snippet` in `core/wagtail_hooks.py`); `CollectionViewSet`, `TopicViewSet`,
`ColorPaletteModelViewSet`, `CatalogViewSet` (all `ModelViewSet`); and the **dynamically generated**
per-DataFeed `ModelViewSet`s built in `sources/wagtail_hooks.py::get_data_feed_viewsets()` — these
are created with `type(...)` in a loop, so any scoping mixin must be injected into that factory, not
hand-added per class.

### 2.4 Admin search — a silent bypass

Wagtail's generic `IndexView` applies `search_fields` via the search backend. Two failure shapes:
(a) if search runs through the DatabaseSearchBackend it re-queries the model and can **re-introduce
rows your `get_queryset` filtered out** unless the search queryset is also scoped; (b) if a project
uses an external backend (Elasticsearch), the search index is populated model-wide and returns
cross-org hits regardless of DB filters. GeoRiva models declare `search_fields` (e.g. `Catalog`,
`Topic`, `Item` are `Indexed`), so admin search over Catalog/Item/Asset is a concrete surface to
scope. Verify that your index `get_queryset` override is applied **before** search, or override the
view's search path.

### 2.5 Reports & reference index

- **ReportView / PageReportView**: GeoRiva registers **no** custom `ReportView` (grep for
  `ReportView` in `georiva/src/georiva` returns nothing) and `core/wagtail_hooks.py` hides the
  built-in "reports" menu (`hide_some_menus` drops `["documents","help","snippets","reports"]`). So
  reports are not an active leak today — but note the hide is **cosmetic menu removal**, not URL
  removal: report URLs (e.g. workflow/locked-pages/aging-pages reports) may still resolve directly if
  their `register_admin_urls`/viewsets remain registered. If you later add org dashboards as
  `ReportView`s, they take an unscoped queryset by default — scope it.
- **Reference index / "Usage"**: Wagtail's reference index powers the "Used by" / usage views and
  delete-confirmation "this will affect…" listings. It is populated model-wide and can disclose the
  **existence and titles** of cross-org referencing objects on an in-org object's usage page. This is
  a subtle metadata leak to check once FKs cross org boundaries.

### 2.6 GeoRiva-specific surfaces to guard (table)

| Surface | GeoRiva location | Resolves by PK independent of list? | Guard required |
|---|---|---|---|
| Catalog index/edit/delete | `core/viewsets.py` `CatalogViewSet` (+ Create/Edit/Delete views) | Yes (edit/delete by PK) | Scope `get_queryset` on **all** member views, not just index |
| Catalog chooser | `core/viewsets.py` `CatalogChooserViewSet` | Yes (`chosen/<pk>/`) | Override `get_object_list` **and** chosen/PK resolution |
| Boundary chooser | `core/viewsets.py` `BoundaryChooserViewSet` | Yes | Decide shared vs scoped; if scoped, guard both paths |
| Collection index/edit/delete | `core/viewsets.py` `CollectionViewSet` | Yes | Transitive scope via `catalog__organisation` |
| Item (snippet) | `core/wagtail_hooks.py` `register_snippet(ItemViewSet)` | Yes | Scope index + edit + inspect + history; also `list_filter=["collection"]` must not expose cross-org collections |
| Asset (snippet) | `register_snippet(AssetViewSet)` | Yes | Scope; `list_filter=["format","variable"]` filter options must be org-scoped |
| DataFeed list/detail | `sources/views.py` (FBVs `data_feed_list`, `data_feed_detail`, edit, delete, runs, lineage, wizard) | Yes (many `<int:pk>` routes) | These are **function-based views** with manual `get_object_or_404` — each must add org filter; easy to miss one of ~30 routes |
| DataFeed chooser | `sources/viewsets.py` `DataFeedChooserViewSet` | Yes | Guard both paths |
| Per-DataFeed subtype viewsets | `sources/wagtail_hooks.py` `get_data_feed_viewsets()` (dynamic `type(...)`) | Yes | Inject scoping mixin **in the factory** |
| STAC API | `stac/views.py` `STACAPIView` subclasses; `stac/urls.py` | N/A (public API) | **No `permission_classes`, no `REST_FRAMEWORK` settings → DRF default `AllowAny`.** `STACCatalogListView` filters only `is_active=True`, no org filter. Biggest current gap |
| EDR API | `edr/views.py` `EDRAPIView` subclasses | N/A | Same as STAC: unscoped, public |
| Analysis API | `analysis/urls.py`, `timeseries/`, `zonal_stats/` | N/A | Same; verify per-endpoint |
| Zonal-stats vector tiles | Martin `/martin/boundary_stats/{z}/{x}/{y}` (external server) | N/A | Served by Martin from PostGIS **outside Django** — Django scoping cannot reach it; needs its own filter/tenant column |
| Hook-registered admin URLs | `register_admin_urls` in `core`, `sources`, etc. | Yes | FBVs registered by hooks are ordinary Django views with no policy — each needs a manual membership + org check |

---

## 3. Failure modes & session pitfalls

### 3.1 The PK-resolution leak (restating, because it is the #1 failure mode)
Every "I filtered the list" mitigation is incomplete unless the by-PK views (`edit/<pk>`,
`delete/<pk>`, chooser `chosen/<pk>`, inspect, usage, and every FBV that does
`get_object_or_404(Model, pk=...)`) also apply the org filter. In GeoRiva, `sources/views.py` alone
registers ~30 `<int:pk>`/`<int:feed_pk>` routes; each is a separate place to forget the filter.
Baserow's answer — a single `check_permission(workspace=...)` choke point every path must call —
is the structural fix. ([Baserow permissions guide](https://baserow.io/docs/technical/permissions-guide))

### 3.2 Unscoped DRF APIs (most severe today)
`config/settings/*` defines **no** `REST_FRAMEWORK` block, so DRF defaults apply:
`DEFAULT_PERMISSION_CLASSES = [AllowAny]` and no default authentication that would attach a user for
scoping. The STAC/EDR/analysis views subclass plain `APIView` with no `permission_classes` and build
querysets like `Catalog.objects.filter(is_active=True)` / `Collection.objects.filter(...)` with no
org predicate. Once `Catalog` gets an `organisation` FK, these endpoints will happily serve every
org's data to anyone. Decide the public-vs-private contract explicitly: a `public` flag on Catalog
plus request-user org membership, enforced in each view's queryset (or a shared scoping mixin/base
class for `STACAPIView`/`EDRAPIView`).

### 3.3 Admin search bypass — see §2.4. Test with a cross-org row that matches a search term.

### 3.4 Reference-index / usage metadata leak — see §2.5.

### 3.5 Martin vector tiles bypass Django entirely
`/martin/boundary_stats/{z}/{x}/{y}` is served by Martin straight from PostGIS. No Django middleware,
policy, or queryset override touches it. If zonal stats become org-scoped, isolation must be done in
the SQL function/source Martin uses (tenant column + a tenant param), or the endpoint fronted by
Nginx auth — Django-side scoping is structurally unable to help here.

### 3.6 Session-scoped active-org pitfalls

- **Stale org after membership revocation.** If the active org is stored in the session and only
  validated at *switch* time, a user removed from an org keeps access until they switch. **Validate
  membership on every request** (middleware that, given `session["active_org_id"]`, confirms the
  current user still has a live membership; on failure, clear it and fall back to a default org or
  block). Never trust the session org id alone as proof of authorization — it is a *hint*, and the
  authorization is the membership row.
- **Concurrent tabs share one session.** The active org lives in one server-side session shared by
  all tabs of that browser. Switching org in tab A silently changes tab B's context; a form opened
  under org A can submit under org B, writing/reading the wrong tenant. Mitigations: bind the active
  org into each request explicitly (e.g. a per-request org from URL/header, or an org token embedded
  in forms and re-validated on POST) rather than relying solely on ambient session state; at minimum,
  re-check on POST that the object's org equals the *submitting request's* resolved org.
- **Check-on-request vs check-on-switch.** Prefer check-on-request. The membership lookup is one
  indexed query; caching it per-request is fine, caching it across requests re-introduces staleness.
- **Fail closed.** If `active_org_id` is missing/invalid/not-a-membership, deny or force re-selection
  — do not default to "all orgs" or "first org" silently.

---

## 4. Recommendations for GeoRiva

1. **Adopt django-organizations for membership + invitations only**; do not expect isolation from it.
   Map `Organisation`→Organization, membership→OrganizationUser, Admin/Member→role field/owner.
   ([usage](https://django-organizations.readthedocs.io/en/latest/usage.html))
2. **Do not use django-tenants / wagtail-tenants.** Schema-per-tenant conflicts with GeoRiva's shared
   cross-org public catalog, TimescaleDB hypertables, single MinIO layout, and session-switch (not
   hostname) model; wagtail-tenants is additionally ~3 years stale with no Wagtail-7 bound.
3. **Fail-closed core models.** Put the org FK on `Catalog`; give `Catalog`/`Collection`/`Item`/
   `Variable`/`Asset` a scoping manager. Seriously consider **django-scopes** so an unscoped query
   *errors* instead of leaking. ([django-scopes](https://github.com/raphaelm/django-scopes))
4. **One choke point, Baserow-style.** Add a `require_org_access(user, obj_or_org, action)` /
   `scoped_queryset(user)` helper and route **every** admin FBV, viewset member view, chooser, and
   DRF view through it. Do not scatter ad-hoc `.filter()` calls.
   ([Baserow](https://baserow.io/docs/technical/permissions-guide))
5. **Guard by-PK paths, not just lists.** For every viewset, override `get_queryset` on index **and**
   edit/delete/inspect/history; for every chooser, override `get_object_list` **and** the
   `chosen/<pk>/` object resolution. Inject a scoping mixin into
   `sources/wagtail_hooks.py::get_data_feed_viewsets()` so the dynamically generated viewsets inherit
   it.
6. **Lock down the APIs first — highest current exposure.** Add a `REST_FRAMEWORK` block (auth +
   sane default permissions) and an org-scoping base class for `STACAPIView`/`EDRAPIView`/analysis
   views; introduce an explicit `public` flag so cross-org public data is opt-in, not the default.
7. **Handle non-Django surfaces.** Martin (`/martin/boundary_stats/...`) needs SQL-level tenant
   filtering or an auth proxy; MinIO paths/buckets need an org segment or bucket-policy story since
   object storage stays shared.
8. **Session org = hint, membership = authority.** Add middleware that re-validates
   `session["active_org_id"]` against a live membership **every request**, clears it on
   revocation, and fails closed. Re-check the submitting request's org on POST to defuse
   concurrent-tab cross-writes.
9. **Search & reference index.** Ensure admin search re-applies the org filter (DB backend) or that
   any external search index is org-partitioned; audit usage/reference-index pages for cross-org
   title disclosure once FKs cross org lines.

---

### Sources (primary)

- django-organizations: [usage](https://django-organizations.readthedocs.io/en/latest/usage.html),
  [custom usage](https://django-organizations.readthedocs.io/en/latest/custom_usage.html),
  [backends](https://django-organizations.readthedocs.io/en/latest/reference/backends.html),
  [cookbook](https://django-organizations.readthedocs.io/en/latest/cookbook.html),
  [repo](https://github.com/bennylope/django-organizations)
- django-tenants: [repo](https://github.com/django-tenants/django-tenants),
  [docs/use](https://django-tenants.readthedocs.io/en/latest/use.html)
- wagtail-tenants: [repo](https://github.com/borisbrue/wagtail-tenants),
  [docs](https://github.com/borisbrue/wagtail-tenants/blob/main/docs/source/index.md),
  [PyPI JSON](https://pypi.org/pypi/wagtail-tenants/json)
- Wagtail: [permission_policies/base.py](https://github.com/wagtail/wagtail/blob/main/wagtail/permission_policies/base.py),
  [admin/viewsets/chooser.py](https://github.com/wagtail/wagtail/blob/main/wagtail/admin/viewsets/chooser.py),
  [multi-tenancy doc](https://github.com/wagtail/wagtail/blob/main/docs/advanced_topics/multi_site_multi_instance_multi_tenancy.md),
  [discussion #12653](https://github.com/wagtail/wagtail/discussions/12653)
- Baserow: [permissions guide](https://baserow.io/docs/technical/permissions-guide),
  [source](https://github.com/baserow/baserow/blob/master/docs/technical/permissions-guide.md)
- django-scopes: [repo](https://github.com/raphaelm/django-scopes);
  django-multitenant: [repo](https://github.com/citusdata/django-multitenant)
- GeoRiva source (this repo, verified 2026-07-21): `georiva/src/georiva/core/viewsets.py`,
  `core/wagtail_hooks.py`, `core/models/catalog.py`, `sources/viewsets.py`, `sources/wagtail_hooks.py`,
  `sources/views.py`, `stac/views.py`, `stac/urls.py`, `edr/views.py`, `analysis/`,
  `config/settings/` (no `REST_FRAMEWORK` block present).
