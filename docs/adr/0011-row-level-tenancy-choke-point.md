# Row-level tenancy through one choke point

## Status

accepted

## Context

Multi-organisation tenancy (PRD #265) puts several institutions on one instance,
each reachable at its own subdomain. `OrganisationMiddleware` already resolves a
request's Host to its `Organisation` and fails closed on an unknown one, and
#267 made storage paths org-first so ingestion cannot misfile data. What none of
that constrains is *reads and writes in the admin*: every Wagtail listing,
edit/delete view, chooser and JSON endpoint resolved rows by primary key with no
reference to the requesting organisation at all.

On a single-tenant instance this is invisible. With two tenants it is not a leak
in the abstract — catalog slugs are unique per organisation since #267, so two
institutions may both run a catalog called `forecast`, and an unscoped lookup
does not merely expose the other's rows but can answer *with* them.

The obvious implementations were each rejected:

- **Per-view filters.** Correct wherever somebody remembers; the failure mode is
  a view added later, and there is no way to audit "every view".
- **A permission-manager chain** (Baserow's approach, our reference
  implementation). Powerful, but it is a framework: every query routed through a
  registry of manager objects, for a rule that is one `filter()`.
- **`django-scopes`.** Implicit scoping at the manager level. The magic is the
  problem: a queryset that silently returns nothing outside an active scope is
  hard to debug and easy to bypass, and Celery tasks would need scope
  ceremony they do not otherwise want.

## Decision

**One module holds the rule, and every admin surface reaches tenant rows through
it.** `organisations/access.py` provides `scoped_queryset`,
`get_org_object_or_404`, `require_org_object`, `require_org_member` and
`require_org_admin`. Row scoping is explicit at every call site — nothing is
implicit at the manager level — but there is exactly one implementation to audit.

**A model declares its own route to its organisation.** `ORGANISATION_LOOKUP` is
the ORM path from a model to `Organisation` (`"organisation"` on `Catalog`,
`"collection__catalog__organisation"` on `Item`). Only `Catalog` carries an
organisation FK; everything beneath it is owned transitively, as decided in #259.

**Deny by default.** A model that declares no path cannot be scoped: the helpers
raise `ImproperlyConfigured` rather than returning every row. Forgetting a new
model is a loud error, not a silent leak. A nullable link anywhere along a
declared path means the row belongs to *nobody* — never to everybody.

**Superusers skip the membership gate, not the host.** The instance admin may
enter any organisation's admin without a membership row (#257). They do not get
a cross-tenant view: on Kenya's host they read and write Kenya's rows, so
nothing they create there can be filed under another institution's prefix.
Reaching another organisation's data means visiting its host.

**Membership gates the admin plane as a whole**, in the middleware, re-read from
the database every request. A signed-in non-member never reaches a view, so a
revoked membership fails closed on the next request rather than at logout. The
public plane stays open to anonymous readers and is scoped separately (#278).

**Two independent layers.** Wagtail's `ModelPermissionPolicy` and Django groups
stay exactly as they were — the *capability* layer, answering what a user may
do. Tenancy is a second layer answering whose rows they may do it to. Joining an
organisation grants the standard data-manager capabilities so the two never have
to be kept in sync by hand.

**No checks inside Celery tasks.** A task reaches its organisation by
construction, through the FK chain from the catalog it was handed. Adding checks
there would mean inventing a request-like context for code that has none.

## Consequences

- The scoping seams are Wagtail's, not ours: `get_base_queryset`,
  `get_base_object_queryset`, chooser `get_object_list` **and** `chosen/<pk>/`,
  and form field querysets. `organisations/scoping.py` wraps every `*_view_class`
  a viewset exposes rather than an enumerated few, so a view added by a Wagtail
  upgrade or a plugin's generated viewset is scoped without being remembered.
- Scoping form field querysets closes most of the wrong-org *create* residual
  #258 accepted: a hand-crafted POST naming another organisation's object fails
  validation rather than saving. What remains is a create whose relations are all
  in-org — harmless by construction.
- The guarantee is tested by sweep rather than by list: a two-organisation
  fixture with colliding catalog slugs drives every registered admin URL that
  takes a pk with the other organisation's ids, and a static check forbids bare
  `get_object_or_404` in org-owned apps. New views are covered on the day they
  are registered.
- A `DataFeed` with no catalog now belongs to no organisation and is therefore
  reachable from no admin. The column stays nullable for rows that predate the
  link, but the admin form requires one.
- Reads on the public plane (STAC, EDR, tile-config, dataset pages) are **not**
  covered here; they resolve catalogs by bare slug and are #278's subject.
