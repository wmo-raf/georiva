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

**Every model in the codebase declares where it stands.**
`ORGANISATION_LOOKUP` is either the ORM path from a model to `Organisation`
(`"organisation"` on `Catalog`, `"collection__catalog__organisation"` on `Item`)
or one of the declarations below, each of which is itself a decision:

- `SHARED_REFERENCE_DATA` — no organisation owns it, every organisation reads it
  (topics, units, administrative boundaries). Scoping passes it through.
- `ORGANISATION_SELF` — it *is* an organisation. Scoping matches on identity.
- `PAGE_TREE` — its owner is decided by where it sits in the page tree, not by a
  field, because that is how a Wagtail page is owned (#261, ADR 0016).
- `via_related(path)` — whoever owns the object at the end of `path` owns this
  row. Resolved by reading *that* model's declaration, so it composes.
- `via_content_object(content_type_field, object_id_field)` — the row's subject
  is polymorphic. Resolved by splitting the rows by content type and scoping
  each part by that part's own declaration.
- `NOT_ORM_SCOPABLE` — no route of any kind reaches an organisation: pipeline
  bookkeeping keyed by a storage path (`FileIngestion` and its jobs), records
  reached only through an already-scoped parent (`DerivationRun`), a credential
  belonging to a person rather than an institution (`ApiKey`). Scoping
  *refuses* these.

One model declares a *tier* on top of its path: `ColorPalette` sets
`ORGANISATION_GLOBAL_TIER` beside `ORGANISATION_LOOKUP = "organisation"` on a
nullable FK, meaning a null organisation is not a broken route but the
instance-wide library (#269). Reads widen to include those ownerless rows —
listings and choosers offer both tiers — while writes stay narrow: an
organisation edits only its own, and the ownerless rows are the instance
admin's. That is one helper apart, `require_writable_org_object`, which the
write half of every scoped view class calls where the read half calls
`require_org_object`.

Only `Catalog` carries an organisation FK; everything beneath it is owned
transitively, as decided in #259. The declarations live on the models, in
`organisations/lookups.py`'s vocabulary — a module deliberately free of imports
so a model can declare without pulling in the enforcement machinery that reads
the organisation models.

**One dispatcher reads the whole vocabulary** (#296). `organisations/ownership.py`
offers two entry points — `scope_rows` for rows the ORM has yet to fetch and
`belongs_to_active_org` for an object already in hand — and every admin surface
calls one of them. They dispatch on the same declarations, so a listing and a
detail view over one model cannot disagree about a row, and a model that gains a
declaration becomes scopable on every surface at once.

Both entry points switch exactly once, on `lookups.kind_of` — a declaration is
resolved to its kind in one place, and adding a kind is one branch in each
consumer rather than a cascade several functions have to keep agreeing about.
The places where the two halves could legitimately part company are settled
deliberately: a null link is nobody's on both sides, and a subject whose model
belongs everywhere is admitted on both sides without either resolving the row —
so a deleted shared subject cannot make a listing show a row that its detail
view 404s.

The last three kinds are why the dispatcher exists. `NOT_ORM_SCOPABLE` said only
how a model *cannot* be scoped, so every model wearing it needed bespoke code
somewhere else — which pages had, in the page-tree module, and nothing else did.
The additions say how a model *can* be:

- `via_related` is deliberately more general than "a path to a page", the shape
  #296 proposed. A page log entry, a page-child orderable and a workflow task
  state all delegate through a foreign key; only the first two land on a page,
  and hard-coding pages would have left the third unscopable. It sits *beside*
  `NOT_ORM_SCOPABLE` rather than subsuming it — five models still reach nothing,
  and saying so is worth more than a declaration that pretends otherwise.
- Generic subjects are scoped **as a queryset**, with one materialised step: a
  generic key stores its subject's id in a character column, so the surviving ids
  are fetched per content type and sent back as literals. The cost is bounded by
  how much of a *subject* type one organisation owns rather than by the table
  being scoped, and the surfaces that use it (moderation queues, audit trails)
  are small. If that stops holding the fix is a stored denormalised owner, not a
  cleverer filter.
- A content type whose model declares nothing is **refused**, loudly, rather than
  passed through — the same rule as everywhere else, and safe for the same
  reason: no model in this codebase reaches production undeclared, and a model
  from outside it reads as shared. Shared reference data appearing as a
  workflow's subject is coherent and is admitted whole, matching what the
  object-level half already did.

**Models we did not write cannot declare**, and four of Wagtail's own are
exactly what the admin's reports and dashboard panels list. `EXTERNAL_DECLARATIONS`
in the dispatcher speaks for them — the page audit log by `via_related`, the
model audit log and workflow state by `via_content_object`, task state by
`via_related` onto the state — in one table, checked by the same sweep, rather
than each surface narrowing them by hand.

**Deny by default, at the seam that matters.** An undeclared model is refused
where scoping is applied, not quietly passed through: a silent pass-through
would be exactly the leak the declaration exists to prevent, and it would be
invisible, because an unscoped listing looks identical to a scoped one until a
second organisation exists. A test enumerates every model in the codebase and
fails on any that has declared nothing, which is what makes refusing safe — no
model reaches production undeclared. A second test goes further and fails on
any model whose declaration the dispatcher cannot *act on*, with the five that
genuinely reach nothing pinned by name — declaring something and being scopable
are different properties, and it is the second one a surface depends on. A
nullable link anywhere along a declared path means the row belongs to *nobody* —
never to everybody, unless the model says otherwise in as many words by declaring
a global tier (below), which is a decision somebody wrote down rather than an
inference from a `None`.

**Superusers skip the membership gate, not the host.** The instance admin may
enter any organisation's admin without a membership row (#257). They do not get
a cross-tenant view: on Kenya's host they read and write Kenya's rows, so
nothing they create there can be filed under another institution's prefix.
Reaching another organisation's data means visiting its host.

**Membership gates the admin plane as a whole**, in the middleware, re-read from
the database every request. A signed-in non-member reaches no view that reads an
organisation-scoped row, so a revoked membership fails closed on the next request
rather than at logout. The handful of admin URLs they *do* still reach are named
by `organisations.middleware.admin_open_paths` and read no such row: the sign-in
and sign-out pair, the sprite, translation catalog and password reset Wagtail
itself leaves unauthenticated, and the org-hopper, which answers a non-member
with nothing. Without them the refusal page could not be escaped, and — because
a refusal is a rendered HTML page — a guarded subresource would hand the sign-in
page a document where it expected an asset. This
is a coarse layer above row scoping rather than a replacement for it: scoping
still decides every row, and the gate only saves a stranger from reaching an
empty listing. The public plane stays open to anonymous readers and is scoped
separately (#278).

**Live event streams carry their organisation.** The SSE feeds fan out from one
Redis channel to every organisation's admin, so each published event names the
organisation it belongs to and a listener forwards only its own. Where that name
comes from follows the same rule as everything else: a catalog chain where one
exists, the storage key's leading segment where none does. An event that cannot
be attributed reaches nobody.

**Two independent layers.** Wagtail's `ModelPermissionPolicy` and Django groups
stay exactly as they were — the *capability* layer, answering what a user may
do. Tenancy is a second layer answering whose rows they may do it to. Joining an
organisation grants the standard data-manager capabilities so the two never have
to be kept in sync by hand.

**The two roles differ on org-management surfaces only.** Members do every bit
of the data work their admins do. What an admin has in addition is the
organisation's own account: its settings, and its roster — adding a member
(account and membership together, since there are no invitations to accept,
#257), changing a role, and removing a membership without deleting the account,
because a user may belong to several institutions. Each of those views calls
`require_org_admin` itself; the menu that leads to them reads the same role, so
visibility and enforcement cannot drift.

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
  fixture with colliding catalog slugs drives every registered admin URL — nested
  multi-id routes included — with the other organisation's ids, asserts that no
  pk-less admin listing names the other organisation, and a static check forbids
  bare `get_object_or_404` in org-owned apps. New views are covered on the day
  they are registered. The sweep is what found the collection-drawer JSON
  endpoints, Wagtail's copy view and the snippet chooser's `chosen/<pk>/`.
- A `DataFeed` with no catalog now belongs to no organisation and is therefore
  reachable from no admin. The column stays nullable for rows that predate the
  link, but the admin form requires one.
- Reads on the public plane (STAC, EDR, analysis, dataset pages) are **not**
  covered here; they were #271's subject and reach the same helpers — see
  ADR 0012. The tile-config endpoint is the one lookup that cannot resolve its
  organisation from the Host at all — Titiler calls it over an internal hostname
  belonging to no organisation — and takes it from an explicit path segment
  instead; see ADR 0013.
