# A private visibility tier, and keys that carry identity but not tenancy

## Status

accepted

## Context

ADR 0011 closed the admin, ADR 0012 made each host one organisation's whole
public service, and ADR 0013 gave the machine plane an address that names its
tenant. Between them they answer *whose* rows a request may reach. None of them
answers a different question that had never come up: whether a row is served at
all to somebody who is not signed in.

Until now it did not have to. `Collection.visibility` had two values —
`public`, served everywhere, and `internal`, a derivation intermediate read by
the engine and served nowhere — and every caller on every serving plane was
anonymous by construction. An NMHS with data it may publish to its own staff and
not to the public had exactly two options, both wrong: publish it to the world,
or mark it `internal` and lose it from its own portal too.

Adding the tier is the easy half. The hard half is that a tier only meaningful
to *members* requires the serving planes to know who is asking, and until now
they did not need to. Two kinds of caller want in: a browser on the
organisation's portal, which already has a session cookie, and a script — QGIS,
`pystac-client`, a notebook, a cron job — which has no browser and no login
form.

## Decision

**Three tiers, and the third one is not a smaller version of the first.**
`public` is served to anyone, `private` to authenticated members of the owning
organisation, `internal` to nothing that serves. `internal` deliberately did
*not* become "private with an even smaller audience": a derivation intermediate
is not a dataset with a restricted readership, and collapsing the two would make
every future widening of an audience a chance to publish one by accident.

**A caller who may not see a private collection is not told it exists.** Not
403, not 401: it is absent from every listing and every search, and a fetch by
name is the same 404 a misspelling gets. This follows `require_org_object`'s
existing rule (ADR 0011) for the same reason — which datasets an institution
holds back is not a stranger's business, and a 403 answers precisely the
question the tier exists to keep quiet. It also decides the *shape* of the
implementation: the tier is a **filter on a queryset**, not a check before a
response, because a filter cannot be forgotten on the listing while being
remembered on the fetch.

**One vocabulary, one seam.** `Collection.objects.public()` held the three
conditions that travel together (ADR 0012). It now sits on
`servable()` — active, catalog active, not `internal` — with
`visible_to(request)` above it as the seam every serving surface reaches for.
The `_org_*` helpers in `stac/views.py`, `edr/views.py` and the dataset pages
change one word each and are done; the handful of places that need the *tiers*
rather than a queryset (a `Count(filter=Q(...))` over a catalog's collections)
call `visible_visibilities(request)`, which is the same rule read differently.
The catalog serializer's extent and summaries go through it too — bounds and
variable names of a collection you cannot fetch are still that collection.

**Membership is resolved live, from `request.user`, at the moment it is used.**
`OrganisationMiddleware` already computes a role per request, and on the admin
plane that is the right source. It is the wrong one here: an API-key request is
still anonymous when the middleware runs and only acquires its user when DRF
authenticates it, inside the view, so reading the middleware's answer would deny
every key holder. Rather than let two resolutions drift, the rule itself moved
into `access.resolve_org_role(organisation, user)`; the middleware calls it, and
so does `access.may_see_private(request)`. Two read points with stated reasons,
one implementation to audit.

**A key authenticates a person and carries no organisation.** An `ApiKey`
belongs to a user, is named (people hold several — a laptop, a server, a
notebook — and revoking the leaked one must not log out the other three), and
grants exactly what its holder's memberships already grant, on the host that
serves them. A key that named its own scope would be a second source of truth
for tenancy — the thing ADR 0012 spent a whole decision avoiding — and would go
stale the moment a membership changed. So sessions and keys converge: both
establish identity, and `access.py` decides everything after that.

Its consequences follow from that one property. Keys live in a new `accounts`
app rather than in `organisations`, because what lives there belongs to an
institution and what lives here belongs to a person. `ApiKey` declares
`NOT_ORM_SCOPABLE` — not as a shrug, but because scoping a personal credential
by organisation is a category error and this is what makes attempting it raise.
The management panel scopes every lookup by `request.user` and by nothing else,
so an org admin has no claim over their members' credentials and neither does
the instance admin.

**The secret exists once; the database keeps a digest.** `grv_`-prefixed so it
is recognisable in a log line or to a secret scanner, 256 bits from `secrets`,
SHA-256 at rest. A digest rather than a password hasher is deliberate and is the
standard treatment for a credential of this kind: the secret was not chosen by a
person, so there is no dictionary to run and nothing a slow KDF buys — while a
slow KDF on a credential checked once per tile request would be its own denial
of service. Revocation writes a timestamp instead of deleting the row, because
the question after a leak is what the key was doing, and a deleted row answers
it with silence.

**Two transports, because one of the clients cannot send a header.**
`Authorization: Bearer grv_…` is what a script should use. `?api_key=grv_…`
exists for QGIS and web maps, which take a tile URL and nothing else. The query
parameter is the weaker of the two — URLs reach proxy logs and browser history —
and it is accepted anyway, because the alternative is that the tools people
actually use cannot reach private data at all.

**A presented-but-broken key is 401, not 404.** The 404 elsewhere hides whether
a collection exists; this answer does not touch that question — it is the same
whether the collection is there or not — and telling a scripting user their key
expired saves them debugging a dataset that appears to have vanished.

**Analysis enforces at submission.** `/api/jobs/` is guarded by its job id
rather than by tenancy (ADR 0012), so a job created over a collection the caller
may not see would be a side door with no second gate behind it. The check
therefore lives in the variable-address field, which is where the caller is
still identified — and which previously applied no visibility filter at all, so
`internal` collections were reachable through timeseries. They are not now.

## Consequences

- `tile-config` is the one Django plane that stays public-only. Every other one
  widened by asking who is calling; that one cannot, because on the call it
  exists for there is nobody to ask — Titiler forwards no credential and holds
  no session. Answering for a private collection would mean answering for
  anyone, so it answers for no one. A private variable still renders from the
  palette cache Django warms directly, and authenticating the machine plane
  properly is #274's job.
- Titiler and Martin remain open to anyone who can reach them until that
  gateway lands. ADR 0013 already recorded this, and the private tier does not
  change it: a private collection is hidden on all four Django planes and its
  tiles are not. Operators should not treat `private` as a security boundary for
  raster tiles before #274.

  **Superseded by ADR 0015.** The gateway landed; this warning no longer holds.
  Titiler and Martin are gated at the proxy, and `private` is a security
  boundary for tiles as well as for listings.
- `REST_FRAMEWORK` exists for the first time, with two authentication classes
  and no permission classes. DRF's `BasicAuthentication` default is dropped:
  passwords over the API were never a supported way in, and leaving it on would
  have been a third identity path nobody asked for.
- The analysis plane answers 400 rather than 404 for a private variable, because
  that is what it already answers for a variable that does not exist — the
  addresses are indistinguishable, which is the property that matters, and
  changing the code would have changed it for every malformed address too.
- Widening `Collection.Visibility` widened `derived_products.VISIBILITIES` with
  it: a recipe can now declare a private output. That list is a second,
  independent copy of the same vocabulary and remains one to keep the
  plugin-facing dataclasses free of model imports.
