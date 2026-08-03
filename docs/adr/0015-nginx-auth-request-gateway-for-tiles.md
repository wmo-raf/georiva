# The tile servers are gated at the proxy, not taught who is asking

## Status

accepted

## Context

ADR 0014 gave a `Collection` a `private` tier and hid it on all four Django
planes — STAC, EDR, analysis and the dataset portal. It also recorded, in as
many words, the hole it could not close: *"Operators should not treat `private`
as a security boundary for raster tiles before #274."*

The hole is the machine plane. Titiler and Martin read object storage and the
database directly and resolve no tenant at all — that is ADR 0013's whole
arrangement, and the reason it works is that neither service holds any tenancy
logic to drift. Until now that cost nothing, because everything they served was
public. A private tier makes it cost the tier: a collection Django hides from
every listing and 404s by name still had its rasters readable by anyone who
could spell the URL, and its zonal statistics too — `georiva_boundary_stats`
joins all the way back to the collection and filters on nothing about its
visibility, so an ungated Martin is a second, coarser reading of the same
numbers.

Two ways not to fix it were available and both undo something.

*Teach Titiler and Martin.* They would need the membership rule, the session
store and the API-key digest — the three services would then hold three copies
of one rule, which is precisely the drift ADR 0013 exists to prevent, in the one
place where a copy going stale means publishing a nation's unreleased data.

*Proxy tiles through Django.* Every raster byte through a WSGI worker, to
answer a question that is one indexed query.

## Decision

**Nginx asks Django before it proxies, and neither tile server learns
anything.** An `auth_request` subrequest on the `/titiler/` and `/martin/`
locations carries the original request line and the caller's own credentials to
an internal Django endpoint; Django answers 204 or 403 and nginx proxies or does
not. Titiler and Martin are untouched — same routes, same code, still no tenancy
logic, still ADR 0013's arrangement.

**The address is read by the module that writes it.** `machine_plane.scope_of`
is the exact inverse of the URL builders it sits beside, and it lives beside them
for that reason: a reader that drifted from the writer would authorise one
collection while the tile server read another — the single worst failure this
gate can have. A URI it does not recognise scopes to nothing and is denied, which
covers Titiler's `/docs` and Martin's source listing as a side effect and is the
right answer for both.

**The decision is the planes' own vocabulary, not a second opinion.**
`Collection.objects.public()` is asked first and settles the overwhelming
majority of tiles in one indexed query without reading a membership row; only a
miss goes on to `visible_to(request)`, the same seam STAC, EDR and the portal
read. `internal` is in neither queryset, so it is denied without a rule of its
own — which is the point of routing through the vocabulary rather than
hand-writing a visibility filter here.

**The Host wins, because on this call there is one.** ADR 0013 put the
organisation in the path exactly where nothing could check it. This subrequest
carries the browser's real Host, so the exception's stated reason does not hold
and the ordinary rule applies: an org segment disagreeing with the Host is
denied. The tile-config callback keeps its relaxation, because Titiler genuinely
dials it from nowhere.

**Denials are 404 to the client and 403 on the wire.** `auth_request`
understands only 2xx, 401 and 403 and fails the outer request with a 500 on
anything else, so the view speaks that protocol and one `error_page` line turns
403 into the 404 every other plane already answers. A *presented-but-broken* key
stays 401, passed through untouched: that answer is the same whether the
collection exists or not, so it gives nothing away, and it is what tells a QGIS
user their credential expired rather than leaving them hunting a dataset that
appears to have vanished (ADR 0014).

**The gate is cached for ~60s, keyed on everything the decision reads.** A
raster plane answers per tile; a membership check per tile is a database round
trip per tile. The key is narrowed to the org/catalog/collection triple so every
z/x/y of one collection shares one decision, and widened by the credential *and
the Host* so no two callers and no two tenants ever share one. The Host is not
decoration: the gate denies an org segment that disagrees with it, and the
session cookie is shared across the subdomains
(`SESSION_COOKIE_DOMAIN=.{base}`), so nothing else in the key tells two of an
instance's hosts apart. An address the grammar does not recognise falls back to
the whole URI and shares nothing.

The one thing the key deliberately does *not* narrow is Martin's query, which
would cache better as the bare triple and is left whole anyway. A regex lifting
`org=` out of a query takes the first of a repeated parameter, while `scope_of`
refuses repeats outright — so a crafted `?org=a&org=b` would file its denial
under the key the honest URL uses, and 404 that collection for everyone sharing
the credential until the entry expired. Two addresses that decide differently
must not share a key, and that outranks the hit rate.

Everything the cache key reads comes from `$request_uri` and never from
`$arg_*`. That is not style: an `auth_request` subrequest inherits the original
`$request_uri` but **not** its arguments, so `$arg_*` is empty inside the gate —
which would have collapsed every tile of every collection onto one key and
served the first caller's answer to all of them. The same inheritance rule bites
once more, in the other direction: a subrequest shares its parent's *variable
storage*, so the upstream variable in the gate's location is deliberately not
the `$upstream` every neighbouring location uses. Setting that name in here
rewrites it in the location that called us, and the tile is then proxied to
Django by a gate that has just approved it.

## Consequences

- `private` is a security boundary for tiles now, and ADR 0014's warning is
  withdrawn. Both halves of the map are covered: the raster and the choropleth
  drawn beside it.
- Revocation lags on tiles by up to the cache TTL, and only there. A revoked key
  or membership fails on the very next request on every Django plane; a tile
  already being drawn may keep drawing for under a minute. Stated rather than
  designed away, because the alternative is a database round trip per tile.
- Nginx now knows two things it did not: that a tile URL has an org, a catalog
  and a collection in it, and where. Only the *cache key* depends on that — a
  wrong guess there costs a poorer hit rate, never a wrong decision, because the
  decision reads the unparsed URI in Django. The `default` branch is what keeps
  that true.
- `tile-config` is still public-only, and now sits behind a gate that has
  already decided. A private variable renders from the palette cache Django
  warms for every active variable regardless of tier, so the common path works;
  a cache *miss* on a private variable still yields an unrendered tile. Closing
  that means giving Titiler something to prove the gateway let it through, which
  is a trust mechanism this ADR deliberately does not invent.
- Titiler's `/docs` and `/openapi.json`, and Martin's catalog and health
  endpoints, are no longer reachable through the proxy: they scope to nothing
  and a scope of nothing is a denial. That is wider than #274 asked for, and it
  is kept because the alternative is a gate with an allowlist of exceptions —
  the shape in which the next route added to either service is public until
  somebody remembers. Both services remain reachable on the container network.
- Cross-origin tiles carrying `Authorization` are the one client shape this
  makes harder: the preflight `OPTIONS` carries no credential, so it is judged
  as anonymous. `?api_key=` — the transport ADR 0014 added for clients that can
  only take a URL — is unaffected, and same-origin sessions are the normal case
  through this proxy.
- The `georiva-assets` bucket is still served straight from MinIO under an
  anonymous read policy, so encoded PNGs remain a public side door. It is a
  different mechanism with a different fix (bucket policy, not `auth_request`)
  and is out of this decision's scope; only Titiler and Martin were gated.
