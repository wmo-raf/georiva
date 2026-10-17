# One opt-in proxy cache for `/api/`

## Status

accepted

Sits beside ADR 0021 (on-demand encoded textures), which established the same
no-`proxy_cache_valid` rule for the `tile_content` zone. This extends the rule to
`/api/`; the reasoning is the same and the key is not.

## Context

Nothing under `/api/` is cached at the proxy. Every request is proxied to Django
and answered from the database, however identical the answer, and there has been
no reason to change that: STAC documents are cheap to build and are asked for by
browsers that cache them themselves.

That stops being true for a point forecast. It is the same document for every
caller until the next model run lands; it is asked for by consumer applications
rather than browsers; and it is the endpoint most likely to be hit hard enough
for the arithmetic to matter.

The instinct is to cache `/api/` responses for a few minutes. That instinct is
the danger. `/api/` is one organisation's whole public service (ADR 0012), and
what it serves includes a private collection's STAC document, a tenant-scoped
analysis result and a WMTS capabilities document that may list layers only its
caller may see (ADR 0014). A rule of the form "cache 200s for five minutes"
stores all of them, keyed by a URL that carries no identity, and serves them to
whoever asks next.

The usual mitigation — key the cache on the credential — is worse here than it
looks. It multiplies the cache by the caller, which for an API-key transport
means one entry per key, and it makes the *correctness* of the design depend on
having enumerated every way a credential can arrive. Get one wrong and the cache
is a data leak that nothing anywhere reports: every hit is a 200 with plausible
content.

## Decision

**One proxy cache zone for `/api/`, and nginx invents no lifetimes.** There is no
`proxy_cache_valid` anywhere the `api` zone is used, so nginx stores a response
only when the upstream declares one — `Cache-Control: public, max-age=N` or an
`Expires`. A view that says nothing is not cached.

This makes caching an **upstream decision**. The author of a view decides whether
its response may be shared, in the same file where they decide what is in it, and
nobody can turn a view into a cached one by editing the proxy. It is the rule
ADR 0021 already chose for tile bodies, and it is chosen again here for the
stronger reason: unlike a tile body, an `/api/` response can be tenant-scoped.

**The key is `$host$request_uri`.** The host is part of the key because on this
instance the host *is* the tenant (ADR 0012/0013): two organisations reach the
same paths on different hostnames, and a key without the host would let one
organisation's document be served on another's. This is the one place the `/api/`
cache differs from `tile_content`, whose key is `$request_uri` alone — a tile
body is derived from the COG and is identical for everybody allowed to see it,
which is precisely what an `/api/` document is not.

**Any credential means no cache.** `proxy_no_cache` and `proxy_cache_bypass` are
set on the session cookie, the `Authorization` header and `?api_key=`. The key
carries no credential, so without these a response to a signed-in request could
be stored and replayed to an anonymous one at the same URL, the moment any view
marks itself public.

`?api_key=` is in that list even though it is part of `$request_uri` and so could
not collide today. The rule being written down is "a response to a credentialed
request has no business in a shared cache", which stays true if the key ever
changes; "this credential happens to be in the key" does not.

**`Set-Cookie` and `Vary` are honoured, not ignored.** The `tile_auth` zone
ignores both deliberately, because its key already carries the credential. This
zone's does not, so a response that sets a session cookie is not stored at all,
and `Vary: Cookie` from `SessionMiddleware` makes nginx key variants by cookie.
Both are belts to the braces above.

## Alternatives considered

**`proxy_cache_valid 200 5m` plus a deny-list of paths.** The obvious shape, and
the one to avoid. It inverts the default: every route is cached unless somebody
remembered to exclude it, so the cost of forgetting is paid by the route that was
forgotten — and the routes most likely to be forgotten are the new ones, which
are exactly the ones nobody has thought about yet.

**Caching in Django instead, with the cache framework.** Already used for the
WMTS capabilities document, and correct there. Rejected as the general mechanism
because it does not remove the request from Django: the point of a proxy cache
for a hot public endpoint is that the hit never reaches the application.

**Keying on the credential rather than refusing to cache credentialed
requests.** Higher hit rate for signed-in callers. Rejected: it makes correctness
depend on a complete enumeration of credential transports, and the failure is
silent. Refusing outright costs a cache miss and cannot leak.

**No cache at all, and let the plugin's view set `Cache-Control` for clients
only.** Defensible — browsers and CDNs would honour it. Rejected because the
instance's own proxy is where the saving is largest and the one place we control.

## Consequences

- The zone is empty until some view opts in, and an operator who looks at it and
  sees nothing is seeing it work. This is worth saying out loud, because "the
  cache isn't caching" is the report that leads somebody to add
  `proxy_cache_valid`.
- A view that wants to be cached must be safe to serve to anybody, because that
  is what the marking means under a key with no identity in it. The plugin's
  forecast view marks only successful *public*-model responses, sends
  `private, no-store` otherwise, and marks no error response at all.
- The safety of the default is a property of core's views, not of the proxy. It
  is asserted by `core/tests/test_api_cache_contract.py`, which walks core's own
  `/api/` patterns and fails if any response declares itself public.
- `X-Cache-Status` is exposed on `/api/` responses, as it already is on
  `/titiler/`. It is diagnostic and it tells a caller whether they were served
  from the proxy, which for a public document is not a secret.
