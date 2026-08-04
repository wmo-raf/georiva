# The org-hopper in the admin sidebar

## Status

accepted

## Context

Admin tenancy is host-scoped: the Host resolves the Organisation, and every
listing, chooser, page tree and permission check derives from it (ADR 0011,
ADR 0016). The subdomain *is* the switcher — there is deliberately no session-org
state to flip.

That leaves two gaps for the people this tenancy exists for. A regional-centre
operator who belongs to three institutions has no way to get from one
institution's admin to the next except by typing a hostname, and no reminder of
which one they are in — every admin page looks the same on every host, which is
exactly how somebody publishes Kenya's collection from Uganda's admin. ADR 0016
named this as the affordance still owed.

A prototype (`prototype/org-hopper-menu`, throwaway) put three variants in front
of the team. The workspace block at the top of the sidebar — Slack's shape — won:
it is the only one of the three that is *always visible*, so it answers "which
organisation am I in" without being opened, which is the more common question.

The obstacle is where Wagtail lets an application put it. The admin sidebar is a
React app rendered from a JSON props blob, and the two hooks that reach the admin
chrome globally — `insert_global_admin_css` and `insert_global_admin_js` — are
called **with no arguments**. They cannot see the request, so they cannot know
which organisations to name. Nothing else is global: `construct_main_menu` has
the request but only builds menu items, and overriding `wagtailadmin/base.html`
would mean carrying a copy of Wagtail's furniture across upgrades.

## Decision

**The org-hopper is server-rendered markup, delivered over its own request, and
mounted into the sidebar by a static script.**

Three pieces, and the split follows exactly what varies:

- `organisations/hopper.py` decides everything — who is listed, which entry is
  current, whether there is a popover at all, whether the list gets a search box.
  All of it server-side, from real membership rows.
- `/admin/org-hopper.js` is a view. It renders the block and hands it to
  `window.georivaOrgHopper.mount(…)` as a JavaScript string literal. It is
  `private, no-store` and varies on `Cookie`: this is per-user markup on a URL
  every admin page loads.
- `organisations/static/organisations/org_hopper.js` is behaviour only, and
  therefore static and cacheable: insert the block above `.sidebar-main-menu`,
  work the popover, filter on search.

`insert_global_admin_js` emits both `<script>` tags, deferred, in that order —
which is all a request-blind hook needs to do.

**The route lives in `config/urls.py`, not in `register_admin_urls`.** That hook
wraps its URLs in `require_admin_access`, which answers an anonymous request with
a redirect to the login page — and the login page loads this script too, so every
sign-in would fetch a `<script src>` that redirects to HTML. The view has its own
answer for a request with nothing to show: an empty script.

**Amended: the route is also outside the tenancy guard, and a non-member is one
of the requests with nothing to show.** Staying clear of `require_admin_access`
turned out to be half the job. `OrganisationMiddleware._guard_admin` refuses
every `/admin/` URL to a signed-in non-member, and a refusal is a rendered HTML
page — so a member of one organisation who lands on another's sign-in page hit
the identical failure this decision was written to avoid, by the other route.
The URL therefore joins `organisations.middleware.admin_open_paths`, alongside
the four Wagtail itself leaves unauthenticated for the same reason.

Being open is only safe because the view scopes itself, so the scoping is now
part of this decision rather than a property of the guard in front of it:
`org_hopper_context` returns `None` for a signed-in non-member of the host
organisation, and the view renders that as the empty script it already had. It
previously named the host organisation in that case, reasoning that a block
naming nobody was worse than a block naming the host — sound while the guard
made the case unreachable, wrong once the sign-in page can reach it, because a
stranger's sidebar should not be captioned with an institution they have nothing
to do with.

**A MutationObserver keeps the block mounted.** It is inserted into DOM that
React owns, and React re-renders the sidebar when it is collapsed or the viewport
changes. Rather than fight that, the observer puts the block back if it goes.

**Who is listed is stated, not inherited.** This is the one admin surface that
names organisations other than the host's, so the rule is written down here: a
member sees the organisations they hold a membership row in; the instance admin
sees all of them, which is what `is_superuser` already buys everywhere else. The
list is read per request, so a revoked membership drops off the next page load.

**The search box follows the list's length, not the reader's role.** #270 tied it
to superusers ("Superusers: the block lists all organisations, with a search box
once the list exceeds ~8"), but the reason for a search box is a list too long to
scan, and a regional-centre operator who belongs to a dozen institutions has that
list too. The threshold is the rule; who assembled the list is not.

**Entries are scheme-relative links** — `//<host>/admin/`, not
`https://<host>/admin/`. The deployment terminates TLS ahead of nginx and
`SECURE_PROXY_SSL_HEADER` is deliberately not configured (the forwarded header is
client-supplied at the edge), so `request.is_secure()` reads `False` on an HTTPS
instance: an explicit scheme would hand out `http://` links to every other
organisation. The browser is the one party that reliably knows the scheme the
current page was loaded over, and every organisation on an instance is served the
same way. The port comes from this request's own `Host`, which nginx forwards
verbatim.

## Consequences

- Every admin page load makes one extra request. It is small, uncached by design,
  and it is the price of a request-blind hook.
- The hopper writes nothing — no cookie, no session key, no "last organisation".
  A link is the whole mechanism, so a stale tab cannot leave somebody pointed at
  an organisation they have left: the host they are on decides, on every request.
- Hopping does not carry a user anywhere they could not already reach by typing
  the hostname. The links are wayfinding over access that already existed; the
  middleware still resolves and checks membership on arrival.
- The mount points (`#wagtail-sidebar`, `.sidebar-main-menu`) are Wagtail
  internals. A Wagtail upgrade that renames them leaves the block unmounted — no
  error, just an admin without a hopper. The insertion is guarded rather than
  asserted for that reason.
- If `SECURE_PROXY_SSL_HEADER` is ever configured, the scheme-relative link stays
  correct; nothing here needs revisiting.
