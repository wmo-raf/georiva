// STAC Browser's catalog root, resolved in the browser from the host it was
// served from.
//
// Tenancy on this instance lives in the hostname (ADR 0011): every organisation
// has its own host and its own STAC root, and the root ids stay bare, so
// `/api/stac/` means Kenya's catalog on Kenya's host and Uganda's on Uganda's.
// STAC Browser knows nothing about any of that — it is a static SPA with one
// catalog URL — so the URL has to be computed at page load rather than baked in.
//
// It has to be a *function* for that, and neither of the two simpler forms
// works:
//
//   A string is fixed at build time, so one image could only ever point at one
//   organisation. Every tenant visiting their own /stac-browser/ would be shown
//   whichever organisation was baked in.
//
//   A relative URL ("/api/stac/") is not supported, and fails in a way that
//   looks like a different bug: `isExternalUrl` resolves each link against
//   `catalogUrl` with `URI.relativeTo`, which returns the input unchanged when
//   the base is relative, so *every* link in the catalog is classified as
//   external and routed through /external/ — or refused outright, given
//   `allowExternalAccess` below.
//
// The function form is upstream's own documented answer to this (see
// docs/options.md#catalogurl in the pinned tag). It can only be set from a
// config module, which is why this file — and the build context around it —
// exists at all; SB_catalogUrl is typed as a string in config.schema.json and
// the image's entrypoint JSON-encodes it.
//
// Config precedence upstream, lowest to highest:
//   config.js -> SB_CONFIG (this file) -> SB_* env vars -> runtime-config.js
// The last two are written by the container entrypoint from SB_* variables, so
// setting SB_catalogUrl anywhere would silently win over this file. Nothing in
// docker-compose.yml sets it, and nothing should.
export default {
  catalogUrl: () => `${window.origin.toString().replace(/\/?$/, "")}/api/stac/`,

  // Only ever shown if a root catalog reports no title of its own. Ours always
  // does — the STAC landing page is titled after the organisation serving the
  // host — so this is the instance name rather than any one organisation's.
  catalogTitle: "GeoRiva",

  // Keep the browser inside the catalog it was pointed at. With catalogUrl
  // following the host, "external" means another origin — including another
  // organisation's — and there is no reason for a tenant's browser to wander
  // there. Assets are unaffected: they are rendered and downloaded, not browsed.
  allowExternalAccess: false,
};
