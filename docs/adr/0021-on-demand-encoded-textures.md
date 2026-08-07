# Encoded textures are derived on demand, never stored

## Status

accepted

## Context

Every ingested (or derived) variable used to materialize an asset trio: the
COG holding the raw values, a JSON sidecar, and an **encoded PNG** — the raw
values rescaled into the red channel against the variable's
`value_min`/`value_max` and masked through alpha. WeatherLayers GL consumes
that PNG as a texture and unscales it client-side (`imageUnscale`), which is
what makes hover readouts, temporal interpolation and particle layers
possible; Titiler's colorized tiles cannot provide those, because the values
are gone by the time a colored tile reaches the browser.

Baking the range into pixels created a staleness class with no owner. The
moment an admin edits `value_min`/`value_max`, every existing PNG is not
merely outdated but **numerically wrong**: clients decode with the variable's
*current* range, so old pixels unscale to values they never held. Fixing that
honestly would have required regeneration machinery — staleness marking on
every asset, a confirm-then-enqueue admin flow, a sweep backstop, and a
serving story for the mixed old/new window while thousands of items re-encode.

Meanwhile the tile plane already renders **on demand**: `SemanticRescale` in
`titiler-app` resolves the variable's current range from Redis/Django per
request, and no tile response is cached. The encoded PNG was the only
render-config-dependent artifact we persisted.

A prototype (branch `prototype/encoded-preview-endpoint`, commit `8b4b1c5`)
answered the load-bearing question: a Titiler route serving the whole extent
as a single band rescaled to the current range is **pixel-identical** to the
baked PNG (same native grid, max channel difference 1 from rounding, 100%
alpha-mask agreement on `central/chirps/chirps-monthly/precip`), and
WeatherLayers renders it indistinguishably, hover value readout included.
Uncached generation cost ~700 ms for a 1500×1600 COG — the same cost class as
the uncached tiles the platform already serves.

## Decision

**Artifacts that depend on render configuration are derived at request time,
never stored.** Concretely:

1. `titiler-app` gains `GET
   /{org}/{catalog}/{collection}/{variable}/encoded-preview.png` — the full
   extent as one image, single band rescaled to the tile-config's current
   `vmin`/`vmax`, no colormap ever, nodata as alpha. It reuses the exact
   dependencies the tile routes use (`SemanticPathParams`,
   `SemanticTileConfig`), so the range applied is always the current one and
   no tenancy decision is taken in the tile server (ADR 0013/0015 unchanged —
   the endpoint sits behind the same nginx `auth_request` gate).

2. The URL is built **only** by `core/machine_plane/addresses.py` (ADR 0013)
   and carries `v=<short hash of (value_min, value_max, scale_type)>`. The
   endpoint replies `Cache-Control: public, max-age=31536000, immutable`.
   Invalidation is therefore structural: changing the range changes the URL,
   and every cache between the browser and the COG may be maximally
   aggressive without ever serving a wrongly-scaled texture. The token is a
   hash, not a stored counter — identical config always yields the same URL
   and there is no version state to migrate or drift.

3. `AssetMaterializer` stops writing PNGs; the RGBA encoder and its chunked
   plumbing through ingestion are removed. The materialized trio becomes a
   pair (COG + sidecar). No Asset row with `roles=['visual']` is created
   anywhere.

4. The "visual asset" every consumer previously read from the database
   (`asset.url` + `imageUnscale` extra field) becomes **computed**: the STAC
   serializer, the admin item preview and the dataset pages derive the
   encoded-preview URL and its unscale range from the Variable at read time,
   through `addresses.py`. Nothing stored, nothing to invalidate.

With no encode-time state anywhere, the original questions — how to detect
stale assets, how to warn the user about regeneration, how to mark assets for
recreation — are moot by construction. A range change takes effect on the
next texture request.

## Consequences

- Changing `value_min`/`value_max` is an ordinary config edit: instantly
  live, nothing to regenerate, no admin ceremony.
- First-view latency per texture is the on-demand generation cost (~700 ms
  for a mid-size grid), paid once per URL per browser thanks to `immutable`.
  A shared nginx `proxy_cache` for `/titiler/` images is a deliberate
  **non-decision**: the URL contract already supports it, and it can be added
  as pure ops tuning if real traffic warrants.
- Existing baked PNG objects and their Asset rows are cleaned up manually
  (nothing is deployed yet); no migration or management command exists, and
  none should be added later without revisiting this ADR.
- `Asset.Format.PNG` remains in the model vocabulary but nothing produces it.
- The prototype evidencing pixel-identity is preserved on branch
  `prototype/encoded-preview-endpoint` as the primary source for this
  decision.
