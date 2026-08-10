# Remove the JSON metadata sidecar

## Status

accepted

Extends ADR 0021 (on-demand encoded textures) and ADR 0022 (two-layer
styling).

## Context

Materialization historically wrote a trio per variable per timestep: COG,
encoded PNG, and a JSON metadata sidecar. ADR 0021 removed the stored PNG
(textures are derived on demand by Titiler), turning the trio into a pair.
ADR 0022 / #325 then stripped the sidecar's render-config keys
(`imageUnscale`, `scale`, `color_map`) because stored render config is stale
by design, leaving an 11-key descriptive sidecar (identity, geometry, stats).

An audit of the whole codebase found the remaining sidecar is **write-only**:

- Written in exactly one place (`ingestion/materialization.py`, via
  `AssetWriter.write_metadata`), reached by ingestion, the derivation
  engine, and `rematerialize_derived_assets`.
- Read in exactly zero places. It has no `Asset` row, so it never appears
  in STAC/EDR output; titiler-app reads only the COG; the frontend reads
  EDR JSON and tile-config. WeatherLayers GL consumes the on-demand
  `encoded-preview.png` plus live palette/unscale config — it never touched
  the sidecar, and its historical tie (the render-config keys) was already
  severed by #325 with no fallout.
- The only sidecar-aware code was the orphan-cleanup sweep, which
  deliberately *excluded* it from deletion.
- No external (out-of-repo) consumers exist; the #325 key-stripping served
  as a canary for any silent reader.

Everything the sidecar carried is available authoritatively elsewhere: the
`Asset`/`Item`/`Variable` rows, the COG's own embedded georeferencing and
tags, and the STAC API.

## Decision

1. **Stop producing the sidecar.** Materialization writes the COG only.
   `AssetWriter.write_metadata` is deleted outright rather than left as an
   unused API — a dangling writer invites a caller, which is how stored
   render config crept back historically.

2. **Existing sidecars are left in place ("leave to rot").** No cleanup
   sweep. Every pre-0024 materialization keeps its `{base_name}.json`
   sibling on the buckets indefinitely. The orphan-cleanup carve-out
   (`core/storage/asset_cleanup.py` excludes `.json` from
   `DELETABLE_EXTENSIONS`) therefore remains load-bearing and keeps its
   guard test — it now protects legacy files instead of live ones.

3. **`Asset.Format.JSON` stays in the model vocabulary.** It was never used
   and costs nothing; a future, properly STAC-registered metadata asset may
   want it.

## Consequences

- The materialized output per variable per timestep is a single COG with an
  `Asset` row. `AssetMaterializer` has no non-fatal branch left: COG failure
  raises, and there is nothing else to fail.
- Buckets contain two generations of layout: pre-0024 prefixes have a
  `.json` next to each `.tif`; newer prefixes do not. This is expected and
  documented here — do not "fix" it, and do not let a future extension-based
  sweep eat the legacy files (the kerchunk manifests in `virtual_zarr/` are
  also `.json` objects on a bucket).
- Any hypothetical unknown reader degrades gracefully per-timestep: old
  files keep working; only new timesteps lack the file.
- `docs/plugins/georiva-storage-architecture.md` and
  `docs/architecture/README.md` still describe the pre-0021 trio and are
  wrong about more than the sidecar; their rewrite is tracked as a separate
  issue rather than patched piecemeal here.
