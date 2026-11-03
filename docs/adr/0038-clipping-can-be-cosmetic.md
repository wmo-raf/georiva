# Clipping can be cosmetic: masking moves to the map

## Status

accepted

## Context

A catalog clips its data to a boundary at ingestion, with `clip_mode`: `none`, `bbox` (crop to the boundary's bounding
box) or `mask` (crop, then blank everything outside the geometry). `mask` is the default, and it is the only way to
get a country-shaped map.

It does that by writing NaN into the stored COG. Every consumer then reads the NaN: STAC, EDR, the analysis endpoints,
zonal statistics, virtual Zarr, and a publisher plugin that has to hand a value for every point of an area. A Forti
publication cannot store "no value" — it packs int16 with no missing-value code, so the plugin refuses the whole
publish rather than write a believable wrong number — and a Kenya-shaped WRF run left 39% of its cells NaN. The same
file cannot be both cropped for the map and complete for a point forecast.

In the case that surfaced this, the crop was only ever for how the map looks. Nothing wanted the data outside the
border withheld; it just was not wanted on screen.

## Decision

**A fourth `clip_mode`, `display`, makes the clip cosmetic.**

- At ingestion it is a plain bounding-box crop: the data is stored complete over the boundary's bounding box, with no
  NaN from a mask. `service.py` and the derivation engine already read the mode as "clip unless `none`, mask only
  when `mask`", so no ingestion code changes.
- The catalog's boundary travels to the tile server in the tile config (`boundary`, a simplified GeoJSON geometry, in
  both the Redis payload and the `tile-config` endpoint). Titiler masks each tile to it where it rescales, so the
  colormap draws nothing outside. WMTS `GetTile` re-enters the same tile route and inherits it. The value-encoded
  texture (`encoded-preview.png`, ADR 0021) is cropped the same way, through the same helper, because it is read
  from the COG without going through the tile algorithm and is what WeatherLayers draws and reads hover values from.
- WMTS `GetFeatureInfo` follows the map: a click outside the boundary answers a null value, as it does for any
  point with no data.
- Saving a catalog re-warms the render config of every variable under it, because the outline lives in each.

`none`, `bbox` and `mask` keep exactly their meaning. `display` is opt-in per catalog, and nothing changes for an
install that does not choose it.

**Only the visualization plane is cropped.** EDR, the analysis endpoints, zonal statistics, STAC, virtual Zarr and any
publisher read the stored, uncropped data. If an installation needs values outside a border to be unavailable
everywhere, that is a data rule and not this: it should keep `mask`.

## Alternatives considered

**Redefine `mask` as a render-time crop.** Rejected: it would silently stop every existing deployment from masking its
stored data the next time it ingests.

**A second, unmasked copy of the data for publishers.** Rejected as the answer to a cosmetic need: it stores and
processes every run twice. It remains the way to do it for a catalog that keeps `mask`.

**Let a publisher invent values past the border** (fill from the nearest valid cell). Rejected: a point forecast
outside the model's valid area would carry made-up numbers.

**A `clip_margin` around the bounding box.** Rejected: the data then covers a margin nothing asked for, and the
publisher's own area follows the data extent instead.

## Consequences

- A migration alters `Catalog.clip_mode`'s choices; no data changes.
- Titiler gains `app/boundary.py` (the tile mask and a point-in-polygon test) and reads `boundary` from the tile config.
- The outline is simplified to 0.002° before it is cached, which is far finer than any grid a map is drawn from and
  keeps a country outline from being hundreds of kilobytes in every Redis value. A change to the boundary row itself
  (rather than the catalog) is not seen until the next palette warm.
- The encoded texture is served `immutable` under a `v` token that hashes what is baked into it. That token now
  includes the display boundary's id, so moving a catalog to or from `display` is a different URL while every other
  mode keeps the one it had. Without it a browser would keep the uncropped texture it already fetched.
- Items ingested before a catalog moves to `display` stay masked until they are re-ingested.
- Publisher areas follow the data's extent, so a plugin's area picker no longer pads a country by a margin.
