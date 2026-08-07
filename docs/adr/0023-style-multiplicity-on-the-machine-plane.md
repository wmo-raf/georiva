# Multiple styles per variable, selected by query param on the machine plane

## Status

accepted

## Context

ADR 0022 makes styles per-variable rows (`VariableStyle`, one-to-many) with
exactly one default. The product wants full multiplicity served — e.g. an
"official public" palette and an "analyst" palette on the same variable — not
just staged alternates.

The serving plane was styleless throughout: the machine-plane address is
`org/catalog/collection/variable` (ADR 0013), the Redis palette key mirrors
it, Titiler's `SemanticColorMap` resolves one colormap per variable, and
STAC/EDR/dataset pages each expose one palette. The nginx `auth_request`
gate (ADR 0015) authorizes on the org segment; a style adds no tenancy of its
own — it is an attribute of a variable, governed by the collection's
visibility.

ADR 0021 made invalidation structural for encoded textures via a URL token
hashed from the encoding config. Colorized tiles carry no version token —
nginx's header-driven cache stores nothing for them today precisely because
their URLs cannot be trusted across a palette edit.

## Decision

1. **Style is a rendering parameter, not an addressable resource.** Tile,
   preview and tile-config URLs gain an optional **`?style=<slug>`** query
   param; omission means the default style. It is the same category as
   Titiler's existing `rescale`/`colormap` params. Route shapes, nginx
   location matching and the `auth_request` gate are untouched. All such
   URLs are built only by `core/machine_plane/addresses.py` (ADR 0013).

2. **Cache keys grow a style segment.** The Redis key becomes
   `georiva:palette:{org}:{catalog}:{collection}:{variable}:{style}`; the
   styleless key remains as the default style's alias so existing consumers
   keep working. `VariableStyle` `post_save`/`post_delete` signals warm and
   prune per-style keys (also closing the pre-existing gap where deletes and
   out-of-Wagtail stop edits never invalidated). Flipping `is_default`
   rewrites the alias key. Titiler resolves the key from the path params plus
   the `style` query param.

3. **Colorized outputs get a per-style version token.** URLs that bake a
   style into pixels carry `v=<short hash of (value_min, value_max,
   scale_type, style stops)>`, extending ADR 0021's token scheme; encoded
   textures keep the styleless token (encoding is style-independent). This
   makes colorized responses honestly cacheable for the first time — an
   edited style changes the URL. The nginx body cache must key on the full
   request line including the query string (verify in the proxy config
   before enabling caching for these routes).

4. **Discovery rides existing surfaces; no new endpoint.** The tile-config
   payload becomes the resolved style's config plus a
   `styles: [{slug, title, is_default}]` index; fetching a specific style's
   config takes the same `?style=` param. STAC collections/items adopt the
   standard **Render extension** (`renders`) to enumerate named styles with
   their colormap/rescale, which STAC Browser and Titiler-based clients
   already understand. EDR and dataset pages serve the default style only
   until a consumer needs more there. Visibility is inherited from the
   collection — no new auth surface, and `config_view.py`'s public-only
   constraint applies to styles exactly as it does to the default config.

## Consequences

- A dashboard or public map can pin a non-default style by URL today, and
  operators can stage an alternate style and promote it to default without
  any client change.
- An unknown `?style=` slug resolves like a missing config (404 via the
  tile-config lookup), never a fallback to the default — silently serving
  the wrong style would be worse than failing.
- Warm-all and prune sweeps iterate styles, not just variables; key
  cardinality is styles-per-variable, which stays small in practice.
- The styleless Redis alias is a compatibility seam: if it ever diverges
  from the default style's own key, that is a bug in the default-flip
  signal, not a feature.
- Selecting styles in EDR responses and dataset pages is deliberately
  deferred; adding it later is serializer work, not model or plane work.
