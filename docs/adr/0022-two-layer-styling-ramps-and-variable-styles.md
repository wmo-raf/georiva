# Styling is two layers: value-free color ramps, per-variable style snapshots

> **Amended by #382 — the styling surface previews on real data.**
> The surface in §6 gains a live map of the latest item, colored in the browser
> from the form's unsaved stops; and re-applying a ramp in §3 stops being a
> database write. Both are amendments to how the surface behaves, not to the
> two-layer model, which is untouched. See the amendments under **Decision**.

## Status

accepted

## Context

All render styling used to hang off a single pair of models: `ColorPalette`
(org-tiered, shared by FK from `Variable.palette`) and its `PaletteStop` rows,
whose `value` fields hold **absolute physical values**. `Variable.clean()`
demanded the palette's first/last stops match the variable's
`value_min`/`value_max` within 0.01 — so a palette could effectively serve
only variables with identical ranges, and editing a range without editing the
palette (or vice versa) made the pair invalid. "Reusable" palettes were not
reusable in practice, and a fresh instance shipped with zero of them.

The write side was scattered and contradictory. Five independent sites set
ranges/palettes, each with its own policy: the source setup service used
`update_or_create` with the range in `defaults` — **silently resetting
operator-tuned ranges to the plugin's declared range (or a hardcoded 0.0–1.0)
on every re-provision** — while the upload wizard and derived-product recipes
used `get_or_create` and the manual-upload views documented
"operator-is-truth". The plugin contract's `CollectionVariable.palette` field
was parsed and discarded; no consumer read it. Three form-level copies of the
`min < max` check existed and none on the model. Cache invalidation needed a
`ColorPalette post_save` fan-out over `variable_set` because stops lived away
from the variable, and `PaletteStop` edits outside Wagtail never re-warmed.

Mature GIS systems (QGIS, matplotlib/rio-tiler, ColorBrewer) all converge on
the same two-layer shape: a **value-free color ramp** picked from a catalog,
*applied* over a layer's min/max into concrete value→color entries that are
then hand-tunable. Meteorology needs that final hand-tuning stage because
authoritative palettes pin colors to physical thresholds (0 °C, warning
levels) — a pure normalized stretch cannot express that, and a purely
absolute palette cannot be reused.

## Decision

Styling splits into a reusable **aesthetic** layer and a per-variable
**semantic** layer. Concretely:

1. **`ColorRamp`** (new model) is the catalog entry: ordered colors with
   optional 0–1 positions and a type (sequential/diverging/qualitative). It
   carries **no values**. It is tiered exactly like other shared reference
   data (nullable `organisation` = global tier, ADR 0011). A data migration
   seeds ~15–20 curated instance-wide ramps under matplotlib-compatible names
   (viridis, cividis, RdBu, ColorBrewer sequentials, grayscale) so plugins
   and operators always have a base vocabulary.

2. **`VariableStyle`** (new model, one-to-many from `Variable`) is the
   applied result: name/slug, `is_default` (exactly one per variable,
   enforced by a partial unique constraint), a nullable `ramp` FK kept for
   lineage/re-apply only, step count + mode (continuous/stepped), and the
   materialized absolute value→color **stops snapshot**. Applying a ramp
   over the variable's range *generates* stops; the operator may then
   fine-tune any stop, including pinning physical thresholds.

3. **Snapshot semantics, not live-linking.** Changing `value_min`/`value_max`
   after fine-tuning never silently rewrites stops; the styling UI offers
   "re-apply ramp", which regenerates the snapshot and discards fine-tuning
   with a warning. (A live-linked or pinned-stop hybrid is deferred until an
   operator actually loses tuning they care about.)

   **Amended (#382): applying a ramp writes nothing.** The gesture was a POST
   that regenerated the snapshot in the database, guarded by a confirm
   round-trip because it destroyed work. That guard existed to protect the
   operator from a step they could not see the result of — the only way to find
   out what a ramp looked like on real data was to commit to it first. With the
   map preview, the answer is available before the commitment, so the
   destruction is unnecessary: a read-only endpoint returns the stops
   `generate_stops` would produce, the form is filled with them unsaved, and the
   map repaints. Save is the only thing that persists, and a reload undoes it.
   The warning survives, now telling the truth — it is about unsaved form state,
   not a snapshot. `VariableStyle.apply_ramp()` is unchanged and still seeds a
   style created with a ramp and no stops.

   This removes the surface's last no-JS path. The page now carries a WebGL map;
   assuming JavaScript there is not a new assumption.

4. **`Variable` keeps `value_min`/`value_max`/`scale_type`.** They are the
   encoding contract (`imageUnscale`, the ADR 0021 render-config token), not
   styling. The `min < max` validation moves onto `Variable.clean()`,
   replacing the three form copies; the three aliases of the range tuple
   collapse to one.

5. **Provisioning seeds; only the styling surface tunes.** The plugin
   contract becomes a tiered, all-optional, **create-only** seed with
   precedence `palette_stops` > `palette` > grayscale:
   - `value_range` — as today, but the setup service stops carrying it in
     `update_or_create` defaults: a re-provision never touches an existing
     Variable's range or styling.
   - `palette: str` — the previously dead field, now a ramp name; the system
     stretches it over the range into the default style's snapshot.
   - `palette_stops: list[(value, hex)]` — exact canonical stops,
     materialized verbatim; when present the **range is derived from the
     stops** (declaring both merely warns on disagreement).
   Unknown ramp or malformed stops degrade one tier with a warning;
   provisioning never fails on styling.

6. **One editing surface.** A collection-level Styling page (range + swatch
   per variable at a glance) drills into the single canonical per-variable
   form: range, scale type, ramp picker, steps (continuous by default;
   stepped offers 7 classes), stop fine-tuning, live legend preview, and
   management of the style set with default promotion. The Wagtail inline
   Variables panel and the manual-upload forms drop their range/palette
   fields in favor of a read-only swatch + link. The upload wizard keeps its
   auto-scanned range inputs — that is seeding, not tuning.

   **Amended (#382): the surface previews on real data.** A gradient bar cannot
   answer the question the operator has while placing a threshold — does this
   land where the weather is? — so the form carries a map of the latest item
   beside the stops, pinned so it cannot scroll away while they work.

   It costs no new serving machinery, because ADR 0021 already separated values
   from color: `encoded-preview.png` is the item's extent with pixel =
   rescale(value, vmin→vmax, 0→255), and the browser unscales it back to
   physical units and colors it on the GPU. Recoloring is therefore a palette
   swap — a stop edit repaints with no request and no write — and the palette is
   built from the same stops `as_weatherlayers_palette()` hands the tile config,
   so the preview is what Titiler will serve rather than an approximation of it.
   Stepped mode needs no special case: §3's snapshots already carry their class
   boundaries as doubled stops.

   Which item: newest `reference_time`, then its earliest valid time. Plain
   `-time` ordering would pick a forecast feed's furthest horizon, and styling
   against a ten-day-out field judges a guess.

   Two edges the panel names rather than hides. The texture is encoded against
   the *saved* range, so stops outside it render as flat clipped color — the
   panel says so instead of letting that read as a bad ramp. And when there is
   nothing to draw, it says which of the three reasons applies (nothing ingested
   yet, no COG, no recorded extent), because on a fresh feed only the specific
   answer tells the operator the problem is not theirs.

7. **`ColorPalette`/`PaletteStop` are retired.** A data migration
   materializes each variable's assigned palette into a default
   `VariableStyle` and normalizes each distinct palette (values → 0–1) into
   an org-tier `ColorRamp` so its aesthetic survives into the catalog.

## Consequences

- Reusability gets an honest meaning: what is reused is the aesthetic (the
  ramp); what is per-variable is the semantics (values). No shared mutable
  styling object remains, so the copy-on-write and shared-mutation hazards of
  the old model are unrepresentable.
- Cache invalidation simplifies: styles are variable-owned rows, warmed and
  pruned by their own `post_save`/`post_delete` signals — the palette
  `post_save` fan-out and the missing `PaletteStop`/delete signals disappear
  as a class (serving-plane details in ADR 0023).
- A freshly provisioned automated feed comes up looking reasonable (declared
  ramp or canonical stops) instead of grayscale over 0.0–1.0, and an
  operator's tuning survives every re-provision.
- Plugins that omit `value_range` still get the 0.0–1.0 fallback at create
  time; the fix here is that it is applied once, not re-applied on every
  provision.
- Derived-product recipes keep their create-only `get_or_create` behavior,
  now consistent with every other write path.
- `ingestion/materialization.py` stops baking `color_map`/`imageUnscale`
  snapshots into JSON sidecars — under snapshot styles and multiplicity they
  are stale by design and rebuildable from tile-config (extends ADR 0021's
  "no stored render-config-dependent artifacts").
- **(#382)** The preview inherits ADR 0021's separation whole, including its
  limits. `scale_type` is carried in the tile-config payload but consumed by no
  Titiler code path, so a log-declared variable renders linearly on both sides
  and the preview matches by accident rather than by design; implementing log
  must change both or the preview silently diverges. The texture reaches the
  browser through the ADR 0015 gate on the operator's own session, so an
  operator on a host that resolves a different organisation gets the
  texture-failed placeholder rather than a map.
