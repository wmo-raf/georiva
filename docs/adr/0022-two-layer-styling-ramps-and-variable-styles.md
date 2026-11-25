# Styling is two layers: value-free color ramps, per-variable style snapshots

> **Amended by #382 — the styling surface previews on real data.**
> The surface in §6 gains a live map of the latest item, colored in the browser
> from the form's unsaved stops; and re-applying a ramp in §3 stops being a
> database write. Both are amendments to how the surface behaves, not to the
> two-layer model, which is untouched. See the amendments under **Decision**.

> **Amended by #806 — every style records its origin: template, ramp or custom.**
> How a style was made is stored, and decides what drives its stops and which controls the editor shows. A ramp
> style's stops are always its ramp's; a template style's are its template's; only a custom style's are tuned by
> hand. "Customise stops" is the one way from the first two to the third. This replaces the #801 note about live
> regeneration until stops are hand-tuned. See §3.

> **Amended by [ADR-0046](0046-values-are-stored-as-produced-and-converted-when-read.md) — stops are in the source unit.**
> A stop's "absolute physical value", and `value_min`/`value_max`, are stored in the Variable's source unit and
> shown in its served unit; the styling surface converts at its boundary, and a ramp is applied in the served unit.
> The two-layer model is untouched.

> **Amended by [ADR-0049](0049-a-variables-scale-type-is-its-encoding-curve.md) — `scale_type` is an encoding curve.**
> `scale_type` no longer only spaces a ramp's stops: it chooses the curve values are encoded through, in Titiler and
> in the browser's decode alike. The #382 consequence below, that a log-declared variable renders linearly, no
> longer holds. A curve change never rewrites stops.

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
   lineage/re-apply only (superseded by the #806 amendment in §3: a ramp
   style's stops are its ramp's on every save), step count + mode (continuous/stepped), and the
   materialized absolute value→color **stops snapshot**. Applying a ramp
   over the variable's range *generates* stops; the operator may then
   fine-tune any stop, including pinning physical thresholds.

3. **Snapshot semantics, not live-linking.** Changing `value_min`/`value_max`
   after fine-tuning never silently rewrites stops; the styling UI offers
   "re-apply ramp", which regenerates the snapshot and discards fine-tuning
   with a warning. (A live-linked or pinned-stop hybrid is deferred until an
   operator actually loses tuning they care about.) *Superseded by the #806
   amendment below: there is no "re-apply ramp" gesture; a ramp style is
   regenerated on every save, and tuned stops exist only on a custom style.*

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

   **Amended (#806): every style records its origin.** #801 guessed whether a
   style's stops were "as generated" by regenerating its ramp over the current
   range and comparing, and let ramp changes regenerate live only while they
   were. That guess is gone. `VariableStyle.origin` records how the style was
   made, and is set once: applying a template gives `template`, starting from a
   ramp gives `ramp`, starting from scratch gives `custom`.

   - A **ramp** style's stops are always what its ramp, mode, steps and reverse
     (now stored) produce over the Variable's range. Its editor shows those
     controls and no stops table; any change refills the stops through the
     read-only endpoint and repaints the map, and a save regenerates them on
     the server — posted stops are never taken. Saving the range never
     touches stops, so a ramp style saved over an older range keeps its old
     stops until its next save; its editor shows the stops that save will
     store, marked unsaved, with a note that they are laid out over the old
     range.
   - A **template** style's stops are its template's bands. No save changes
     them, except a template picked in place (#807); its editor shows the name, the template it came from and the
     out-of-range settings.
   - A **custom** style's stops are what the operator saves: the stops or
     classes table, JSON, and Reverse, which reverses colours in place. This is
     the only place stops are tuned by hand, so pinning 0 °C means a custom
     style.

   "Customise stops" turns a ramp or template style into a custom one, after a
   confirmation, keeping its stops; the ramp or template link stays as history
   and never drives anything again. "Regenerate from ramp" and "Restyle from a
   color ramp" are gone with the guess. The rule above holds more strictly than
   before: tuned stops can only exist on a custom style, and nothing but its own
   save rewrites them. A data migration recorded the origin of every existing
   style by the #801 comparison, once: a hand-tuned ramp style became custom.

   **Amended (#807): a style's path can be switched without making a new one.**
   A template style's editor picks its template in place, from the catalogue
   the chooser offers; another pick repaints the preview at once and a save
   writes its bands and link. "Start over", on every path, reopens the chooser
   for the selected style and, after a confirmation, replaces its origin, stops
   and ramp or template link. Both keep the style's name, slug and default
   status. Only the default style may take a template's range and curve this
   way, as only a first style may in the chooser.

4. **`Variable` keeps `value_min`/`value_max`/`scale_type`.** They are the
   encoding contract (`imageUnscale`, the ADR 0021 render-config token), not
   styling. The `min < max` validation moves onto `Variable.clean()`,
   replacing the three form copies; the three aliases of the range tuple
   collapse to one.
   `imageUnscale` is only ever derived from this range, never from a style's
   stops (their span is a legend concern): `machine_plane/render_contract.py`
   builds it once, and the STAC asset, styling preview, EDR `x-georiva` block
   and item preview page all read it from there.

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
   _Amended (#792):_ a fourth tier, `style_template` (a Style template
   slug), sits between `palette_stops` and `palette`. It seeds a stepped
   default style converted into the source unit and sets the range and
   `scale_type` from the template — set, not proposed, since a new Variable
   has no encoding to protect. Still create-only.

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
  limits. ~~`scale_type` is carried in the tile-config payload but consumed by no
  Titiler code path, so a log-declared variable renders linearly on both sides
  and the preview matches by accident rather than by design; implementing log
  must change both or the preview silently diverges.~~ Superseded by ADR-0049:
  both sides encode and decode through the curve `scale_type` names. The texture reaches the
  browser through the ADR 0015 gate on the operator's own session, so an
  operator on a host that resolves a different organisation gets the
  texture-failed placeholder rather than a map.
