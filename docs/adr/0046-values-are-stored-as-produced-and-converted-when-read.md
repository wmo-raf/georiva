# Values are stored as produced and converted when read

## Status

proposed — see wmo-raf/georiva#737. Supersedes the last sentence of
[ADR-0044](0044-an-upstream-store-is-harvested-step-by-step-and-titiler-reads-the-store.md) §9 ("nothing is
de-accumulated or converted on the way") for units: still nothing is de-accumulated. Gives
[ADR-0043](0043-the-read-plane-reads-an-assets-bytes-wherever-its-href-points.md) §1's location seam its value-reading
counterpart. Restates [ADR-0022](0022-two-layer-styling-ramps-and-variable-styles.md)'s "absolute physical values" as
values in the source unit. Leaves [ADR-0021](0021-on-demand-encoded-textures.md)'s encoding untouched, and
#725 (a materialized copy is byte-identical to its upstream) true.

## Context

A Variable has a `source_unit` (what the producer sends) and a `unit` (what we show). Today ingestion converts one to
the other as it writes the COG, and every reader takes the bytes as being in `unit`. That held while every asset was
written by us. It no longer does:

- A **Remote** or **Store** asset (ADR 0042, 0044) is bytes we never wrote. AIFS's `temperature_2m` is Kelvin
  upstream and cannot be rewritten. ADR 0044 §9 therefore served it as it came, and the only way to show it in °C was
  to label it °C — which a dev feed does today: Kelvin values, a °C label, a −40…40 palette.
- A **materialized** copy is byte-identical to its upstream file (#725), so it is local and still unconverted. Titiler
  opens a local asset by a URL built from the request path and consults no row, so it cannot tell such a copy from an
  ingested file.
- Changing `unit` on an ingested Variable means re-ingesting it: the old unit is baked into every COG, every asset
  statistic and every zonal-statistics row.
- A consumer with an outside contract — the Forti publisher must publish °C, mm and hPa — has nowhere to ask for a
  unit, and reads bytes by building MinIO URLs by hand, which a Remote asset already breaks.

Nothing is deployed yet, so there are no converted COGs to carry forward.

## Decision

**Every value GeoRiva holds is in its Variable's source unit. Every value that leaves is converted to the unit asked
for — the Variable's served unit unless the reader names another.**

### The rule

1. **Bytes are in `source_unit`, for every kind of asset.** Ingested, Remote, Store, materialized and derived alike.
   Ingestion and Promotion stop converting units. They still turn a source file into a COG (float32, NaN as nodata,
   clipped, ADR 0038) and still apply vector transforms (magnitude, direction): those are not unit conversions, and
   a magnitude in `source_unit` converts like any other value.

2. **`unit` is the served unit.** It is what every reader returns by default, and it may be changed at any time.
   Changing it rewrites nothing.

3. **Every number stored about values is in `source_unit`.** Asset statistics (`stats_*`), zonal statistics
   (`BoundaryZonalStats`), `value_min` / `value_max` and style stops. A stop at 0 °C is stored as 273.15 K and keeps
   meaning the same physical threshold whatever `unit` becomes. Forms convert at their boundary: an operator types
   and reads the served unit. A ramp is applied (ADR 0022 §3) in the served unit, since `scale_type` spaces stops in
   what the operator sees, and the stops it yields are stored converted.

### The conversion

4. **Only positive affine conversions.** `Variable.clean` computes, with pint, `offset = f(0)` and
   `scale = f(1) − f(0)`, and checks `f(2) = offset + 2·scale`. A conversion that fails the check (logarithmic units:
   dBZ), one pint cannot make (a rate to an amount), or one with `scale ≤ 0` is refused. A positive scale keeps every
   stored minimum a minimum and every ordering an ordering, which rules 14 and 15 depend on.

5. **The Variable stores its `scale` and `offset`**, recomputed on save from `source_unit → unit`. They are how
   readers outside Django apply the conversion — Martin's SQL, tile-config's clients — with arithmetic and no unit
   library. pint lives in Django only.

6. **Water equivalence is a flag on the Variable.** `kg m-2 ↔ mm` holds only for water, and `kg m-2` is also a column
   load of dust or ozone. The `precipitation` context in `core/unit_utils.py` stays opt-in; a Variable flagged as
   water enables it when its scale and offset are computed. A wizard proposes the flag from the source's own name
   or standard name (precipitation, snowfall or runoff water equivalent, column water) and the operator confirms it.
   Unflagged, `kg m-2 → mm` is refused.

7. **A difference is declared as one.** A Variable whose values are differences — an anomaly, a range — is declared
   in a delta unit (`delta_degC`), so it is scaled and never offset. Statistics that are differences are converted
   so whatever the Variable: `std` by `scale` alone.

### The source unit

8. **`source_unit` is required, and locked once the Variable has an asset.** Every path that creates a Variable sets
   it: the upload wizard, push-feed setup, a plugin's `collection_definitions`, the Harvest Feed Wizard and a recipe's
   output. It states what bytes that already exist hold, so the Variable form shows it read-only once there are any.
   A wrong label is corrected by an explicit **Correct source unit** action, which says what follows — every value
   of the Variable, its range, stops and stored statistics are reinterpreted — converts nothing, and is recorded
   with who made it and when.

9. **A harvested Variable's source unit is the Upstream's.** The Harvest Feed Wizard lifts it from the Upstream's own
   metadata (a store array's `units`, `raster:bands[].unit`) and shows it read-only, asking the operator for the served
   unit only. An Upstream that declares none is asked for both, the source unit worded as describing the Upstream's
   bytes. The value range a sample read estimates is in the source unit, like every other stored number.

### Reading values

10. **One read module in core, by asset, wherever its bytes are.** A local COG (a windowed read from our bucket), a
    Remote COG (a windowed range read upstream, by the allowlisted href) and a Store asset (the store opened with the
    feed's Pinned locations only, ADR 0044 §11; the field decoded; the window cut) are read by the same calls:
    a window, a point or area over time, a whole field. Each returns north-up float32 with NaN for nodata — the
    contract `processing/raster_io.read_north_up` holds today — in a stated unit. It sits beside
    `core/storage/asset_location.py`, which still answers *where*; this answers *what values*.

11. **The served unit by default; `in_unit` on request.** A reader that names `in_unit` gets `source_unit → in_unit`
    in one conversion, refused up front if rule 4 refuses it. A program with a contract of its own names its unit
    and is unaffected by what anybody chooses to look at.

12. **Reads happen in a session.** `with values.session(in_unit=…) as s:` opens each store once, pins one snapshot of
    each for the session's life, reuses decoded fields inside it, and closes everything at the end. A batch — a
    publish, a recipe's production unit, a timeseries request — is one session, and reads one moment of a store that
    commits every few seconds. One-call helpers are one-call sessions. A cache across sessions, should timeseries
    need one, goes behind this interface, not in front of it.

13. **A recipe declares the unit of each input.** Every `InputRef` carries an `in_unit`, and the engine opens one
    session per production unit and reads each input in it. A recipe declaring none is refused at registration. Its
    output Variable's `source_unit` is the recipe's declared output unit. A recipe's output is stored bytes under a
    fixed label, so the unit it read in is written down, never inferred from a setting someone may change.

### Titiler

14. **Tiles and textures are drawn in the source unit.** Rendering is a linear rescale over `vmin`/`vmax` into a
    colormap built from absolute stops. Raw pixels drawn with raw stops and a raw range are the same colours as
    converted pixels drawn with converted ones, for any positive affine conversion; so are the bytes of ADR 0021's
    encoded texture. Titiler is handed raw ranges and stops and converts no pixel. A test renders one tile with a
    Variable served in K and in °C and compares the PNGs.

15. **Only numbers that leave as numbers are converted.** A point read and GetFeatureInfo apply `scale` and `offset`
    and name the served unit. Tile-config gives a client the served `vmin`/`vmax` beside the texture, so WeatherLayers
    unscales straight into the served unit, and the served labels and unit symbol for legends.

16. **Decoded fields are cached raw.** A unit change does not invalidate a worker's cache, and a store is not reopened
    because of one. The render-config version token (ADR 0021) changes with `unit`, as it does with the range.

17. **Martin converts in its function.** `boundary_stats` already joins the Variable; it serves
    `mean·scale + offset`, `min·scale + offset`, `max·scale + offset`, `sum·scale + offset·count` and `std·scale`.

### What a direct reader sees

18. **Raw bytes, described.** Whoever opens an href — a STAC client, a virtual-Zarr consumer — reads the source unit.
    STAC states it with `raster:bands[]`'s `scale`, `offset` and `unit` (the served unit), and virtual Zarr with CF's
    `scale_factor`, `add_offset` and `units`: both generated when read, so both follow `unit` without a rewrite. No
    scale or offset is written into a COG's header, which could not follow a later change.

### Enforcement

19. **Reading values any other way fails CI.** A core test fails on `rasterio.open`, `xarray.open_*` or a bucket's
    `read_bytes` outside an allowlist of modules that handle bytes, not values: ingestion's writer and the format
    plugins, the materializer, virtual Zarr's layout reader, the Upstream survey and the read module itself. A new
    entry is a decision about that module, made in review.

20. **Plugins carry the same rule, in their own repositories.** `source-plugin-boilerplate` ships it as a ruff
    `banned-api` rule, and an existing plugin adds it to its own `ruff.toml`. Core knows no plugin, and no plugin is
    checked by core's CI. A plugin that publishes to an outside contract passes `in_unit` on every read.

## Consequences

- `unit` becomes a display choice that costs nothing to change, for every Variable, harvested or not.
- A materialized copy needs nothing special: its bytes are in the source unit, like every asset.
- Every reader of values is a converter or calls one. The Forti publisher and the CHIRPS plugin's recipe reader move
  onto the read module and require the core version that has it. `read_north_up` becomes a caller of it.
- A direct consumer that ignores `raster:bands` scale and offset, or CF attributes, sees Kelvin where the map shows
  °C. Before, an ingested COG held the served unit; now none does.
- The conversion is applied in three places, each by arithmetic on the Variable's `scale` and `offset`: the read
  module, tile-config's point answers, and Martin's SQL. Changing what `scale` and `offset` mean changes all three.
- A style made in one served unit and viewed in another keeps its physical thresholds and loses its round numbers:
  stops at 0, 10, 20 °C read as 32, 50, 68 °F. Re-applying a ramp is how an operator gets round numbers back.
- The Correct source unit action is the one way to change what a Variable's bytes are said to hold. It leaves
  every stored range, stop and statistic meaning a different physical value, and says so.
- A recipe written for a later version declares units it did not have to before.

## Out of scope

- Non-affine conversions: logarithmic units, and anything that integrates or differentiates over time
  (de-accumulation stays a reader's own work, as Forti's `accumulation_window` does).
- Negative scales.
- A cache of decoded fields across reading sessions.
- What the Forti publisher does with a since-start accumulation that is not held at a run's first step.

## Alternatives considered

- **Convert on write, as before; convert on read only for Remote and Store assets.** A materialized copy would be a
  third case — local and unconverted — that Titiler cannot tell apart without consulting a row per tile. Or
  materializing converts, and #725's byte-identical copy goes.
- **Store the served unit's numbers and rewrite them when `unit` changes.** Every asset statistic, zonal-statistics
  row and style at once, half old and half new until it finishes.
- **Machine-computed numbers in the source unit, human-written ones (range, stops) in the served unit.** Two rules,
  and every reader must know which number follows which.
- **A stored unit separate from `source_unit`**, to mark COGs that were already converted. There are none.
- **The served unit only; no `in_unit`.** A consumer with an outside contract would follow a display setting, and a
  recipe would write an archive in whatever unit was chosen that week.
- **Return raw values and a conversion; the caller applies it.** Every caller can forget, silently.
- **Convert every pixel in Titiler.** A pass over every tile, ranges and stops converted back to draw with, and a
  decoded-field cache that a unit change invalidates — for the same PNG.
- **Convert in the browser.** The conversion in every client.
- **Always enable water equivalence.** An aerosol load shown in millimetres without complaint.
- **Convention only, no CI guard.** The failure is a wrong number with a right-looking label, and nothing errors.
