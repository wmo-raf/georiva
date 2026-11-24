# A Variable's scale type is its encoding curve

## Status

accepted — see wmo-raf/georiva#787 (spec #784). Amends [ADR-0021](0021-on-demand-encoded-textures.md) (what an
encoded texture's pixel is) and [ADR-0022](0022-two-layer-styling-ramps-and-variable-styles.md) (the #382
consequence that a log-declared Variable renders linearly). [ADR-0046](0046-values-are-stored-as-produced-and-converted-when-read.md)
§14 is untouched: tiles are still drawn in the source unit.

## Context

Values reach a map as one of 256 codes, encoded over the Variable's `value_min`…`value_max`. Every renderer encoded
linearly. `scale_type` (`linear` / `sqrt` / `log` / `diverging`) only spaced stops when a ramp was applied, and
nothing read it when drawing; ADR 0022 recorded that as a consequence.

A linear encoding starves uneven bands. Over 0–200 mm one code is about 0.8 mm, so precipitation bands at 0.5, 1 and
2 mm get one code or none, and a style template's low levels (#784) cannot be seen.

## Decision

**A Variable's `scale_type` chooses the curve its values are encoded through, everywhere values become codes.**

1. **The curve.** With `t = (v − vmin) / (vmax − vmin)` clipped to 0–1, the code is `int(255 · f(t))`:
   - `linear`: `f(t) = t`.
   - `sqrt`: `f(t) = √t`.
   - `log`: `f(t) = log(v / vmin) / log(vmax / vmin)`, which needs `vmin > 0`.
   - `diverging` encodes linearly, so a centred range stays symmetric.

   Every number is in the source unit, the unit tiles are drawn in.

2. **Resolved once, in Django.** `core/machine_plane/colour_table.encoding_curve(variable)` turns `scale_type` into
   the curve, and Titiler's palette payload carries it as `curve`. Titiler never reads `scale_type` itself. A payload
   without `curve`, written before this, encodes linearly.

3. **Where it is applied.** Titiler's tile rescale (tiles and WMTS GetTile go through it) and the encoded texture
   (`app/curve.py`). Core's `colour_table.encode` is the same function, value by value, and tests on both sides
   pin it to the same codes. The vector texture (#593) stays linear over its shared range: its two bands must
   decode to a direction.

4. **Where it is inverted.** The decode is the curve's exact inverse. The colour table is built by decoding each code
   through it and interpolating the stops in value space, so a code's colour is the colour of the value it stands
   for. The browser's hover readout decodes a code through it too.

5. **The texture contract.** The texture stays an 8-bit PNG. `X-Image-Unscale` keeps carrying the served range, and a
   new `X-Image-Curve` header names the curve (`linear`, `sqrt` or `log`). A code's fraction of the range is the same
   in the stored and the served unit (the conversion is positive and affine), except that a `log` code's fraction
   depends on the stored range itself, `(r^f − 1) / (r − 1)` with `r = vmax / vmin`. So the render contract, EDR's
   `x-georiva` and the STAC visual asset carry `encoding`: `{"curve", "range"}`, the range stored. A client finds
   the fraction over `encoding` and reads that fraction of the served range. EDR's `scale_type` is meaningful and is
   documented as the curve declared.

6. **A curve change is a new URL.** `scale_type` was already part of the render-config version token; it stays
   there, so a texture or tile encoded with the old curve is never served from cache.

7. **`log` needs a minimum above zero.** The range form refuses `log` unless the stored minimum is above zero
   (`Variable.validate_scale_type`, also run by `Variable.clean`). It is the stored minimum that counts: −10 °C is
   263.15 K. A Variable declared `log` over such a range before this rule encodes linearly.

8. **Existing Variables.** A Variable already declared `sqrt` or `log` starts rendering with its curve. No migration,
   no report. A curve change leaves every Variable style's stops alone (ADR 0022's snapshot semantics).

## Consequences

- `sqrt` and `log` Variables look different after this ships, with more contrast at the low end. This is intended.
- ADR 0022's #382 consequence, "a log-declared variable renders linearly on both sides", no longer holds: both sides
  now encode and decode through the curve.
- A STAC Render extension entry still says `rescale` + `colormap`. A generic Titiler client applying it to a `sqrt`
  or `log` Variable encodes linearly and so colours it with the wrong entries; GeoRiva's own Titiler does not. An
  `expression` carrying the curve is a possible follow-up.
- Additional curves (symlog, power γ) and float textures stay out of scope (#784).
