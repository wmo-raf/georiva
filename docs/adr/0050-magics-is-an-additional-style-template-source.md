# Magics is an additional Style template source

## Status

accepted — see wmo-raf/georiva#811 (parent #798). Adds a second shipped catalogue beside the earthkit-plots one
(#784, #789). Nothing about that catalogue changes.

## Context

GeoRiva ships Style templates converted from earthkit-plots' styles. earthkit-plots does not author those styles: it
generates them from the named contour styles of ECMWF's **Magics** plotting library (`share/magics/styles/ecmwf/`),
and the translation drops or reshapes detail. Its precipitation style ends at 100 mm with an "extend above" end, where
Magics' `sh_blured_f05t300lst` lists levels up to 250 mm. Magics is Apache-2.0, like earthkit-plots.

Magics holds two kinds of file. `styles.json` names 576 styles by their flat `mcont` parameters (`contour_level_list`,
`contour_shade_colour_list`, ...). About 200 rule files say which fields a style is for (GRIB `shortName`, NetCDF
`long_name` ...) and, often, the unit Magics converts the field to first (`prefered_units`).

## Decision

**GeoRiva also ships the shaded Magics styles as Style templates (`origin` = `magics`), read from Magics at a pinned
commit, translated as Magics draws them.**

1. **Pinned.** Magics `develop` at `cb97794b8830e9fdcd2f40e659d8070d74bf60af`. The generator
   `georiva/scripts/magics_styles/generate.py` downloads that commit, or reads a checkout, and writes
   `core/style_catalogue/magics-cb97794b.json`. That file is committed, and migration `0032` loads it with
   `load_catalogue`, upserting by slug, as `0026` loads earthkit-plots. Tests and migrations never touch the network.
2. **Reading is separate from converting.** `read_magics` fills a `MagicsSource` (styles, rules, colour names);
   `convert` turns any such source into templates. A reader of earthkit-plots' copy of the same styles could feed the
   same conversion, so the source can be switched without touching the translation.
3. **Shaded styles only.** `sh_` styles; the `ct_` contour-line styles are out of scope, since GeoRiva colours rasters.
4. **Units come from the rules.** A style becomes one template per distinct unit among the rule files that list it:
   their `prefered_units`, or the unit the rule's parameter is plotted in (`catalogue.yml`). A style no rule lists, or
   whose every rule's unit does not resolve, is skipped and reported. Template units are GeoRiva `Unit`s, validated
   by the unit registry, and the picker's unit check (#809) applies to them as to any template.
5. **Translated as Magics draws, read from its source.** Levels exactly as listed (an `interval` style: the levels
   `IntervalSelectionType` computes), clipped by `contour_shade_min_level` / `max_level`. n levels take the first
   n − 1 colours of the list, repeating the last if it is short (`ColourTableDefinitionCompute`, policy `lastone`).
   Colour names are Magics' own table, parsed from `Colour.cc`. Below the lowest level and above the top one is
   transparent: Magics fills a polygon only between two levels (`PolyShadingMethod::index` is −1 outside them, and
   `PolyShadingMethod::operator()` leaves such a polygon unfilled). The issue asked for the last colour above the top;
   the source says otherwise, and the translation follows the source. So `sh_blured_f05t300lst` is 7 bands from 0.5
   to 250 mm, nothing drawn beyond them: its list's eighth colour,
   charcoal, is never drawn by Magics, and is not here.
6. **Named and grouped as the earthkit-plots templates are.** "Precipitation, 0.5 to 250 mm, 7 bands", grouped by the
   rule's parameter into the same `Quantity` groups. `source_name` is the Magics style name, shown in the picker as a
   small source label ("Magics · sh_blured_f05t300lst") and searchable in the Style templates listing. The curve
   comes from the level spacing (even: linear; roughly multiplying: log; otherwise widening: sqrt);
   `corrections.yml` overrides a style's curve or group.
7. **Both catalogues stay.** Neither supersedes or hides the other; the picker lists both, filtered by unit as usual.

## Consequences

- The catalogue grows by about 120 read-only templates; the Style templates listing pages them.
- A Magics style that cannot be translated faithfully is left out, not approximated: colours Magics calculates rather
  than lists, levels by count, a level or colour Magics itself cannot read (it would draw red).
- Refreshing is a new pin in `catalogue.yml`, a regenerated JSON file beside the old one and a migration that loads
  it, as for earthkit-plots.
- The two generators share `georiva/scripts/style_sources.py`: fetching a pinned GitHub commit, the catalogue's
  envelope and source block, the command line and the `Skip` a style is left out with. Only reading and translating
  an upstream differ.
- A further source is one `StyleTemplate.Origin` entry (its operator-facing label), its catalogue file and the
  migration that loads it. `origin` stays a plain `CharField`, not one with choices: only `load_catalogue` writes it.
- `NOTICE` carries the Magics attribution (Apache-2.0).
