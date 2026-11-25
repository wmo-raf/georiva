# /// script
# requires-python = ">=3.12,<3.14"
# dependencies = ["pyyaml==6.0.2", "matplotlib==3.10.3"]
# ///
"""Generate GeoRiva's shipped Style template catalogue from earthkit-plots (#789).

    uv run georiva/scripts/earthkit_styles/generate.py                  # fetch the pinned commit
    uv run georiva/scripts/earthkit_styles/generate.py --source DIR     # an earthkit-plots checkout

Reads the curated list in ``catalogue.yml`` beside this file, converts each named
earthkit-plots shaded style into a Style template, and writes the vendored
catalogue that the data migration loads
(``georiva/src/georiva/core/style_catalogue/earthkit-plots-<tag>.json``). What it
could not convert is printed, and nothing is half-converted. A dev-only tool:
earthkit-plots is never imported (its YAML is only parsed, with ``safe_load``),
and neither it nor matplotlib is ever a GeoRiva dependency.

How a style is flattened
------------------------

* **Levels.** A list of boundaries is kept as it is; ``"range(a, b, step)"`` is
  expanded the way earthkit does (integers, ``b`` exclusive). A style with no
  levels is levelled from the data at render time and cannot be flattened.
* **Colours, one per band.** A list with exactly one colour per band is kept as
  it is, the band between ``levels[i]`` and ``levels[i+1]`` taking ``colors[i]``
  — the Magics list convention these styles come from. Any other count (the
  255/256-colour continuous ramps, lists with an extra colour reserved for
  out-of-range values, plain mismatches) is dropped and reported. A matplotlib
  colormap name is sampled: with ``e`` extended ends, the ``n`` bands and the
  ``e`` out-of-range bins share the colormap evenly (bin ``k`` of ``n + e`` takes
  ``cmap(k / (n + e - 1))``), which is what earthkit's ``BoundaryNorm`` draws to
  within rounding; the bands keep their bins' colours. Colours are written
  ``#rrggbb``, or ``#rrggbbaa`` when not opaque (alpha is kept: a band can be
  translucent or clear).
* **Under and over.** earthkit's ``extend`` says which ends are drawn beyond the
  levels: an extended end clamps (it draws in the end band's colour here, where
  earthkit has a colour of its own), an end that is not extended is transparent,
  as a contourf leaves it.
* **Unit.** earthkit's unit string is mapped through ``unit_strings``; the
  curated entry names the unit when earthkit declares none. A string with no
  mapping, or an entry and an earthkit unit that disagree, drops the style.
"""

import sys
from pathlib import Path

import yaml
from matplotlib import colormaps
from matplotlib.colors import to_hex, to_rgba

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import style_sources  # noqa: E402 - scripts/ is not a package
from style_sources import Skip  # noqa: E402

HERE = Path(__file__).resolve().parent
CONFIG = HERE / "catalogue.yml"

COPYRIGHT = "Copyright 2022- European Centre for Medium-Range Weather Forecasts (ECMWF)"
MODIFIED = (
    "Converted from earthkit-plots' YAML styles and modified by GeoRiva: flattened to one colour per band, "
    "out-of-range behaviour taken from 'extend', units mapped, renamed and grouped by quantity."
)


def parse_levels(levels):
    if levels is None:
        raise Skip("no levels: earthkit levels it from the data at render time")
    if isinstance(levels, str):
        text = levels.strip()
        if not (text.startswith("range(") and text.endswith(")")):
            raise Skip(f"levels {levels!r} are neither a list nor range(...)")
        args = [int(part) for part in text[len("range(") : -1].split(",")]
        return [float(v) for v in range(*args)]
    if isinstance(levels, list) and all(isinstance(v, (int, float)) for v in levels):
        return [float(v) for v in levels]
    raise Skip(f"levels {levels!r} are not a list of numbers")


def hex_colour(colour):
    """``#rrggbb``, or ``#rrggbbaa`` when the colour is not opaque."""
    if isinstance(colour, list):
        colour = tuple(colour)
    rgba = to_rgba(colour)
    return to_hex(rgba, keep_alpha=rgba[3] < 1.0).lower()


def flatten_colours(colours, bands, extend):
    if isinstance(colours, str):
        if colours not in colormaps:
            raise Skip(f"colours {colours!r} are one colour, not a colormap or one colour per band")
        cmap = colormaps[colours]
        below = 1 if extend in ("min", "both") else 0
        above = 1 if extend in ("max", "both") else 0
        bins = bands + below + above
        return [hex_colour(cmap(k / (bins - 1))) for k in range(below, below + bands)]
    if not isinstance(colours, list):
        raise Skip(f"colours {colours!r} are neither a list nor a colormap name")
    if len(colours) != bands:
        raise Skip(f"{len(colours)} colours over {bands} bands, not one per band")
    return [hex_colour(c) for c in colours]


def convert(entry, style, unit_strings, units):
    if style.get("type") != "Style":
        raise Skip(f"a {style.get('type')} style, not a shaded one")
    levels = parse_levels(style.get("levels"))
    if not style_sources.strictly_ascending(levels) or len(levels) < 3:
        raise Skip("levels are not strictly ascending boundaries of at least two bands")
    extend = style.get("extend")
    colours = flatten_colours(style.get("colors"), len(levels) - 1, extend)

    earthkit_unit = style.get("units")
    mapped = None
    if earthkit_unit is not None:
        if earthkit_unit not in unit_strings:
            raise Skip(f"earthkit unit {earthkit_unit!r} has no GeoRiva unit")
        mapped = unit_strings[earthkit_unit]
    unit = entry.get("unit", mapped)
    if unit is None:
        raise Skip("earthkit declares no unit and the curated entry names none")
    if mapped is not None and unit != mapped:
        raise Skip(f"the entry says {unit!r} but earthkit says {earthkit_unit!r} ({mapped!r})")
    if unit not in units:
        raise Skip(f"unit {unit!r} is not among the catalogue's units")

    return {
        "slug": entry["slug"],
        "name": entry["name"],
        "quantity": entry["quantity"],
        "levels": [int(v) if v.is_integer() else v for v in levels],
        "colors": colours,
        "unit": unit,
        "under": "clamp" if extend in ("min", "both") else "transparent",
        "over": "clamp" if extend in ("max", "both") else "transparent",
        "scale_type": entry["scale_type"],
        "source_name": style["name"],
        "source_style": f"{entry['file']}/{entry['style']}",
    }


def generate(source_root, config):
    styles_dir = Path(source_root) / config["source"]["styles_path"]
    units = {u["symbol"]: u for u in config["units"]}
    templates, dropped, slugs = [], [], set()
    for entry in config["templates"]:
        if entry["slug"] in slugs:
            raise SystemExit(f"duplicate slug in catalogue.yml: {entry['slug']}")
        slugs.add(entry["slug"])
        where = f"{entry['file']}/{entry['style']}"
        try:
            doc = yaml.safe_load((styles_dir / f"{entry['file']}.yml").read_text())
            style = doc["styles"].get(entry["style"])
            if style is None:
                raise Skip("no such style in the file")
            templates.append(convert(entry, style, config["unit_strings"], units))
        except Skip as reason:
            dropped.append({"source_style": where, "slug": entry["slug"], "reason": str(reason)})
    used = {t["unit"] for t in templates}
    catalogue = style_sources.catalogue(
        config["source"],
        tag=config["source"]["tag"],
        copyright_line=COPYRIGHT,
        modified=MODIFIED,
        generator="georiva/scripts/earthkit_styles/generate.py",
        units=[units[s] for s in units if s in used],
        templates=templates,
    )
    return catalogue, dropped


def report(catalogue, dropped, output):
    print(f"Wrote {len(catalogue['templates'])} templates to {output}")
    for template in catalogue["templates"]:
        print(f"  {template['quantity']:<13} {template['slug']:<40} <- {template['source_style']}")
    print(f"Dropped {len(dropped)}:")
    for drop in dropped:
        print(f"  {drop['source_style']} ({drop['slug']}): {drop['reason']}")


def main():
    return style_sources.run(
        __doc__,
        CONFIG,
        generate=generate,
        output_name=lambda source: f"{source['name']}-{source['tag']}.json",
        report=report,
    )


if __name__ == "__main__":
    sys.exit(main())
