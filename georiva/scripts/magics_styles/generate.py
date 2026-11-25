# /// script
# requires-python = ">=3.12,<3.14"
# dependencies = ["pyyaml==6.0.2"]
# ///
"""Generate GeoRiva's Magics Style template catalogue (#811).

    uv run georiva/scripts/magics_styles/generate.py                  # fetch the pinned commit
    uv run georiva/scripts/magics_styles/generate.py --source DIR     # a Magics checkout

Reads ECMWF's Magics at the commit pinned in ``catalogue.yml`` beside this file
and writes the vendored catalogue the data migration loads
(``georiva/src/georiva/core/style_catalogue/magics-<commit>.json``). A dev-only
tool: Magics is only read (JSON files, and its colour-name table parsed out of
``Colour.cc``), never built or imported, and is never a GeoRiva dependency.

Reading and converting are separate: :func:`read_magics` turns a checkout into
a :class:`MagicsSource` (named styles, the rule files that use them, colour
names), and :func:`convert` turns a source into templates. Another reader that
fills a ``MagicsSource`` — earthkit-plots' copy of the same styles, say — can
feed the same conversion.

What is taken
-------------

Shaded (``sh_``) styles only: GeoRiva colours a raster and draws no isolines,
so the contour-line ``ct_`` styles have nothing to give it.

A style becomes one template per distinct unit among the rule files that list
it. A rule's unit is its ``prefered_units`` (through ``unit_strings``), or,
when it names none, the unit its parameter is plotted in (``parameters``). The
parameter is the first of the rule's GRIB ``shortName``s that ``parameters``
knows, and it gives the template its name and group (from the first rule in
``prefer``, else in file order); ``rules`` overrides any of
that for one rule file (an ensemble spread is a difference, not the field).

How a style is translated — as Magics draws it
----------------------------------------------

* **Levels**, exactly: a ``level_list`` as listed; an ``interval`` style the
  levels Magics computes (its min and max levels, and every multiple of the
  interval from the reference level strictly between). Levels outside
  ``contour_shade_min_level`` / ``contour_shade_max_level`` are dropped, as
  ``LevelListSelectionType::calculate`` drops them.
* **Colours**: n levels are coloured with the first n - 1 colours of the list,
  repeating the last when the list is short (``ColourTableDefinitionCompute``,
  policy ``lastone``). A colour is a Magics name, ``rgb()``/``rgba()`` (0–1, or
  0–255), ``hsl()``/``hsla()`` or ``#rrggbb``, written ``#rrggbb`` (or
  ``#rrggbbaa`` when not opaque); ``none`` is a clear band.
* **Out of range**: transparent at both ends. Magics fills a polygon only
  between two levels (``PolyShadingMethod::index`` is -1 outside them, and
  ``operator()`` leaves such a polygon unfilled), so nothing is drawn below the
  lowest level or above the top one.
* **Curve**, from the level spacing: even gives linear, roughly multiplying
  gives log, otherwise widening gives sqrt. ``corrections.yml`` overrides the
  curve or the group of any style.

Skipped, and reported with why: a style no rule file lists; one whose every
rule's unit does not resolve; one that cannot be translated faithfully
(calculated colours, levels by count, a level that is not a number, a colour
name Magics does not know and draws red instead); one drawn exactly as an
earlier style is.
"""

import argparse
import colorsys
import io
import json
import math
import re
import sys
import tarfile
import tempfile
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent
CONFIG = HERE / "catalogue.yml"
CORRECTIONS = HERE / "corrections.yml"
OUTPUT_DIR = HERE.parents[1] / "src" / "georiva" / "core" / "style_catalogue"

COPYRIGHT = "Copyright 1996- European Centre for Medium-Range Weather Forecasts (ECMWF)"
MODIFIED = (
    "Converted from Magics' named contour styles and modified by GeoRiva: shaded styles only, translated to "
    "levels and one colour per band as Magics draws them, units and groups taken from Magics' rule files, renamed."
)

#: Why a style was skipped, by kind.
UNUSED, UNIT, UNTRANSLATABLE, DUPLICATE = "unused", "unit", "untranslatable", "duplicate"


class Skip(Exception):
    """A style (or one rule's use of it) that cannot be taken; ``str()`` is why."""


@dataclass
class MagicsSource:
    #: Style name -> its flat ``mcont`` parameters (``styles.json``).
    styles: dict
    #: The rules, in file order: dicts of ``file``, ``styles``, ``short_names``, ``prefered_units``.
    rules: list
    #: Colour name -> ``(r, g, b, a)`` in 0–1, or ``None`` for a clear one (``none``).
    colours: dict = field(default_factory=dict)


# ---------------------------------------------------------------- reading


def _listed(value):
    if value is None:
        return []
    return [value] if isinstance(value, str) else list(value)


def read_magics(root, styles_path="share/magics/styles/ecmwf", colours_path="src/common/Colour.cc"):
    """A :class:`MagicsSource` from a Magics checkout."""
    root = Path(root)
    directory = root / styles_path
    styles = json.loads((directory / "styles.json").read_text())
    rules = []
    for path in sorted(directory.glob("*.json")):
        if path.name == "styles.json":
            continue
        for rule in json.loads(path.read_text()):
            short_names = []
            for match in rule.get("match", []):
                for name in _listed(match.get("shortName")):
                    if name not in short_names:
                        short_names.append(name)
            rules.append(
                {
                    "file": path.name,
                    "styles": _listed(rule.get("styles")),
                    "short_names": short_names,
                    "prefered_units": rule.get("prefered_units"),
                }
            )
    return MagicsSource(styles=styles, rules=rules, colours=read_colour_names((root / colours_path).read_text()))


_COLOUR_LINE = re.compile(r'colours_\["([^"]+)"\]\s*=\s*Rgb\(([^)]*)\)')


def read_colour_names(source):
    """Magics' colour names from ``Colour.cc``: the table ``Colour::init`` fills,
    up to the ``#if 0`` that disables the CSS names after it."""
    colours = {}
    for line in source.split("#if 0")[0].splitlines():
        match = _COLOUR_LINE.search(line)
        if not match:
            continue
        parts = [float(p) for p in match.group(2).split(",")]
        if any(p < 0 for p in parts):  # none, undefined: Rgb(-1, -1, -1)
            colours[match.group(1)] = None
        else:
            colours[match.group(1)] = (*parts[:3], parts[3] if len(parts) > 3 else 1.0)
    return colours


# ---------------------------------------------------------------- colours


def _hex(r, g, b, a=1.0):
    channels = [round(min(max(c, 0.0), 1.0) * 255) for c in (r, g, b)]
    text = "#" + "".join(f"{c:02x}" for c in channels)
    if a < 1.0:
        text += f"{round(a * 255):02x}"
    return text


def _numbers(text):
    # Magics reads the numbers off a stream: a missing ")" is no matter to it.
    inside = text[text.index("(") + 1 :].rstrip(")")
    return [float(p) for p in inside.split(",")]


def resolve_colour(text, names):
    """A Magics colour as ``#rrggbb`` or ``#rrggbbaa``."""
    colour = text.strip().lower()
    if colour in names:
        rgba = names[colour]
        return "#00000000" if rgba is None else _hex(*rgba)
    try:
        if colour.startswith("rgba(") or colour.startswith("rgb("):
            values = _numbers(colour)
            rgb, alpha = values[:3], values[3] if len(values) > 3 else 1.0
            if any(v > 1 for v in rgb):  # 0–255, as Magics reads any component over 1
                rgb = [v / 255 for v in rgb]
            return _hex(*rgb, alpha)
        if colour.startswith("hsla(") or colour.startswith("hsl("):
            values = _numbers(colour)
            hue, saturation, light = values[:3]
            alpha = values[3] if len(values) > 3 else 1.0
            r, g, b = colorsys.hls_to_rgb(hue / 360, light, saturation)
            return _hex(r, g, b, alpha)
        if re.fullmatch(r"#[0-9a-f]{6}", colour):
            return colour
        if re.fullmatch(r"#[0-9a-f]{3}", colour):
            return "#" + "".join(c * 2 for c in colour[1:])
    except (ValueError, IndexError) as exc:
        raise Skip(f"colour {text!r} cannot be read") from exc
    raise Skip(f"colour {text!r} is not a Magics colour: Magics draws it red")


# ---------------------------------------------------------------- levels

_UNSET = 1.0e21


def _number(style, key, default=None):
    value = style.get(key)
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise Skip(f"{key} {value!r} is not a number") from exc


def _levels(style):
    kind = style.get("contour_level_selection_type", "count")
    shade_min = _number(style, "contour_shade_min_level", -_UNSET)
    shade_max = _number(style, "contour_shade_max_level", _UNSET)
    lowest = max(shade_min, _number(style, "contour_min_level", -_UNSET))
    highest = min(shade_max, _number(style, "contour_max_level", _UNSET))
    if kind in ("level_list", "list"):  # its XML and Fortran spellings
        raw = style.get("contour_level_list")
        if raw is None:
            raise Skip("a level list style with no level list")
        values = raw if isinstance(raw, list) else str(raw).split("/")
        try:
            levels = [float(v) for v in values if str(v).strip() != ""]
        except ValueError as exc:
            raise Skip(f"level list {raw!r} has a level that is not a number") from exc
        return [v for v in levels if lowest <= v <= highest]
    if kind == "interval":
        # IntervalSelectionType::calculate, for a style that bounds its levels.
        if abs(lowest) >= _UNSET or abs(highest) >= _UNSET:
            raise Skip("interval levels with no minimum or maximum are drawn from the data")
        interval = _number(style, "contour_interval", 8.0)
        reference = _number(style, "contour_reference_level", 0.0)
        levels = {lowest, highest}
        steps = math.ceil(abs(highest - reference) / interval) + math.ceil(abs(reference - lowest) / interval) + 2
        for i in range(-steps, steps + 1):
            level = reference + i * interval
            if lowest < level < highest and not math.isclose(level, lowest) and not math.isclose(level, highest):
                levels.add(round(level, 10))
        return sorted(levels)
    raise Skip(f"levels by {kind!r}: Magics draws them from the data")


def _colours(style, bands, names):
    method = style.get("contour_shade_colour_method", "calculate")
    if method != "list":
        raise Skip(f"colours by {method!r}, not listed")
    raw = style.get("contour_shade_colour_list")
    listed = [c for c in (raw if isinstance(raw, list) else str(raw or "").split("/")) if c.strip()]
    if not listed:
        raise Skip("no colour list")
    if str(style.get("contour_shade_colour_reverse_list", "off")).lower() == "on":
        listed.reverse()
    # ColourTableDefinitionCompute::set, policy lastone.
    chosen = [listed[min(i, len(listed) - 1)] for i in range(bands)]
    return [resolve_colour(c, names) for c in chosen]


def scale_type(levels):
    """The encoding curve the level spacing suggests."""
    gaps = [b - a for a, b in zip(levels, levels[1:], strict=False)]
    if max(gaps) <= 1.5 * min(gaps):
        return "linear"
    if levels[0] > 0:
        ratios = [b / a for a, b in zip(levels, levels[1:], strict=False)]
        if min(ratios) >= 1.4 and max(ratios) <= 3 * min(ratios):
            return "log"
    if gaps[-1] > gaps[0]:
        return "sqrt"
    return "linear"


def translate(style, names):
    """A style's levels, colours and out-of-range settings, as Magics draws them."""
    if str(style.get("contour_shade", "off")).lower() != "on":
        raise Skip("not shaded")
    levels = _levels(style)
    if len(levels) < 3:
        raise Skip(f"{len(levels)} levels: a template needs at least two bands")
    if any(b <= a for a, b in zip(levels, levels[1:], strict=False)):
        raise Skip("levels are not strictly ascending")
    colours = _colours(style, len(levels) - 1, names)
    return {
        "levels": [int(v) if float(v).is_integer() else v for v in levels],
        "colors": colours,
        "under": "transparent",
        "over": "transparent",
    }


# ---------------------------------------------------------------- converting


def _rule_use(rule, config):
    """``(unit symbol, label, quantity)`` for a rule, or :class:`Skip`."""
    override = (config.get("rules") or {}).get(rule["file"], {})
    parameters = config.get("parameters") or {}
    parameter = next((parameters[n] for n in rule["short_names"] if n in parameters), None)
    if parameter is None and "label" not in override:
        known = ", ".join(rule["short_names"]) or "no shortName"
        raise Skip(f"{rule['file']}: no parameter GeoRiva knows ({known})")
    merged = {**(parameter or {}), **override}
    if "unit" in override:
        unit = override["unit"]
    elif rule.get("prefered_units"):
        unit = (config.get("unit_strings") or {}).get(rule["prefered_units"])
        if unit is None:
            raise Skip(f"{rule['file']}: Magics unit {rule['prefered_units']!r} has no GeoRiva unit")
    else:
        unit = merged.get("unit")
        if unit is None:
            raise Skip(f"{rule['file']}: names no unit, and {merged['label']} has none in catalogue.yml")
    if unit not in {u["symbol"] for u in config["units"]}:
        raise Skip(f"{rule['file']}: unit {unit!r} is not among the catalogue's units")
    return unit, merged["label"], merged["quantity"]


def _span(value):
    return f"{value:g}".replace("-", "−")


def _name(label, levels, unit, bands):
    shown = f" {unit}" if unit else ""
    return f"{label}, {_span(levels[0])} to {_span(levels[-1])}{shown}, {bands} bands"


def _slug(name, unit_slug):
    return "magics-" + re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") + f"-{unit_slug}"


def _colour_words(style):
    match = re.search(r"Colou?r\s*:\s*(.+?)(?:\s+Used for|\s+Thickness|$)", style.get("contour_description", ""))
    return match.group(1).strip().rstrip(".").lower() if match else ""


def convert(source, config, corrections):
    """Templates from ``source``'s shaded styles, and the styles skipped.

    Returns ``(templates, skipped)``; each skip is a dict of ``source_name``,
    ``kind`` (``unused``, ``unit``, ``untranslatable`` or ``duplicate``) and
    ``reason``. A style drawn exactly as an earlier one is, in the same unit,
    is a duplicate (Magics styles that differ only in how the shading is
    rendered, cells or polygons, are one template here).
    """
    units = {u["symbol"]: u for u in config["units"]}
    # The rule that names a template: the first in `prefer`, then file order.
    prefer = {file: i for i, file in enumerate(config.get("prefer") or [])}
    uses = {}
    for rule in sorted(source.rules, key=lambda r: prefer.get(r["file"], len(prefer))):
        for name in rule["styles"]:
            uses.setdefault(name, []).append(rule)

    templates, skipped, drawn_already = [], [], {}
    for name in sorted(n for n in source.styles if n.startswith("sh_")):
        style = source.styles[name]
        if name not in uses:
            skipped.append({"source_name": name, "kind": UNUSED, "reason": "no rule file lists it"})
            continue
        by_unit, unit_reasons = {}, []
        for rule in uses[name]:
            try:
                unit, label, quantity = _rule_use(rule, config)
            except Skip as reason:
                unit_reasons.append(str(reason))
                continue
            by_unit.setdefault(unit, (label, quantity))
        if not by_unit:
            skipped.append({"source_name": name, "kind": UNIT, "reason": "; ".join(unit_reasons)})
            continue
        try:
            drawn = translate(style, source.colours)
        except Skip as reason:
            skipped.append({"source_name": name, "kind": UNTRANSLATABLE, "reason": str(reason)})
            continue
        correction = corrections.get(name, {})
        for unit, (label, quantity) in by_unit.items():
            same = drawn_already.get((unit, json.dumps(drawn)))
            if same:
                skipped.append({"source_name": name, "kind": DUPLICATE, "reason": f"in {unit}, drawn as {same} is"})
                continue
            drawn_already[(unit, json.dumps(drawn))] = name
            shown = units[unit].get("shown", unit)
            templates.append(
                {
                    "slug": _slug(name, units[unit]["slug"]),
                    "name": correction.get("name") or _name(label, drawn["levels"], shown, len(drawn["colors"])),
                    "quantity": correction.get("quantity", quantity),
                    **drawn,
                    "unit": unit,
                    "scale_type": correction.get("scale_type") or scale_type(drawn["levels"]),
                    "source_name": name,
                    "_colour_words": _colour_words(style),
                }
            )
    _distinguish(templates)
    return templates, skipped


def _distinguish(templates):
    """Two templates of one name are told apart by their colours, as the style's
    description words them, and failing that by the Magics style name."""
    by_name = {}
    for template in templates:
        by_name.setdefault(template["name"], []).append(template)
    for same in by_name.values():
        if len(same) > 1:
            words = [t["_colour_words"] for t in same]
            distinct = all(words) and len(set(words)) == len(words)
            for template in same:
                template["name"] += f", {template['_colour_words']}" if distinct else f" ({template['source_name']})"
    for template in templates:
        del template["_colour_words"]


def build(source, config, corrections):
    """The catalogue JSON, and the skips."""
    templates, skipped = convert(source, config, corrections)
    used = {t["unit"] for t in templates}
    pin = config["source"]
    catalogue = {
        "source": {
            "name": pin["name"],
            "repository": pin["repository"],
            "tag": pin["commit"][:8],
            "commit": pin["commit"],
            "license": "Apache-2.0",
            "copyright": COPYRIGHT,
            "modified": MODIFIED,
        },
        "generator": "georiva/scripts/magics_styles/generate.py",
        "units": [
            {k: v for k, v in unit.items() if k not in ("slug", "shown")}
            for unit in config["units"]
            if unit["symbol"] in used
        ],
        "templates": templates,
    }
    return catalogue, skipped


def fetch(pin, into):
    """Download and unpack the pinned commit of Magics; return its root."""
    url = f"https://codeload.github.com/ecmwf/magics/tar.gz/{pin['commit']}"
    with urllib.request.urlopen(url, timeout=120) as response:  # noqa: S310 - a fixed https URL
        data = response.read()
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        archive.extractall(into, filter="data")
    (root,) = Path(into).iterdir()
    return root


def main():
    import yaml

    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--source", type=Path, help="a Magics checkout at the pinned commit (default: fetch it)")
    parser.add_argument("--output", type=Path, help="where to write the catalogue JSON")
    args = parser.parse_args()

    config = yaml.safe_load(CONFIG.read_text())
    corrections = yaml.safe_load(CORRECTIONS.read_text()) or {}
    pin = config["source"]
    output = args.output or OUTPUT_DIR / f"{pin['name']}-{pin['commit'][:8]}.json"
    if args.source:
        source = read_magics(args.source, pin["styles_path"], pin["colours_path"])
    else:
        with tempfile.TemporaryDirectory() as tmp:
            source = read_magics(fetch(pin, tmp), pin["styles_path"], pin["colours_path"])
    catalogue, skipped = build(source, config, corrections)

    output.write_text(json.dumps(catalogue, indent=1, ensure_ascii=False) + "\n")
    shaded = sum(1 for n in source.styles if n.startswith("sh_"))
    print(f"Read {len(source.styles)} styles, {shaded} shaded, from {len(source.rules)} rules")
    print(f"Wrote {len(catalogue['templates'])} templates to {output}")
    for template in catalogue["templates"]:
        print(f"  {template['quantity']:<13} {template['name']:<60} <- {template['source_name']}")
    print(f"Skipped {len(skipped)} shaded styles:")
    for kind in (UNUSED, UNIT, UNTRANSLATABLE, DUPLICATE):
        these = [s for s in skipped if s["kind"] == kind]
        print(f"  {kind}: {len(these)}")
        if kind != UNUSED:
            for skip in these:
                print(f"    {skip['source_name']}: {skip['reason']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
