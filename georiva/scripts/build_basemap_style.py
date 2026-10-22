"""Build GeoRiva's one basemap style on OpenFreeMap's tiles.

    python georiva/scripts/build_basemap_style.py

The maps load a single style and switch things on and off by flipping layer
groups with ``setLayoutProperty`` — ``setStyle`` would reload the style and
take every data layer down with it. Each layer carries its group in
``metadata["maplibre:group"]``, declared in ``metadata["maplibre:groups"]``:

    basemap-light / basemap-dark         land, water, roads: the ground itself
    boundaries-light / boundaries-dark   country and first-level borders, and the coastline
    labels-light / labels-dark           place and water names

The basemap layers are Positron's and Dark's own, minus their symbol layers and
their boundary lines. Boundaries and labels are authored here, labels after the
layout Global Forest Watch uses: three tiers each of countries, states and
cities with the sizes and zoom ramps of their style, uppercase spaced state
names, dots under the larger cities, and spaced-out marine names. Not carried
over: POIs, airports, neighbourhoods and road names, which only appear at
street zooms and clutter a data map; and the typeface, because OpenFreeMap's
glyph server has Noto Sans and no Fira Sans.

Layer order is what keeps data between the two: every basemap layer, then an
empty ``georiva-data-anchor`` layer, then the boundaries, then the labels. Data
layers insert before the anchor, so they draw above the ground and below the
borders and names, in either variant.

Everything ships ``visibility: none``. The client turns on the groups it wants
before the map is created (see ``config/static/js/georiva-basemap.js``).

Only the style is ours. Tiles, sprites and glyphs stay on OpenFreeMap, which is
why the output reuses their URLs verbatim. Re-run after OpenFreeMap changes a
style and review the diff.
"""

import json
import sys
import urllib.request
from pathlib import Path

OPENFREEMAP = "https://tiles.openfreemap.org/styles"
UPSTREAM = {"light": "positron", "dark": "dark"}
ANCHOR_ID = "georiva-data-anchor"
OUTPUT = Path(__file__).resolve().parents[1] / "src/georiva/config/static/basemap/georiva-basemap.json"

FONT = ["Noto Sans Regular"]

# What each variant draws with. The labels read as GFW's do: black at 70% with a
# white halo on light, white at 90% with a dark halo on dark. Borders sit over
# the data, so they are translucent and the opposite tone of the basemap.
PALETTE = {
    "light": {
        "text": "hsl(0,0%,0%)",
        "text_opacity": 0.7,
        "halo": "#ffffff",
        "border": "hsl(0,0%,20%)",
    },
    "dark": {
        "text": "hsl(0,0%,100%)",
        "text_opacity": 0.9,
        "halo": "hsla(0,0%,10%,0.75)",
        "border": "hsl(0,0%,100%)",
    },
}

NAME = ["to-string", ["coalesce", ["get", "name_en"], ["get", "name"]]]


def fetch(name):
    # OpenFreeMap turns away urllib's default agent.
    request = urllib.request.Request(f"{OPENFREEMAP}/{name}", headers={"User-Agent": "georiva-basemap-build"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def zoom(*stops, base=None):
    """An interpolate-on-zoom expression from (zoom, value) pairs."""
    kind = ["linear"] if base is None else ["exponential", base]
    return ["interpolate", kind, ["zoom"], *[v for stop in stops for v in stop]]


def place_filter(*classes, rank=None):
    clauses = [["match", ["get", "class"], list(classes), True, False]]
    if rank is not None:
        low, high = rank
        if low is not None:
            clauses.append([">=", ["get", "rank"], low])
        if high is not None:
            clauses.append(["<=", ["get", "rank"], high])
    return ["all", *clauses]


def symbol(layer_id, source_layer, filter_, layout, minzoom=None, maxzoom=None):
    layer = {"id": layer_id, "type": "symbol", "source": "openmaptiles", "source-layer": source_layer}
    if minzoom is not None:
        layer["minzoom"] = minzoom
    if maxzoom is not None:
        layer["maxzoom"] = maxzoom
    layer["filter"] = filter_
    layer["layout"] = {"text-font": FONT, "text-field": NAME, **layout}
    return layer


def label_layers():
    """The label layers, paint left to the variant. Tiers follow GFW's lg/md/sm."""
    city_dot = {"icon-image": "dot_11", "text-anchor": ["step", ["zoom"], "top", 8, "center"]}
    by_rank = {"symbol-sort-key": ["get", "rank"]}
    layers = [
        # Countries — the iso code stands in for the name at the lowest zooms.
        symbol(
            "country-lg",
            "place",
            place_filter("country", rank=(None, 1)),
            {"text-max-width": zoom((0, 5), (3, 6)), "text-size": zoom((1, 10), (6, 24)), **by_rank},
            minzoom=1,
            maxzoom=7,
        ),
        symbol(
            "country-md",
            "place",
            place_filter("country", rank=(2, 2)),
            {
                "text-field": ["step", ["zoom"], ["get", "iso_a2"], 2, NAME],
                "text-max-width": 6,
                "text-size": zoom((3, 10), (8, 24)),
            },
            minzoom=1,
            maxzoom=8,
        ),
        symbol(
            "country-sm",
            "place",
            place_filter("country", rank=(3, None)),
            {"text-max-width": 6, "text-size": zoom((5, 14), (9, 22), base=0.9)},
            minzoom=1,
            maxzoom=10,
        ),
        # States and provinces — uppercase and spaced, thinning out with rank.
        *[
            symbol(
                f"state-{tier}",
                "place",
                place_filter("state", "province", rank=rank),
                {
                    "text-transform": "uppercase",
                    "text-letter-spacing": 0.15,
                    "text-max-width": 6,
                    "text-size": size,
                    **by_rank,
                },
                minzoom=minzoom,
                maxzoom=maxzoom,
            )
            for tier, rank, size, minzoom, maxzoom in (
                ("lg", (None, 3), zoom((4, 10), (7, 18)), 4, 7),
                ("md", (4, 5), zoom((5, 10), (8, 16)), 5, 8),
                ("sm", (6, None), zoom((6, 10), (9, 14)), 6, 9),
            )
        ],
        # Cities — dots beside the larger ones until the names are big enough alone.
        symbol(
            "city-lg",
            "place",
            place_filter("city", rank=(None, 2)),
            {**city_dot, "text-max-width": 7, "text-size": zoom((4, 12), (12, 22), base=0.9), **by_rank},
            minzoom=1,
            maxzoom=14,
        ),
        symbol(
            "city-md",
            "place",
            place_filter("city", rank=(3, 4)),
            {
                **city_dot,
                "icon-image": "dot_10",
                "text-max-width": 7,
                "text-size": zoom((5, 12), (12, 22), base=0.9),
                **by_rank,
            },
            minzoom=4,
            maxzoom=14,
        ),
        symbol(
            "city-sm",
            "place",
            place_filter("city", rank=(5, None)),
            {"text-max-width": 7, "text-size": zoom((6, 12), (14, 22)), **by_rank},
            minzoom=6,
            maxzoom=14,
        ),
        symbol(
            "town",
            "place",
            place_filter("town"),
            {"text-max-width": 7, "text-size": zoom((7, 11.5), (15, 20)), **by_rank},
            minzoom=7,
            maxzoom=15,
        ),
        symbol(
            "village",
            "place",
            place_filter("village"),
            {"text-max-width": 7, "text-size": zoom((10, 11.5), (16, 18))},
            minzoom=11,
            maxzoom=15,
        ),
        symbol(
            "island",
            "place",
            place_filter("island"),
            {"text-max-width": 7, "text-size": zoom((10, 11), (18, 16))},
            minzoom=6,
            maxzoom=16,
        ),
    ]

    # Oceans, seas, bays and lakes arrive as points and, for the larger lakes, as
    # lines to follow. Wide letter spacing is what marks a marine name.
    on_points = ["match", ["geometry-type"], ["Point", "MultiPoint"], True, False]
    on_lines = ["match", ["geometry-type"], ["LineString", "MultiLineString"], True, False]
    water = (
        ("ocean", ["ocean"], 0.25, zoom((1, 14), (4, 30)), 1, 5),
        ("sea", ["sea"], 0.15, zoom((2, 14), (5, 20)), 2, 8),
        ("bay", ["bay"], 0.1, zoom((3, 12), (6, 16)), 4, 10),
        ("lake", ["lake"], 0.05, zoom((5, 11), (10, 14), (16, 18)), 5, 16),
    )
    for name, classes, spacing, size, minzoom, maxzoom in water:
        for shape, geometry in (("pt", on_points), ("ln", on_lines)):
            layout = {"text-letter-spacing": spacing, "text-max-width": 5, "text-size": size}
            if shape == "ln":
                layout |= {"symbol-placement": "line", "text-pitch-alignment": "viewport"}
            layers.append(
                symbol(
                    f"water-{name}-{shape}",
                    "water_name",
                    ["all", ["match", ["get", "class"], classes, True, False], geometry],
                    layout,
                    minzoom=minzoom,
                    maxzoom=maxzoom,
                )
            )

    layers.append(
        symbol(
            "river",
            "waterway",
            ["match", ["get", "class"], ["river", "canal"], True, False],
            {
                "symbol-placement": "line",
                "text-letter-spacing": 0.1,
                "text-max-angle": 30,
                "text-pitch-alignment": "viewport",
                "text-size": zoom((8, 11), (14, 13), (18, 16)),
            },
            minzoom=8,
        )
    )
    return layers


def boundary_layers():
    """The coastline and country and first-level borders.

    OpenMapTiles has no coastline layer: the shore is where a water polygon ends,
    so it is drawn as the outline of the ocean and lake polygons. It goes first,
    under the borders. Maritime border lines stay out; disputed ones are dashed.
    """
    not_maritime = ["!=", ["get", "maritime"], 1]
    undisputed = ["!=", ["get", "disputed"], 1]
    country = ["==", ["get", "admin_level"], 2]
    return [
        {
            "id": "coastline",
            "type": "line",
            "source": "openmaptiles",
            "source-layer": "water",
            "filter": ["match", ["get", "class"], ["ocean", "lake"], True, False],
            "layout": {"line-join": "round"},
            "paint": {"line-width": zoom((0, 0.4), (6, 0.8), (12, 1.3)), "line-opacity": 0.55},
        },
        {
            "id": "boundary-country",
            "type": "line",
            "source": "openmaptiles",
            "source-layer": "boundary",
            "filter": ["all", country, not_maritime, undisputed, ["!", ["has", "claimed_by"]]],
            "layout": {"line-cap": "round", "line-join": "round"},
            "paint": {"line-width": zoom((1, 0.6), (5, 1.2), (12, 2.5)), "line-opacity": 0.6},
        },
        {
            "id": "boundary-disputed",
            "type": "line",
            "source": "openmaptiles",
            "source-layer": "boundary",
            "filter": ["all", country, not_maritime, ["==", ["get", "disputed"], 1]],
            "paint": {
                "line-width": zoom((1, 0.6), (5, 1.2), (12, 2.5)),
                "line-opacity": 0.5,
                "line-dasharray": [2, 2],
            },
        },
        {
            "id": "boundary-state",
            "type": "line",
            "source": "openmaptiles",
            "source-layer": "boundary",
            "minzoom": 3,
            "filter": ["all", ["==", ["get", "admin_level"], 4], not_maritime, undisputed],
            "layout": {"line-join": "round"},
            "paint": {
                "line-width": zoom((3, 0.4), (8, 0.9), (12, 1.6)),
                "line-opacity": 0.45,
                "line-dasharray": [3, 2],
            },
        },
    ]


def authored(layers, variant, group, paint):
    """Copies of ``layers`` namespaced to a variant, hidden, in a group, with the variant's paint."""
    out = []
    for source in layers:
        layer = json.loads(json.dumps(source))
        layer["id"] = f"{variant}-{layer['id']}"
        layer.setdefault("layout", {})["visibility"] = "none"
        layer["metadata"] = {"maplibre:group": group}
        layer["paint"] = {**layer.get("paint", {}), **paint(layer)}
        out.append(layer)
    return out


def label_paint(variant):
    colors = PALETTE[variant]

    def paint(layer):
        out = {
            "text-color": colors["text"],
            "text-opacity": colors["text_opacity"],
            "text-halo-color": colors["halo"],
            "text-halo-width": 1.25,
        }
        if "icon-image" in layer["layout"]:
            # Dots belong below z8; past that the name stands on its own.
            out["icon-opacity"] = zoom((7.99, 1), (8, 0))
        return out

    return paint


def boundary_paint(variant):
    return lambda layer: {"line-color": PALETTE[variant]["border"]}


def upstream_basemap(variant, style):
    """Upstream's ground layers: no symbols, and no boundary lines, which are ours."""
    out = []
    for source in style["layers"]:
        if source["type"] == "symbol" or source.get("source-layer") == "boundary":
            continue
        layer = json.loads(json.dumps(source))
        layer["id"] = f"{variant}-{layer['id']}"
        layer.setdefault("layout", {})["visibility"] = "none"
        layer["metadata"] = {**layer.get("metadata", {}), "maplibre:group": f"basemap-{variant}"}
        out.append(layer)
    return out


def build():
    upstream = {variant: fetch(name) for variant, name in UPSTREAM.items()}
    first = next(iter(upstream.values()))

    # One source, one sprite, one glyph set: a variant that disagrees would
    # render with the wrong assets, so fail rather than guess.
    for variant, style in upstream.items():
        for key in ("sources", "sprite", "glyphs"):
            if style[key] != first[key]:
                sys.exit(f"{variant} disagrees with the others on {key!r}; merge by hand")

    basemap, boundaries, labels = [], [], []
    for variant, style in upstream.items():
        basemap += upstream_basemap(variant, style)
        boundaries += authored(boundary_layers(), variant, f"boundaries-{variant}", boundary_paint(variant))
        labels += authored(label_layers(), variant, f"labels-{variant}", label_paint(variant))

    anchor = {
        "id": ANCHOR_ID,
        "type": "background",
        "layout": {"visibility": "visible"},
        "paint": {"background-opacity": 0},
    }

    groups = {
        f"{kind}-{variant}": {"name": f"{kind}-{variant}"}
        for variant in UPSTREAM
        for kind in ("basemap", "boundaries", "labels")
    }

    return {
        "version": 8,
        "name": "GeoRiva basemap",
        "metadata": {"maplibre:groups": groups},
        "sources": first["sources"],
        "sprite": first["sprite"],
        "glyphs": first["glyphs"],
        "layers": [*basemap, anchor, *boundaries, *labels],
    }


if __name__ == "__main__":
    style = build()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(style, indent=2, ensure_ascii=False) + "\n")
    print(f"wrote {OUTPUT} ({len(style['layers'])} layers)")
