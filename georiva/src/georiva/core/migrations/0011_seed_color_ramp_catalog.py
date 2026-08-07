# Seed the instance-wide Color ramp catalog (ADR 0022): a curated,
# colorblind-aware set under matplotlib-compatible names, so plugins and
# operators always have a base vocabulary. Colors are sampled from matplotlib's
# own colormaps (9 even samples for continuous ramps; Set2's 8 discrete
# colors). Positions are left empty: even spacing is the ramp's default.
from django.db import migrations

SEED_RAMPS = [
    # (name, ramp_type, [hex, ...])
    ("viridis", "sequential", [
        "#440154", "#472d7b", "#3b528b", "#2c728e", "#21918c",
        "#28ae80", "#5ec962", "#addc30", "#fde725",
    ]),
    ("cividis", "sequential", [
        "#00224e", "#1a386f", "#434e6c", "#61656f", "#7d7c78",
        "#9b9476", "#bcae6c", "#dec958", "#fee838",
    ]),
    ("plasma", "sequential", [
        "#0d0887", "#4c02a1", "#7e03a8", "#aa2395", "#cc4778",
        "#e66c5c", "#f89540", "#fdc527", "#f0f921",
    ]),
    ("magma", "sequential", [
        "#000004", "#1d1147", "#51127c", "#832681", "#b73779",
        "#e75263", "#fc8961", "#fec488", "#fcfdbf",
    ]),
    ("inferno", "sequential", [
        "#000004", "#210c4a", "#57106e", "#8a226a", "#bc3754",
        "#e45a31", "#f98e09", "#f9cb35", "#fcffa4",
    ]),
    ("Blues", "sequential", [
        "#f7fbff", "#deebf7", "#c6dbef", "#9dcae1", "#6aaed6",
        "#4191c6", "#2070b4", "#08509b", "#08306b",
    ]),
    ("Greens", "sequential", [
        "#f7fcf5", "#e5f5e0", "#c7e9c0", "#a0d99b", "#73c476",
        "#40aa5d", "#228a44", "#006c2c", "#00441b",
    ]),
    ("Oranges", "sequential", [
        "#fff5eb", "#fee6ce", "#fdd0a2", "#fdae6a", "#fd8c3b",
        "#f16813", "#d84801", "#a53603", "#7f2704",
    ]),
    ("Purples", "sequential", [
        "#fcfbfd", "#efedf5", "#dadaeb", "#bcbddc", "#9e9ac8",
        "#807cba", "#6950a3", "#53268f", "#3f007d",
    ]),
    ("Reds", "sequential", [
        "#fff5f0", "#fee0d2", "#fcbba1", "#fc9272", "#fb694a",
        "#ee3a2c", "#ca181d", "#a30f15", "#67000d",
    ]),
    ("YlGnBu", "sequential", [
        "#ffffd9", "#edf8b1", "#c6e9b4", "#7ecdbb", "#40b5c4",
        "#1d90c0", "#225da8", "#243392", "#081d58",
    ]),
    ("YlOrRd", "sequential", [
        "#ffffcc", "#ffeda0", "#fed976", "#feb24c", "#fd8c3c",
        "#fc4d2a", "#e2191c", "#bb0026", "#800026",
    ]),
    ("Greys", "sequential", [
        "#ffffff", "#f0f0f0", "#d9d9d9", "#bdbdbd", "#959595",
        "#727272", "#515151", "#242424", "#000000",
    ]),
    ("RdBu", "diverging", [
        "#67001f", "#bb2a34", "#e58368", "#fbceb7", "#f6f7f7",
        "#c0dceb", "#68abd0", "#2870b1", "#053061",
    ]),
    ("RdYlBu", "diverging", [
        "#a50026", "#de402e", "#f98e52", "#fed485", "#feffc0",
        "#d1ecf4", "#8ec2dc", "#4f81ba", "#313695",
    ]),
    ("BrBG", "diverging", [
        "#543005", "#995d13", "#cfa256", "#f1dfb3", "#f4f5f5",
        "#b4e2db", "#58b0a7", "#0c7169", "#003c30",
    ]),
    ("PuOr", "diverging", [
        "#7f3b08", "#be630a", "#ef9e3c", "#fed7a2", "#f6f6f7",
        "#cecde4", "#988dbe", "#5d3790", "#2d004b",
    ]),
    ("Set2", "qualitative", [
        "#66c2a5", "#fc8d62", "#8da0cb", "#e78ac3", "#a6d854",
        "#ffd92f", "#e5c494", "#b3b3b3",
    ]),
]


def seed_catalog(apps, schema_editor):
    ColorRamp = apps.get_model("georivacore", "ColorRamp")
    ColorRampStop = apps.get_model("georivacore", "ColorRampStop")
    for name, ramp_type, hexes in SEED_RAMPS:
        ramp, created = ColorRamp.objects.get_or_create(
            organisation=None, name=name, defaults={"ramp_type": ramp_type}
        )
        if not created:
            # An instance that already has a global ramp by this name keeps it:
            # the seed provides a vocabulary, it does not overwrite curation.
            continue
        ColorRampStop.objects.bulk_create([
            ColorRampStop(ramp=ramp, hex_value=hex_value, sort_order=i)
            for i, hex_value in enumerate(hexes)
        ])


def unseed_catalog(apps, schema_editor):
    ColorRamp = apps.get_model("georivacore", "ColorRamp")
    ColorRamp.objects.filter(
        organisation__isnull=True, name__in=[name for name, _, _ in SEED_RAMPS]
    ).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("georivacore", "0010_colorramp_colorrampstop_and_more"),
    ]

    operations = [
        migrations.RunPython(seed_catalog, unseed_catalog),
    ]
