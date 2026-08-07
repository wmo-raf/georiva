"""Retire the value-anchored palettes into the two-layer model (ADR 0022).

Two conversions, both preserving what an operator configured:

* every ``ColorPalette`` with stops is normalized (values → 0–1 positions)
  into a ``ColorRamp`` in the same tier, so its aesthetic survives into the
  catalog and stays re-applyable;
* every variable's *assigned* palette is materialized, stop for stop, into
  that variable's default ``VariableStyle`` — absolute values verbatim, so
  serving output is unchanged from any client's point of view.

The pure transforms live as module functions so the tests exercise exactly
what the migration runs.
"""
from django.db import migrations
from django.utils.text import slugify

#: The legacy palette-type vocabulary in ramp-type terms. Only the name of the
#: categorical idea changes; the shipped catalog says "qualitative".
RAMP_TYPE_FOR_PALETTE_TYPE = {
    'sequential': 'sequential',
    'diverging': 'diverging',
    'categorical': 'qualitative',
}


def canonical_hex(hex_value: str) -> str:
    """A stop color with its '#' — the legacy rows stored either spelling."""
    value = (hex_value or "").strip()
    return value if value.startswith('#') else f"#{value}"


def normalized_positions(values: list) -> list:
    """Absolute stop values → 0–1 ramp positions, preserving stop order.

    A degenerate span (every value equal, or a single stop) spreads evenly:
    there is no scale left to preserve, only the color sequence.
    """
    if not values:
        return []
    low, high = min(values), max(values)
    span = high - low
    if span == 0:
        last = len(values) - 1
        return [i / last for i in range(len(values))] if last else [0.0]
    return [(value - low) / span for value in values]


def style_stops_from(stop_rows: list) -> list:
    """[(value, hex)] rows → the stops snapshot, values verbatim."""
    return [
        {"value": value, "color": canonical_hex(hex_value)}
        for value, hex_value in stop_rows
    ]


def style_slug_from(name: str) -> str:
    """A style slug from a palette name, with a floor for unsluggable names."""
    return slugify(name)[:255] or "default"


def _unique_ramp_name(ColorRamp, organisation_id, base_name):
    """``base_name``, suffixed just enough to clear the tier's unique names —
    the seeded catalog may already own e.g. "viridis" at the instance tier."""
    name = base_name
    counter = 1
    while ColorRamp.objects.filter(
        organisation_id=organisation_id, name=name
    ).exists() or (
        organisation_id is None
        and ColorRamp.objects.filter(organisation__isnull=True, name=name).exists()
    ):
        counter += 1
        suffix = " (migrated)" if counter == 2 else f" (migrated {counter - 1})"
        name = f"{base_name[:255 - len(suffix)]}{suffix}"
    return name


def forward(apps, schema_editor):
    ColorPalette = apps.get_model('georivacore', 'ColorPalette')
    ColorRamp = apps.get_model('georivacore', 'ColorRamp')
    ColorRampStop = apps.get_model('georivacore', 'ColorRampStop')
    Variable = apps.get_model('georivacore', 'Variable')
    VariableStyle = apps.get_model('georivacore', 'VariableStyle')

    converted = {}
    for palette in ColorPalette.objects.order_by('pk'):
        stop_rows = [
            (stop.value, stop.hex_value)
            for stop in palette.stops.order_by('sort_order', 'pk')
        ]
        ramp = None
        if stop_rows:
            ramp = ColorRamp.objects.create(
                organisation_id=palette.organisation_id,
                name=_unique_ramp_name(
                    ColorRamp, palette.organisation_id, palette.name
                ),
                ramp_type=RAMP_TYPE_FOR_PALETTE_TYPE.get(
                    palette.palette_type, 'sequential'
                ),
            )
            positions = normalized_positions([value for value, _ in stop_rows])
            ColorRampStop.objects.bulk_create([
                ColorRampStop(
                    ramp=ramp,
                    hex_value=canonical_hex(hex_value),
                    position=position,
                    sort_order=index,
                )
                for index, ((_, hex_value), position)
                in enumerate(zip(stop_rows, positions))
            ])
        converted[palette.pk] = (palette.name, ramp, stop_rows)

    for variable in Variable.objects.filter(palette__isnull=False).order_by('pk'):
        name, ramp, stop_rows = converted[variable.palette_id]
        VariableStyle.objects.create(
            variable=variable,
            name=name,
            slug=style_slug_from(name),
            is_default=True,
            ramp=ramp,
            mode='continuous',
            stops=style_stops_from(stop_rows),
        )


class Migration(migrations.Migration):

    dependencies = [
        ('georivacore', '0012_variablestyle'),
    ]

    operations = [
        migrations.RunPython(forward, migrations.RunPython.noop),
    ]
