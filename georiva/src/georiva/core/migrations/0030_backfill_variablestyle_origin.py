"""Record how every existing Variable style was made (#806).

A style is ``template`` when its stops are what its template produces today,
``ramp`` when they are what its ramp produces over the Variable's current range
(forwards, or reversed, which ``reverse`` now records), and ``custom``
otherwise — a hand-tuned ramp style included. No stops change.

The comparison runs the app's own generators (``ramp_stops_over``,
``band_stops``, ``recorded_origin``), which read only plain fields and a ramp's
ordered stops, so they work on historical models.
"""

from django.db import migrations

from georiva.core.models.visualization import band_stops, ramp_stops_over, recorded_origin
from georiva.core.unit_utils import Conversion, ConversionRefused


def _template_stops(style, variable):
    """The template's bands converted into the Variable's source unit, or None."""
    template = style.template
    if template is None:
        return None
    try:
        convert = Conversion.between(template.unit.symbol, variable.source_unit.symbol, water=variable.water).forward
        return band_stops(template.levels, template.colors, convert)
    except (ConversionRefused, ValueError, TypeError):
        return None


def _ramp_stops(style, variable):
    """The ramp forwards over the Variable's served range, as stored (source unit), or None."""
    if style.ramp is None:
        return None
    served = Conversion(variable.scale, variable.offset)
    try:
        stops = ramp_stops_over(
            style.ramp,
            served.forward(variable.value_min),
            served.forward(variable.value_max),
            mode=style.mode,
            steps=style.steps,
            scale_type=variable.scale_type,
        )
    except (ValueError, TypeError, ZeroDivisionError):
        return None
    return [{**stop, "value": served.inverse(stop["value"])} for stop in stops]


def backfill_origins(apps, schema_editor):
    VariableStyle = apps.get_model("georivacore", "VariableStyle")
    styles = VariableStyle.objects.select_related("variable__source_unit", "template__unit", "ramp")
    for style in styles.iterator():
        variable = style.variable
        origin, reverse = recorded_origin(
            style.stops or [],
            template_stops=_template_stops(style, variable),
            ramp_stops=_ramp_stops(style, variable),
        )
        if (style.origin, style.reverse) != (origin, reverse):
            VariableStyle.objects.filter(pk=style.pk).update(origin=origin, reverse=reverse)


class Migration(migrations.Migration):
    dependencies = [
        ("georivacore", "0029_variablestyle_origin_reverse"),
    ]

    operations = [
        migrations.RunPython(backfill_origins, migrations.RunPython.noop),
    ]
