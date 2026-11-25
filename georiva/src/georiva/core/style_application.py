"""Applying a Style template to a Variable (#791).

A template is levels and band colours in the unit it declares; a Variable
style is stops in the Variable's source unit (ADR 0046 §3). Applying one
writes the other once, as a stepped snapshot the operator then owns (ADR
0022): each band becomes two stops sharing its boundaries, converted through
the same positive-affine check a Variable's own units pass, water flag
included. Nothing links back — editing the style never touches the template,
and a template edited later never touches the style.

The template also recommends an encoding: a range from its first to its last
level, and a curve (#787). That is only ever a proposal here. A Variable's
range and curve are shared by all its styles, so only a default style may
propose them, and only an explicit confirmation sets them
(:func:`accept_encoding`). Whichever encoding ends up in force,
:func:`warnings` says which bands it starves of codes and which levels it
clips.
"""

from dataclasses import dataclass

from django.db import transaction
from django.utils.text import slugify

from georiva.core.machine_plane.colour_table import CODES, encode, encoding_curve
from georiva.core.models import Variable, VariableStyle
from georiva.core.models.visualization import band_stops
from georiva.core.unit_utils import Conversion, ConversionRefused
from georiva.utils.slugs import free_slug

#: A band given fewer codes than this is drawn as a sliver, or not at all.
MIN_BAND_CODES = 2

__all__ = [
    "MIN_BAND_CODES",
    "ConversionRefused",
    "Encoding",
    "accept_encoding",
    "apply_template",
    "conversion",
    "current_encoding",
    "proposed_encoding",
    "template_stops",
    "unique_style_slug",
    "warnings",
]


@dataclass(frozen=True)
class Encoding:
    """A Variable's range and curve: stored (source-unit) bounds and a ``scale_type``.

    Shaped like the Variable fields it stands for, so
    :func:`~georiva.core.machine_plane.colour_table.encoding_curve` resolves it
    as it would the Variable.
    """

    value_min: float
    value_max: float
    scale_type: str


def conversion(template, variable) -> Conversion:
    """Values in ``template``'s unit into ``variable``'s source unit, or
    :class:`ConversionRefused` with the reason it is not offered."""
    return Conversion.between(template.unit.symbol, variable.source_unit.symbol, water=variable.water)


def template_stops(template, variable) -> list:
    """The template's bands as stepped stops in ``variable``'s source unit:
    each band two stops, at its lower and upper level, in the band's colour."""
    return band_stops(template.levels, template.colors, conversion(template, variable).forward)


def current_encoding(variable) -> Encoding:
    return Encoding(variable.value_min, variable.value_max, variable.scale_type)


def proposed_encoding(template, variable) -> Encoding:
    """The range from the template's first to its last level, converted, and
    its recommended curve — or linear, where the curve is ``log`` and the
    converted range does not start above zero, which ``log`` cannot encode."""
    convert = conversion(template, variable).forward
    value_min, value_max = convert(template.levels[0]), convert(template.levels[-1])
    scale_type = template.scale_type
    if scale_type == Variable.ScaleType.LOG and value_min <= 0:
        scale_type = Variable.ScaleType.LINEAR
    return Encoding(value_min, value_max, scale_type)


def _band_label(low, high, unit) -> str:
    return f"{low:g}–{high:g} {unit}"


def warnings(template, variable, encoding: Encoding) -> list[str]:
    """What ``encoding`` does to ``template``'s bands on ``variable``, as
    sentences naming them: bands with fewer than :data:`MIN_BAND_CODES` of the
    256 codes, and levels outside the range, which are clipped.

    A band wholly outside the range is reported by its levels only.
    """
    convert = conversion(template, variable).forward
    vmin, vmax = encoding.value_min, encoding.value_max
    curve = encoding_curve(encoding)
    unit = template.unit.symbol
    levels = template.levels

    def outside(value):
        return value < vmin or value > vmax

    starved = []
    for low, high in zip(levels, levels[1:], strict=False):
        stored_low, stored_high = convert(low), convert(high)
        if stored_high <= vmin or stored_low >= vmax:
            continue
        codes = encode(stored_high, vmin, vmax, curve) - encode(stored_low, vmin, vmax, curve)
        if codes < MIN_BAND_CODES:
            starved.append(_band_label(low, high, unit))
    clipped = [f"{level:g}" for level in levels if outside(convert(level))]

    messages = []
    if starved:
        messages.append(
            f"These bands get fewer than {MIN_BAND_CODES} of the {CODES} codes under the "
            f"{curve} encoding of this range, so they will barely show on the map: {', '.join(starved)}."
        )
    if clipped:
        messages.append(
            f"These levels fall outside the Variable's range and will be clipped: {', '.join(clipped)} {unit}."
        )
    return messages


def unique_style_slug(variable, name) -> str:
    """A slug for a new style, unique within the variable.

    The slug is the machine-plane selector (ADR 0023), so it is minted once
    from the name and never edited.
    """
    return free_slug(slugify(name) or "style", set(variable.styles.values_list("slug", flat=True)))


def _new_style(variable, name, **fields) -> VariableStyle:
    """An unsaved style on ``variable``: its slug minted from ``name``, the
    default when it is the Variable's first."""
    return VariableStyle(
        variable=variable,
        name=name,
        slug=unique_style_slug(variable, name),
        is_default=variable.new_style_is_default,
        **fields,
    )


def start_from_ramp(ramp, variable, *, mode=VariableStyle.Mode.CONTINUOUS, steps=None) -> VariableStyle:
    """A new style on ``variable`` from ``ramp`` (#800): named after the ramp,
    its stops generated over the Variable's current range, the default when it
    is the Variable's first. ``steps`` counts only when ``mode`` is stepped."""
    with transaction.atomic():
        style = _new_style(
            variable,
            ramp.name,
            origin=VariableStyle.Origin.RAMP,
            ramp=ramp,
            mode=mode,
            steps=steps if mode == VariableStyle.Mode.STEPPED else None,
        )
        style.apply_ramp()
        style.full_clean()
        style.save()
    return style


def apply_template(template, variable, *, name=None) -> VariableStyle:
    """A new stepped style on ``variable`` with ``template``'s look.

    The style is the default when it is the Variable's first
    (:attr:`Variable.new_style_is_default`); a later one is promoted from its
    own tab (#800). The Variable's range and curve are left alone:
    :func:`proposed_encoding` is a proposal, set by :func:`accept_encoding`
    only. Raises :class:`ConversionRefused` when the
    template's unit does not convert into the Variable's source unit.
    """
    stops = template_stops(template, variable)
    name = name or template.name
    with transaction.atomic():
        style = _new_style(
            variable,
            name,
            mode=VariableStyle.Mode.STEPPED,
            steps=len(template.colors),
            stops=stops,
            under=template.under,
            over=template.over,
            template=template,
            origin=VariableStyle.Origin.TEMPLATE,
        )
        style.full_clean()
        style.save()
    return style


def accept_encoding(variable, encoding: Encoding):
    """Set ``variable``'s range and curve to ``encoding`` — the confirmed proposal."""
    Variable.validate_value_range(encoding.value_min, encoding.value_max)
    Variable.validate_scale_type(encoding.scale_type, encoding.value_min)
    variable.value_min, variable.value_max = encoding.value_min, encoding.value_max
    variable.scale_type = encoding.scale_type
    variable.save(update_fields=["value_min", "value_max", "scale_type", "modified"])
