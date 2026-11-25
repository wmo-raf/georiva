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

from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils.text import slugify
from django.utils.translation import gettext as _

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
    "new_style",
    "proposed_encoding",
    "save_edit",
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


def new_style(variable, name, **fields) -> VariableStyle:
    """An unsaved style on ``variable``: its slug minted from ``name``, the
    default when it is the Variable's first."""
    return VariableStyle(
        variable=variable,
        name=name,
        slug=unique_style_slug(variable, name),
        is_default=variable.new_style_is_default,
        **fields,
    )


def start_from_ramp(ramp, variable, *, mode=VariableStyle.Mode.CONTINUOUS, steps=None, style=None) -> VariableStyle:
    """A new style on ``variable`` from ``ramp`` (#800): named after the ramp,
    its stops generated over the Variable's current range, the default when it
    is the Variable's first. ``steps`` counts only when ``mode`` is stepped.

    Given ``style``, that style is remade from ``ramp`` instead ("Start over",
    #807): its origin, stops and link replaced, its name, slug and default
    status kept."""
    with transaction.atomic():
        if style is None:
            style = new_style(variable, ramp.name)
        style.start_over(VariableStyle.Origin.RAMP, ramp=ramp)
        style.mode = mode
        style.steps = steps if mode == VariableStyle.Mode.STEPPED else None
        style.apply_ramp()
        style.full_clean()
        style.save()
    return style


def _use_template(style, template):
    """Give ``style`` ``template``'s bands, out-of-range settings and link,
    unsaved: what applying a template writes, and what picking another
    template in a template style's editor replaces (#807). Raises
    :class:`ConversionRefused` when the template's unit does not convert into
    the Variable's source unit."""
    style.stops = template_stops(template, style.variable)
    style.template = template
    style.mode = VariableStyle.Mode.STEPPED
    style.steps = len(template.colors)
    style.under = template.under
    style.over = template.over


def _save_with_template(style, template, take_encoding):
    """Save ``style`` and, when ``take_encoding``, set its Variable's range and
    curve to ``template``'s proposal — together or not at all."""
    with transaction.atomic():
        style.save()
        if take_encoding:
            accept_encoding(style.variable, proposed_encoding(template, style.variable))


def save_edit(
    style,
    *,
    name,
    under,
    over,
    stops=(),
    ramp=None,
    mode=None,
    steps=None,
    reverse=False,
    template=None,
    take_encoding=False,
) -> VariableStyle:
    """Save what ``style``'s editor posted, as its origin says (#806) — the one
    place that decides what a save does to each kind of style:

    * ``template``: the stops stay the template's bands. ``template`` names
      another template picked in place (#807): its bands and link replace
      them, and the default style takes its range and curve when
      ``take_encoding``.
    * ``ramp``: the stops are what ``ramp``, ``mode``, ``steps`` and
      ``reverse`` give over the Variable's current range; ``stops`` is ignored.
    * ``custom``: the stops are ``stops`` (source unit), and there must be
      some — "from scratch" opens on an empty table but never stores one.

    Every origin takes ``name``, ``under`` and ``over``. Raises
    :class:`ValidationError` or :class:`ConversionRefused`, saving nothing.
    """
    style.name = name
    if style.origin == VariableStyle.Origin.TEMPLATE:
        if template is not None:
            _use_template(style, template)
    elif style.origin == VariableStyle.Origin.RAMP:
        if ramp is None:
            raise ValidationError({"ramp": _("Pick a ramp.")})
        style.ramp = ramp
        style.mode = mode or VariableStyle.Mode.CONTINUOUS
        style.steps = steps
        style.reverse = reverse
        style.apply_ramp()
    else:
        if not stops:
            raise ValidationError({"stops": _("Add at least one stop.")})
        style.stops = list(stops)
    style.under = under
    style.over = over
    style.full_clean()
    # Only the default style may set the range and curve every style shares
    # (#791), and only on a switch of template.
    _save_with_template(style, template, template is not None and take_encoding and style.is_default)
    return style


def apply_template(template, variable, *, name=None, style=None, take_encoding=False) -> VariableStyle:
    """A new stepped style on ``variable`` with ``template``'s look — or, given
    ``style``, that style remade from ``template`` ("Start over", #807): its
    origin, stops and link replaced, its name, slug and default status kept.

    A new style is the default when it is the Variable's first
    (:attr:`Variable.new_style_is_default`); a later one is promoted from its
    own tab (#800). The Variable's range and curve are left alone unless
    ``take_encoding``, when they become :func:`proposed_encoding` — the
    caller offers that to the style that is or will be the default only.
    Raises :class:`ConversionRefused` when the template's unit does not
    convert into the Variable's source unit.
    """
    if style is None:
        style = new_style(variable, name or template.name)
    style.start_over(VariableStyle.Origin.TEMPLATE, template=template)
    _use_template(style, template)
    style.full_clean()
    _save_with_template(style, template, take_encoding)
    return style


def accept_encoding(variable, encoding: Encoding):
    """Set ``variable``'s range and curve to ``encoding`` — the confirmed proposal."""
    Variable.validate_value_range(encoding.value_min, encoding.value_max)
    Variable.validate_scale_type(encoding.scale_type, encoding.value_min)
    variable.value_min, variable.value_max = encoding.value_min, encoding.value_max
    variable.scale_type = encoding.scale_type
    variable.save(update_fields=["value_min", "value_max", "scale_type", "modified"])
