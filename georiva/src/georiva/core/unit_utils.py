# This file is heavily inspired by the MetPy (metpy.units) library, which is licensed under the BSD 3-Clause License.

import contextlib
import re

import pint
from django.core.exceptions import ValidationError
from django.utils.translation import gettext_lazy as _

_base_unit_of_dimensionality = {
    "[pressure]": "Pa",
    "[temperature]": "K",
    "[dimensionless]": "",
    "[length]": "m",
    "[speed]": "m s**-1",
}


def _fix_udunits_powers(string):
    """Replace UDUNITS-style powers (m2 s-2) with exponent symbols (m**2 s**-2)."""
    return _UDUNIT_POWER.sub("**", string)


def _fix_udunits_div(string):
    return "s**-1" if string == "/s" else string


# Fix UDUNITS-style powers, percent signs, and ill-defined units
_UDUNIT_POWER = re.compile(
    r"(?<=[A-Za-z\)])(?![A-Za-z\)])"
    r"(?<![0-9\-][eE])(?<![0-9\-])(?=[0-9\-])"
)
_unit_preprocessors = [_fix_udunits_powers, lambda string: string.replace("%", "percent"), _fix_udunits_div]


def setup_registry(reg):
    """Set up a given registry with MetPy's default tweaks and settings."""
    reg.autoconvert_offset_to_baseunit = True

    # For Pint 0.18.0, need to deal with the fact that the wrapper isn't forwarding on setting
    # the attribute.
    with contextlib.suppress(AttributeError):
        reg.get().autoconvert_offset_to_baseunit = True

    for pre in _unit_preprocessors:
        if pre not in reg.preprocessors:
            reg.preprocessors.append(pre)

    # Define commonly encountered units not defined by pint
    reg.define("degrees_north = degree = degrees_N = degreesN = degree_north = degree_N = degreeN")
    reg.define("degrees_east = degree = degrees_E = degreesE = degree_east = degree_E = degreeE")
    reg.define("dBz = 1e-18 m^3; logbase: 10; logfactor: 10 = dBZ")

    # Alias geopotential meters (gpm) to just meters
    reg.define("@alias meter = gpm")

    # Geopotential decametre (gpdam) — the unit ECMWF charts plot geopotential
    # height in (1 gpdam = 10 gpm). Defined explicitly because pint has no
    # decametre alias to hang it off.
    reg.define("gpdam = 10 * meter")

    # Geopotential Φ is a specific energy (J/kg = m²/s²). Naming the dimension
    # lets the geopotential context below bridge it to [length].
    reg.define("[specific_enthalpy] = [energy] / [mass]")

    # custom contexts

    # Define a context for precipitation
    precipitation = pint.Context("precipitation")

    # Precipitation amount
    # 1 mm of rainfall = 1 kg/m² for water
    # Forward transformation (mm -> kg/m²)
    precipitation.add_transformation("[length]", "[mass] / [length] ** 2", lambda reg, x: x * reg("kg/m^2") / reg("mm"))

    # Reverse transformation (kg/m² -> mm)
    precipitation.add_transformation("[mass] / [length] ** 2", "[length]", lambda reg, x: x * reg("mm") / reg("kg/m^2"))

    # Precipitation Rate
    # Forward transformation (mm/h -> kg/m²/h)
    precipitation.add_transformation(
        "[length] / [time]", "[mass] / [length] ** 2 / [time]", lambda reg, x: x * reg("kg/m^2/h") / reg("mm/h")
    )

    # Reverse transformation (kg/m²/h -> mm/h)
    precipitation.add_transformation(
        "[mass] / [length] ** 2 / [time]", "[length] / [time]", lambda reg, x: x * reg("mm/h") / reg("kg/m^2/h")
    )

    reg.add_context(precipitation)

    # Define a context for geopotential <-> geopotential height
    geopotential = pint.Context("geopotential")

    # Geopotential height Z = Φ / g, with g = standard gravity (9.80665 m/s²).
    # This bridges specific energy ([length]²/[time]²) to length, which pint
    # otherwise refuses as dimensionally inconsistent. Combined with the gpdam
    # unit, m²/s² → gpdam divides by g then by 10 in a single conversion.
    geopotential.add_transformation("[length] ** 2 / [time] ** 2", "[length]", lambda reg, x: x / reg("9.80665 m/s**2"))
    geopotential.add_transformation("[length]", "[length] ** 2 / [time] ** 2", lambda reg, x: x * reg("9.80665 m/s**2"))

    reg.add_context(geopotential)

    # Geopotential height conversion is physically unambiguous (it only ever
    # fires for m²/s² <-> length), so enable it globally — callers of
    # affine_conversion need not name the context. Precipitation stays
    # opt-in because mm <-> kg/m² is genuinely ambiguous.
    reg.enable_contexts("geopotential")

    return reg


# Make our modifications using pint's application registry--which allows us to better
# interoperate with other libraries using Pint.
ureg = setup_registry(pint.get_application_registry())


def validate_unit(unit):
    """Check if a unit is valid."""
    try:
        ureg(unit)
    except pint.errors.UndefinedUnitError:
        raise ValidationError(
            _("'%(unit)s' is not defined in the unit registry"),
            params={"unit": unit},
        ) from None


TEMPERATURE_UNITS = [
    "degree_Celsius",
    "celsius",
    "degC",
    "degreeC",
    "°C",
    "degree_Fahrenheit",
    "fahrenheit",
    "degF",
    "degreeF",
    "°F",
    "degree_Kelvin",
    "kelvin",
    "degK",
    "degreeK",
    "K",
    "°K",
    "degree_Rankine",
    "rankine",
    "degR",
    "degreeR",
    "°R",
]


class ConversionRefused(ValueError):
    """A conversion a Variable may not be served through (ADR 0046 §4, §6).

    ``str()`` of it is an operator-readable reason.
    """


#: How closely ``f(2)`` must match ``offset + 2·scale`` for a conversion to count as affine.
_AFFINE_TOLERANCE = 1e-9


def affine_conversion(source_symbol: str, served_symbol: str, *, water: bool = False) -> tuple[float, float]:
    """``(scale, offset)`` such that ``served = raw·scale + offset`` (ADR 0046 §4).

    Computed with pint as ``offset = f(0)`` and ``scale = f(1) − f(0)``, and
    checked with ``f(2) = offset + 2·scale``. Refused — :class:`ConversionRefused`
    — when the units are of different dimensions, when the conversion is not
    affine (dBZ), when the scale is not positive, and when it is ``kg m-2 ↔ mm``
    (or a rate of them) and ``water`` is not set. ``water`` enables the
    ``precipitation`` context for this computation only.
    """
    if source_symbol == served_symbol:
        return 1.0, 0.0
    try:
        source, served = ureg(source_symbol).units, ureg(served_symbol).units
    except (pint.errors.PintError, AttributeError, TypeError, ValueError) as e:
        raise ConversionRefused(f"'{source_symbol}' or '{served_symbol}' is not a unit pint knows ({e}).") from None

    def f(x):
        return float(ureg.Quantity(x, source).to(served).magnitude)

    def at_0_1_2():
        return f(0), f(1), f(2)

    try:
        if water:
            with ureg.context("precipitation"):
                at = at_0_1_2()
        else:
            at = at_0_1_2()
    except pint.errors.DimensionalityError:
        if not water and _converts_as_water(f):
            raise ConversionRefused(
                f"{source_symbol} → {served_symbol} holds only for water. "
                "Set the water flag if this Variable's values are water."
            ) from None
        raise ConversionRefused(
            f"Cannot convert from {source_symbol} to {served_symbol}: incompatible dimensions."
        ) from None
    except pint.errors.PintError as e:
        raise ConversionRefused(f"Cannot convert from {source_symbol} to {served_symbol}: {e}.") from None

    offset, scale = at[0], at[1] - at[0]
    expected = offset + 2 * scale
    if abs(at[2] - expected) > _AFFINE_TOLERANCE * max(1.0, abs(at[2]), abs(expected)):
        raise ConversionRefused(
            f"{source_symbol} → {served_symbol} is not a linear conversion (a scale and an offset), "
            "so it cannot be served."
        )
    if not scale > 0:
        raise ConversionRefused(
            f"{source_symbol} → {served_symbol} has a scale of {scale:g}; only a positive scale can be served."
        )
    return scale, offset


def _converts_as_water(f) -> bool:
    try:
        with ureg.context("precipitation"):
            f(1)
    except pint.errors.PintError:
        return False
    return True


def difference_unit(symbol: str) -> str:
    """The unit a difference of values in ``symbol`` is in (ADR 0046 §7).

    A unit with an offset from its base — °C, °F — has a delta unit of its own
    (``delta_degC``), which converts by scale alone. Any other unit is its own
    difference unit: a difference of K is K, of mm is mm. A symbol pint does not
    know is returned as it is.
    """
    try:
        zero = ureg.Quantity(0, symbol).to_base_units().magnitude
    except (pint.errors.PintError, AttributeError, TypeError, ValueError):
        return symbol
    return symbol if zero == 0 else f"delta_{symbol}"
