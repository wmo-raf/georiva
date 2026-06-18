# This file is heavily inspired by the MetPy (metpy.units) library, which is licensed under the BSD 3-Clause License.

import contextlib
import re

import pint
from django.core.exceptions import ValidationError
from django.utils.translation import gettext_lazy as _

_base_unit_of_dimensionality = {
    '[pressure]': 'Pa',
    '[temperature]': 'K',
    '[dimensionless]': '',
    '[length]': 'm',
    '[speed]': 'm s**-1'
}


def _fix_udunits_powers(string):
    """Replace UDUNITS-style powers (m2 s-2) with exponent symbols (m**2 s**-2)."""
    return _UDUNIT_POWER.sub('**', string)


def _fix_udunits_div(string):
    return 's**-1' if string == '/s' else string


# Fix UDUNITS-style powers, percent signs, and ill-defined units
_UDUNIT_POWER = re.compile(r'(?<=[A-Za-z\)])(?![A-Za-z\)])'
                           r'(?<![0-9\-][eE])(?<![0-9\-])(?=[0-9\-])')
_unit_preprocessors = [_fix_udunits_powers, lambda string: string.replace('%', 'percent'),
                       _fix_udunits_div]


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
    reg.define('degrees_north = degree = degrees_N = degreesN = degree_north = degree_N '
               '= degreeN')
    reg.define('degrees_east = degree = degrees_E = degreesE = degree_east = degree_E '
               '= degreeE')
    reg.define('dBz = 1e-18 m^3; logbase: 10; logfactor: 10 = dBZ')
    
    # Alias geopotential meters (gpm) to just meters
    reg.define('@alias meter = gpm')

    # Geopotential decametre (gpdam) — the unit ECMWF charts plot geopotential
    # height in (1 gpdam = 10 gpm). Defined explicitly because pint has no
    # decametre alias to hang it off.
    reg.define('gpdam = 10 * meter')

    # Geopotential Φ is a specific energy (J/kg = m²/s²). Naming the dimension
    # lets the geopotential context below bridge it to [length].
    reg.define('[specific_enthalpy] = [energy] / [mass]')

    # custom contexts
    
    # Define a context for precipitation
    precipitation = pint.Context('precipitation')
    
    # Precipitation amount
    # 1 mm of rainfall = 1 kg/m² for water
    # Forward transformation (mm -> kg/m²)
    precipitation.add_transformation('[length]', '[mass] / [length] ** 2',
                                     lambda reg, x: x * reg('kg/m^2') / reg('mm'))
    
    # Reverse transformation (kg/m² -> mm)
    precipitation.add_transformation('[mass] / [length] ** 2', '[length]',
                                     lambda reg, x: x * reg('mm') / reg('kg/m^2'))
    
    # Precipitation Rate
    # Forward transformation (mm/h -> kg/m²/h)
    precipitation.add_transformation('[length] / [time]', '[mass] / [length] ** 2 / [time]',
                                     lambda reg, x: x * reg('kg/m^2/h') / reg('mm/h'))
    
    # Reverse transformation (kg/m²/h -> mm/h)
    precipitation.add_transformation('[mass] / [length] ** 2 / [time]', '[length] / [time]',
                                     lambda reg, x: x * reg('mm/h') / reg('kg/m^2/h'))
    
    reg.add_context(precipitation)

    # Define a context for geopotential <-> geopotential height
    geopotential = pint.Context('geopotential')

    # Geopotential height Z = Φ / g, with g = standard gravity (9.80665 m/s²).
    # This bridges specific energy ([length]²/[time]²) to length, which pint
    # otherwise refuses as dimensionally inconsistent. Combined with the gpdam
    # unit, m²/s² → gpdam divides by g then by 10 in a single conversion.
    geopotential.add_transformation('[length] ** 2 / [time] ** 2', '[length]',
                                    lambda reg, x: x / reg('9.80665 m/s**2'))
    geopotential.add_transformation('[length]', '[length] ** 2 / [time] ** 2',
                                    lambda reg, x: x * reg('9.80665 m/s**2'))

    reg.add_context(geopotential)

    # Geopotential height conversion is physically unambiguous (it only ever
    # fires for m²/s² <-> length), so enable it globally — callers of
    # apply_unit_conversion need not name the context. Precipitation stays
    # opt-in because mm <-> kg/m² is genuinely ambiguous.
    reg.enable_contexts('geopotential')

    return reg


# Make our modifications using pint's application registry--which allows us to better
# interoperate with other libraries using Pint.
ureg = setup_registry(pint.get_application_registry())


def validate_unit(unit):
    """Check if a unit is valid."""
    try:
        ureg(unit)
    except pint.errors.UndefinedUnitError as e:
        raise ValidationError(
            _("'%(unit)s' is not defined in the unit registry"),
            params={"unit": unit},
        )


TEMPERATURE_UNITS = [
    'degree_Celsius',
    'celsius',
    'degC',
    'degreeC',
    '°C',
    'degree_Fahrenheit',
    'fahrenheit',
    'degF',
    'degreeF',
    '°F',
    'degree_Kelvin',
    'kelvin',
    'degK',
    'degreeK',
    'K',
    '°K',
    'degree_Rankine',
    'rankine',
    'degR',
    'degreeR',
    '°R'
]
