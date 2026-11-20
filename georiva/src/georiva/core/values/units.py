"""What unit values leave a reading session in (ADR 0046 §2, §4, §11).

Every value we hold is in its Variable's source unit. A session converts each
value it returns to the Variable's served unit — the conversion the Variable
keeps as ``scale`` and ``offset`` — or, when the session names an ``in_unit``,
to that unit, by the one positive affine conversion ``source_unit → in_unit``.
A unit the source unit cannot reach is refused before anything is read.
"""

from __future__ import annotations

import threading

from georiva.core.unit_utils import Conversion, ConversionRefused, affine_conversion

#: Raw bytes whose unit no row states, read as they are.
AS_STORED = Conversion()

#: The ``in_unit`` that reads values as they are held — in their Variable's
#: source unit, whatever it is (ADR 0046 §1). For a reader whose arithmetic
#: holds in any unit and that writes its result down in the unit it read in,
#: as a recipe's identity or climatology does. Not a unit pint knows.
SOURCE_UNIT = "source unit"


class Conversions:
    """One session's conversions, worked out once per Variable."""

    def __init__(self, in_unit: str | None):
        self.in_unit = in_unit
        self._by_variable: dict[int, Conversion] = {}
        self._lock = threading.Lock()

    def of(self, variable_id: int | None) -> Conversion:
        """The conversion for values of the Variable ``variable_id``; ``None``
        for bytes no Variable describes, which can only be read as they are."""
        if self.in_unit == SOURCE_UNIT:
            return AS_STORED
        if variable_id is None:
            if self.in_unit is not None:
                raise ConversionRefused(
                    f"These values' unit is not stated anywhere, so they cannot be read in {self.in_unit}. "
                    "Read the asset row, which names its Variable."
                )
            return AS_STORED
        with self._lock:
            found = self._by_variable.get(variable_id)
        if found is None:
            found = self._work_out(variable_id)
            with self._lock:
                self._by_variable[variable_id] = found
        return found

    def _work_out(self, variable_id: int) -> Conversion:
        from georiva.core.models import Variable

        variable = Variable.objects.select_related("source_unit", "unit").get(pk=variable_id)
        return conversion_for(variable, self.in_unit)


def conversion_for(variable, in_unit: str | None = None) -> Conversion:
    """The conversion of ``variable``'s raw values into ``in_unit``, or into its
    served unit when none is named. :class:`ConversionRefused` when ``in_unit``
    cannot be reached from the source unit."""
    if in_unit is None:
        return variable.served_conversion
    return affine_conversion(variable.source_unit.symbol, in_unit, water=variable.water)
