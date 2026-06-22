"""
Calendar conversion for climate time axes.

CMIP6 models use non-Gregorian calendars (360-day, noleap). To align them to a
standard output cadence the time axis must be converted. Uses xarray's native
``convert_calendar`` (backed by cftime, already in the image) so no xclim
dependency is required for this operation.
"""
from __future__ import annotations


def convert_calendar(obj, calendar: str = "standard", *, align_on: str = "date", missing=None):
    """
    Convert the calendar of an xarray DataArray/Dataset.

    Parameters
    ----------
    obj : xarray.DataArray or xarray.Dataset
        Object with a ``time`` coordinate on some calendar.
    calendar : str
        Target calendar, e.g. ``"standard"`` / ``"proleptic_gregorian"``,
        ``"noleap"``, ``"360_day"``.
    align_on : {"date", "year", None}
        How to map dates when converting to/from 360-day calendars.
    missing
        Fill value inserted for dates that don't exist in the source calendar
        (e.g. Feb 29 when converting from noleap). If None, such dates are
        dropped.
    """
    return obj.convert_calendar(calendar, align_on=align_on, missing=missing)
