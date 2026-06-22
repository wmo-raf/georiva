"""
Temporal aggregation, seasonal climatologies, anomalies, and trends over an
xarray time series.

These operate on xarray DataArrays with a ``time`` dimension. Cadence mismatch
(e.g. dekadal inputs → monthly output) is handled here, not in the engine:
the caller pulls all timesteps in a window and aggregates to the output cadence.

Season selection reads the calendar month from the series' own time coordinate,
so non-standard calendars (e.g. CMIP6 360-day) need no special-casing — the
Climatology recipe family computes ``value``/``anomaly``/``relative anomaly``
per season by composing :func:`climatology` and :func:`anomaly`, and the
``trend`` quantity via :func:`trend`.
"""
from __future__ import annotations

_HOW = {
    "mean": lambda g: g.mean(),
    "sum": lambda g: g.sum(),
    "min": lambda g: g.min(),
    "max": lambda g: g.max(),
}

# Standard meteorological 3-month seasons, by calendar month number. ``annual``
# (or None) selects every month. Months are read from the series' own time
# coordinate, so this works on any calendar (Gregorian, 360-day, …).
SEASONS = {
    "DJF": (12, 1, 2),
    "MAM": (3, 4, 5),
    "JJA": (6, 7, 8),
    "SON": (9, 10, 11),
}


def select_season(da, season, time_dim: str = "time"):
    """
    Filter a time series to the timesteps belonging to ``season``.

    ``season`` is a key of :data:`SEASONS` (e.g. ``"DJF"``), or ``"annual"`` /
    ``None`` to keep every timestep. Selection is by calendar month taken from
    the series' time coordinate, so non-standard calendars (e.g. CMIP6 360-day)
    are handled the same way.
    """
    if season is None or season == "annual":
        return da
    if season not in SEASONS:
        raise ValueError(f"unknown season: {season!r}")
    months = da[time_dim].dt.month
    return da.sel({time_dim: months.isin(SEASONS[season])})


def temporal_aggregate(da, freq: str | None = None, how: str = "mean", time_dim: str = "time"):
    """
    Aggregate a time series along ``time_dim``.

    Parameters
    ----------
    da : xarray.DataArray
        Series with a time dimension.
    freq : str, optional
        A pandas/xarray resample frequency (e.g. ``"MS"``, ``"YS"``). If None,
        collapses the whole series to a single value.
    how : {"mean", "sum", "min", "max"}
        Reduction.
    """
    if how not in _HOW:
        raise ValueError(f"unknown how: {how!r}")

    if freq is None:
        reducer = getattr(da, how)
        return reducer(dim=time_dim)

    grouped = da.resample({time_dim: freq})
    return _HOW[how](grouped)


def climatology(da, season=None, how: str = "mean", time_dim: str = "time"):
    """
    Climatological statistic of a series over a season.

    Selects the timesteps in ``season`` (see :func:`select_season`) and collapses
    the whole window to a single value with ``how``. This is the ``value``
    quantity for the Climatology recipe family; ``anomaly`` is computed against a
    second baseline-window climatology.
    """
    selected = select_season(da, season, time_dim=time_dim)
    return temporal_aggregate(selected, freq=None, how=how, time_dim=time_dim)


def trend(da, season=None, how: str = "mean", time_dim: str = "time"):
    """
    Inter-annual linear trend (slope **per year**) of a seasonal series.

    Selects the timesteps in ``season`` (see :func:`select_season`), reduces each
    calendar year to a single value with ``how``, then fits a degree-1 polynomial
    of value vs. year. The slope is returned in per-year units (the year is taken
    from the time coordinate, so any calendar works).
    """
    if how not in _HOW:
        raise ValueError(f"unknown how: {how!r}")
    selected = select_season(da, season, time_dim=time_dim)
    yearly = _HOW[how](selected.groupby(selected[time_dim].dt.year))
    fit = yearly.polyfit(dim="year", deg=1)
    return fit.polyfit_coefficients.sel(degree=1, drop=True)


def anomaly(da, baseline, relative: bool = False, how: str = "mean", time_dim: str = "time"):
    """
    Anomaly of a series (or value) against a baseline period.

    ``baseline`` may be a DataArray series (reduced to its climatological mean
    via ``how``) or an already-reduced DataArray / scalar. With
    ``relative=True`` returns ``(value - baseline) / baseline`` (relative
    anomaly); otherwise ``value - baseline``.
    """
    base = baseline
    if hasattr(baseline, "dims") and time_dim in getattr(baseline, "dims", ()):
        base = temporal_aggregate(baseline, freq=None, how=how, time_dim=time_dim)

    diff = da - base
    if relative:
        from .algebra import safe_divide
        return safe_divide(diff, base)
    return diff
