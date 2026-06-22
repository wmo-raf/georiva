"""
Temporal aggregation and anomalies over an xarray time series.

These operate on xarray DataArrays with a ``time`` dimension. Cadence mismatch
(e.g. dekadal inputs → monthly output) is handled here, not in the engine:
the caller pulls all timesteps in a window and aggregates to the output cadence.
"""
from __future__ import annotations

_HOW = {
    "mean": lambda g: g.mean(),
    "sum": lambda g: g.sum(),
    "min": lambda g: g.min(),
    "max": lambda g: g.max(),
}


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
