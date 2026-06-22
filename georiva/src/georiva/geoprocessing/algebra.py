"""
Raster algebra — element-wise combination of aligned rasters.

Operates on numpy arrays or xarray DataArrays. Inputs are assumed to be
already aligned (same grid); alignment is the caller's job (see ``regrid``).
NaN is the nodata sentinel and propagates through operations.
"""
from __future__ import annotations

import numpy as np

# Re-exported for callers that want the zonal empty-stats sentinel from one place.
from .zonal import EMPTY_STATS  # noqa: F401


def safe_divide(numerator, denominator):
    """
    Element-wise division with divide-by-zero → NaN (not inf).

    Works for numpy arrays and xarray DataArrays.
    """
    with np.errstate(divide="ignore", invalid="ignore"):
        result = numerator / denominator
    # Replace inf/-inf produced by zero denominators with NaN.
    try:
        return result.where(np.isfinite(result))  # xarray
    except AttributeError:
        result = np.asarray(result, dtype="float64")
        result[~np.isfinite(result)] = np.nan
        return result


def raster_combine(*arrays, op="sum", weights=None):
    """
    Combine aligned rasters element-wise.

    Parameters
    ----------
    *arrays
        Two or more aligned numpy arrays / xarray DataArrays.
    op : {"sum", "mean", "min", "max", "product"}
        Reduction applied across the stacked inputs.
    weights : sequence of float, optional
        Per-array weights, only used for ``op="mean"`` (weighted mean).

    Returns the combined raster, with NaN where inputs were NaN per numpy's
    nan-aware reductions (a cell is NaN only if all inputs are NaN).
    """
    if len(arrays) < 2:
        raise ValueError("raster_combine needs at least two arrays")

    stack = np.stack([np.asarray(a, dtype="float64") for a in arrays], axis=0)

    if op == "sum":
        return np.nansum(stack, axis=0)
    if op == "product":
        return np.nanprod(stack, axis=0)
    if op == "min":
        return np.nanmin(stack, axis=0)
    if op == "max":
        return np.nanmax(stack, axis=0)
    if op == "mean":
        if weights is not None:
            w = np.asarray(weights, dtype="float64")
            if len(w) != len(arrays):
                raise ValueError("weights must match number of arrays")
            mask = ~np.isnan(stack)
            wstack = w[:, None, None] * mask
            num = np.nansum(stack * w[:, None, None], axis=0)
            den = np.nansum(wstack, axis=0)
            return safe_divide(num, den)
        return np.nanmean(stack, axis=0)

    raise ValueError(f"unknown op: {op!r}")
