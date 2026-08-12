"""
Hard heterogeneity guard for Icechunk appends (design decision 7).

Before appending, every new COG's array signature — spatial shape, chunk
shape, dtype, codec pipeline — is compared against the repo's committed
array metadata.  A mismatch fails the build naming the offending asset;
there is no auto-epoch.  Overviews are excluded by construction (``ifd=0``).
"""

from __future__ import annotations

import json
from dataclasses import dataclass


class HeterogeneousAssetError(Exception):
    """A new COG does not match the repo's committed array signature."""


@dataclass(frozen=True)
class ArraySpec:
    """Comparable array signature: last-two (spatial) dims only."""

    shape: tuple[int, ...]
    chunks: tuple[int, ...]
    dtype: str
    codecs: tuple[str, ...]


def assert_compatible(
    existing: ArraySpec,
    candidate: ArraySpec,
    source: str,
) -> None:
    """Raise HeterogeneousAssetError if ``candidate`` diverges from ``existing``."""
    for field in ("shape", "chunks", "dtype", "codecs"):
        expected = getattr(existing, field)
        actual = getattr(candidate, field)
        if expected != actual:
            raise HeterogeneousAssetError(
                f"COG {source} is incompatible with the committed repo array: "
                f"{field} differs (repo={expected!r}, asset={actual!r}). "
                "Heterogeneous assets require a manual rebuild decision."
            )


# ---------------------------------------------------------------------------
# Extractors
# ---------------------------------------------------------------------------


def _serialise_codecs(codecs) -> tuple[str, ...]:
    out = []
    for codec in codecs:
        as_dict = codec if isinstance(codec, dict) else codec.to_dict()
        out.append(json.dumps(as_dict, sort_keys=True, default=str))
    return tuple(out)


def _spec(arr) -> ArraySpec:
    """Spatial-dims signature of any array exposing shape/chunks/dtype/metadata."""
    return ArraySpec(
        shape=tuple(arr.shape[-2:]),
        chunks=tuple(arr.chunks[-2:]),
        dtype=str(arr.dtype),
        codecs=_serialise_codecs(arr.metadata.codecs),
    )


def spec_from_zarr_array(arr) -> ArraySpec:
    """Signature of the repo's committed (time, y, x) array — spatial dims."""
    return _spec(arr)


def spec_from_virtual_variable(vds, name: str) -> ArraySpec:
    """Signature of one virtualized COG's data variable (y, x)."""
    return _spec(vds[name].data)  # virtualizarr ManifestArray
