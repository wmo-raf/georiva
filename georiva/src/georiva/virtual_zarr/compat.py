"""
Which COGs a Variable's virtual Zarr holds (#732).

A repo holds one array: every timestep shares its spatial shape, chunk shape,
dtype and codec pipeline, and has one band — virtual chunk references cannot
be cast, re-chunked or cut to a band. A Variable's local COGs need not agree:
a Materialized copy keeps its upstream file's own dtype and every band
(ADR 0042 §8, #725) where copies made before it were `float32`, and a clip
that changes a raster's dimensions changes its chunks.

So the newest single-band asset's signature is the one held. Every asset of
another signature, and every multi-band asset, is left out of the repo with a
reason, recorded on the commit and shown wherever the manifest is. Nothing is
refused: the asset is still written and served, only not virtualized.
Overviews are excluded by construction (``ifd=0``).

Pure functions over signatures — no I/O — so the choice is unit-testable.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class ArraySpec:
    """Comparable array signature: last-two (spatial) dims, and the band count."""

    shape: tuple[int, ...]
    chunks: tuple[int, ...]
    dtype: str
    codecs: tuple[str, ...]
    bands: int = 1


@dataclass(frozen=True)
class LeftOut:
    """Assets left out of a repo, all for one reason."""

    reason: str
    count: int


@dataclass(frozen=True)
class Selection:
    """What a full build holds: the signature, the positions kept, the rest."""

    signature: ArraySpec | None
    kept: tuple[int, ...]
    left_out: tuple[LeftOut, ...]


def select(specs: Sequence[ArraySpec]) -> Selection:
    """
    Choose what a rebuild holds from every asset's signature, oldest first.

    The newest single-band asset sets the signature; ``signature`` is None
    when there is none, and then nothing is held.
    """
    single = [spec for spec in specs if spec.bands == 1]
    signature = single[-1] if single else None

    kept = []
    reasons = Counter()
    for position, spec in enumerate(specs):
        if spec == signature:
            kept.append(position)
        else:
            reasons[_reason(spec, signature)] += 1

    left_out = tuple(LeftOut(reason, count) for reason, count in reasons.most_common())
    return Selection(signature=signature, kept=tuple(kept), left_out=left_out)


def appends_cleanly(committed: ArraySpec, specs: Sequence[ArraySpec]) -> bool:
    """Whether new assets can all be appended to a repo holding ``committed``.

    When not, the build rebuilds instead: the newest signature may have
    changed, and an append never leaves anything out."""
    return all(spec == committed for spec in specs)


def _reason(spec: ArraySpec, signature: ArraySpec | None) -> str:
    if spec.bands != 1:
        return f"{spec.bands} bands — the virtual Zarr holds single-band arrays only"
    differs = []
    for field in ("shape", "chunks"):
        mine, held = getattr(spec, field), getattr(signature, field)
        if mine != held:
            differs.append(f"{field} {_dims(mine)}, not {_dims(held)}")
    if spec.dtype != signature.dtype:
        differs.append(f"dtype {spec.dtype}, not {signature.dtype}")
    if spec.codecs != signature.codecs:
        differs.append("a different compression")
    return "; ".join(differs) + " (the newest single-band asset's)"


def _dims(dims: tuple[int, ...]) -> str:
    return "×".join(str(d) for d in dims)


def left_out_to_json(left_out: Sequence[LeftOut]) -> list[dict]:
    return [{"reason": entry.reason, "count": entry.count} for entry in left_out]


def left_out_from_json(value) -> tuple[LeftOut, ...]:
    """The record as stored; nothing from a commit that predates it."""
    return tuple(LeftOut(entry["reason"], int(entry["count"])) for entry in value or ())


def left_out_count(left_out: Sequence[LeftOut]) -> int:
    return sum(entry.count for entry in left_out)


def describe_left_out(left_out: Sequence[LeftOut]) -> str:
    return "; ".join(f"{entry.count} × {entry.reason}" for entry in left_out)


def _serialise_codecs(codecs) -> tuple[str, ...]:
    out = []
    for codec in codecs:
        as_dict = codec if isinstance(codec, dict) else codec.to_dict()
        out.append(json.dumps(as_dict, sort_keys=True, default=str))
    return tuple(out)


def _spec(arr, *, leading: int) -> ArraySpec:
    """Signature of an array of ``leading`` non-spatial dims before (band?, y, x)."""
    return ArraySpec(
        shape=tuple(arr.shape[-2:]),
        chunks=tuple(arr.chunks[-2:]),
        dtype=str(arr.dtype),
        codecs=_serialise_codecs(arr.metadata.codecs),
        bands=arr.shape[leading] if len(arr.shape) == leading + 3 else 1,
    )


def spec_from_zarr_array(arr) -> ArraySpec:
    """Signature of the repo's committed (time, y, x) array."""
    return _spec(arr, leading=1)


def spec_from_virtual_variable(vds) -> ArraySpec:
    """Signature of one virtualized COG — (y, x), or (band, y, x) for several bands."""
    return _spec(vds[next(iter(vds.data_vars))].data, leading=0)  # virtualizarr ManifestArray
