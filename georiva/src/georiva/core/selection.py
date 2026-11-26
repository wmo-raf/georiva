"""A **Selection**: one value of each extra axis an Upstream store array is read at (wmo-raf/georiva#830).

A Map-shaped store can hold more than one map per step — one per ensemble
member, per statistic, per pressure level. Each such map is one chunk, and is
read as one field: the array at one fixed value of each extra axis. A Variable
mapped from such an array names that value, ``{"statistic": "mean"}`` or
``{"ensemble_member": 0}``, and the same array may be mapped more than once at
different values.

A Selection holds the **value**, never an index: an Upstream that inserts a
member or a level does not shift it. Every reader resolves it to an index
against the store's own coordinate when it opens the store (:func:`index_of`),
and a value that is gone is a misdescribed store, never a blank map.

Within a harvest, a field — an array at a Selection — is known by its
**field key** (:func:`field_key`): the array's path alone when nothing is
selected, so an array with no extra axis is known as it always was. Titiler
keeps its own copy of the matching rule (``titiler-app/app/store.py``).
"""

from __future__ import annotations

import json
from collections.abc import Mapping

import numpy as np

#: One value of an extra axis: text or a number, as JSON carries it.
Value = str | int | float

#: A Selection, hashable: each axis's name with its value, in order of name.
Selection = tuple[tuple[str, Value], ...]

#: What opens a field key's Selection: no array path in a store has it.
_OPENS = "{"


class SelectionInvalid(ValueError):
    """What was given is not a Selection: a mapping of axis names to text or numbers."""


def selection_of(given: Mapping | None) -> Selection:
    """`given` as a :data:`Selection`; empty for nothing. Raises :class:`SelectionInvalid`."""
    if given is None:
        return ()
    if not isinstance(given, Mapping):
        raise SelectionInvalid("A selection is a mapping of axis names to values.")
    for name, value in given.items():
        if not isinstance(name, str) or not name:
            raise SelectionInvalid("A selection names each axis by its name.")
        if isinstance(value, bool) or not isinstance(value, str | int | float):
            raise SelectionInvalid(f"The value of “{name}” is neither text nor a number.")
    return tuple(sorted(given.items()))


def as_dict(selection: Selection) -> dict[str, Value]:
    """A Selection as it is stored and published: a JSON object."""
    return dict(selection)


def chooses(selection: Mapping, axes: Mapping[str, list]) -> bool:
    """Whether `selection` names one of the listed values of each of `axes` —
    each extra axis by name, with its values — and nothing else."""
    return selection.keys() == axes.keys() and all(selection[name] in values for name, values in axes.items())


def field_key(path: str, selection: Selection) -> str:
    """What one field of a store is known by in a harvest: its array's path, and its Selection after it."""
    if not selection:
        return path
    return path + json.dumps(as_dict(selection), separators=(",", ":"), sort_keys=True)


def split_field_key(key: str) -> tuple[str, Selection]:
    """The array path and Selection a :func:`field_key` names."""
    path, opens, rest = key.partition(_OPENS)
    return path, selection_of(json.loads(opens + rest)) if opens else ()


def index_of(coordinate: np.ndarray, value: Value) -> int | None:
    """Where `value` is along an extra axis, from the store's own `coordinate`;
    ``None`` when it is not there. A number is found among numbers, and text
    among text: ``"0"`` is not member 0."""
    values = np.asarray(coordinate).ravel()
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        if values.dtype.kind not in "iuf":
            return None
        found = np.flatnonzero(values == value)
    elif isinstance(value, str):
        if values.dtype.kind not in "USOT":
            return None
        found = [at for at, each in enumerate(values) if _text(each) == value]
    else:
        return None
    return int(found[0]) if len(found) else None


def _text(each) -> str:
    return each.decode("utf-8", "replace") if isinstance(each, bytes) else str(each)


def described(selection: Selection, *, units: Mapping[str, str] | None = None) -> str:
    """A Selection in an operator's words: ``ensemble member 0, pressure level 850 hPa``."""
    units = units or {}
    said = []
    for name, value in selection:
        unit = units.get(name, "")
        shown = f"{value:g}" if isinstance(value, float) else str(value)
        said.append(" ".join(part for part in (name.replace("_", " "), shown, unit if unit != "1" else "") if part))
    return ", ".join(said)
