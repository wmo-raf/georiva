"""
A harvested asset's `extra_fields` is written by more than one hand: a harvest
lifts the Upstream's metadata into it, and a materialization notes there why it
was refused. Each writes only its own keys, and leaves the rest as the database
holds it at the write — never as it was read a moment earlier, which would undo
whatever the other hand wrote in between (#724).
"""

from __future__ import annotations

import json
from collections.abc import Iterable

from django.db.models.expressions import RawSQL


def rewritten(*, drop: Iterable[str], put: dict | None = None) -> RawSQL:
    """`extra_fields` as an `update()` value: `drop`'s keys removed, then `put`'s merged in."""
    return RawSQL("(extra_fields - %s::text[]) || %s::jsonb", (list(drop), json.dumps(put or {})))
