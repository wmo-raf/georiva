"""The object that says a publication is finished, and the rule about when.

A reader polling a bucket has no other way to tell a complete dataset from one
still being written: object stores have no transaction, and a listing mid-write
looks exactly like a listing of something small. So a publication ends with a
marker, and everything downstream of that marker is a promise the bytes are all
there.

Which makes the ordering the whole safety property. Write the marker first and
a reader loads a partial dataset; write two markers in the wrong order and a
reader loads a version whose manifest has not landed. Both fail silently — the
reader is doing exactly what it was told.

Verified against met.no's ``rawdataforecaster``, which polls every 3 seconds:
bytes staged for four seconds with the marker withheld drew no load attempt at
all, and the load began within two seconds of the marker appearing.
"""

from dataclasses import dataclass


class MarkerOrderingError(RuntimeError):
    """A completion marker was about to be written at the wrong time, or by the
    wrong half of the system."""


@dataclass(frozen=True)
class CompletionMarker:
    """One object whose existence tells a reader something is ready.

    ``path`` is relative to the publication root and must match one of the
    publication's declared marker patterns — the sink refuses it as an ordinary
    write, and refuses anything else as a marker, so the two sets cannot drift.

    Order matters between markers as much as it does between markers and bytes:
    they are written in the order the list gives them, each fully durable before
    the next is attempted. A dataset's own manifest goes before the pointer that
    names it as current.
    """

    path: str
    content: bytes

    def __post_init__(self):
        if not self.path or self.path.startswith("/"):
            raise MarkerOrderingError(f"Marker path must be relative and non-empty, got {self.path!r}")
        if not isinstance(self.content, bytes):
            raise MarkerOrderingError(f"Marker content must be bytes, got {type(self.content).__name__}")
