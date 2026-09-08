"""Writing GeoRiva's data out in somebody else's format.

A *publication* is a re-export: the same data GeoRiva already serves, laid out
for a reader that knows nothing about STAC, COGs or tiles. It goes to its own
bucket under ``{org}/{slug}/``, and the org-first prefix is not decoration — a
foreign service is pointed at one organisation's prefix and is then unable to
resolve anything outside it.

Two things live here, and only two. ``PublicationSink`` is the bucket handle
with that grammar built in. ``CompletionMarker`` is how the ordering rule below
is expressed. What a publication *contains* is not core's business: the format,
the derivations and the schedule belong to whatever plugin publishes it.

**Completion markers go last, and the writer never writes them.** A foreign
reader polls for a marker and loads whatever the marker points at, so a marker
that appears before the bytes it names is a reader loading a half-written
dataset — with no error anywhere, because from the reader's side the marker was
a promise. The sink therefore refuses to write any path matching a publication's
declared marker patterns, and ``publish_markers`` is a separate door that writes
them in the order given, after everything else is staged.
"""

from .markers import CompletionMarker, MarkerOrderingError
from .sink import PublicationSink, PublicationSinkError

__all__ = [
    "CompletionMarker",
    "MarkerOrderingError",
    "PublicationSink",
    "PublicationSinkError",
]
