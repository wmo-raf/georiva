"""Writing GeoRiva's data out in somebody else's format.

A *publication* is a re-export: the same data GeoRiva already serves, laid out
for a reader that knows nothing about STAC, COGs or tiles. It goes to its own
bucket, under one of two roots.

``{org}/{slug}/`` is the org-first prefix used everywhere else in GeoRiva, and
here it is not decoration — a foreign service pointed at it is unable to resolve
anything outside one organisation, so the root *is* the tenancy boundary.

``{root}/`` is one prefix for the whole instance, for a reader built to hold
several organisations at once. It exists because the alternative is a process per
organisation, and a reader that can already filter per request does not need one.
It is the exception to "the first segment of every key is the owning
organisation's slug", and it is deliberate: the root spans tenants, so the
boundary moves out to whatever decides which tenant a request may be answered
from. Core keeps only what it can still guarantee by construction — such a root
must be unspellable as an organisation slug, so it can never shadow a tenant or
be inherited by one registered later.

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
