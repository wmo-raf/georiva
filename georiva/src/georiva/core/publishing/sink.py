"""One publication's share of the publications bucket."""

import fnmatch
import json
import logging
import posixpath
from collections.abc import Iterable, Sequence

from .markers import CompletionMarker, MarkerOrderingError

logger = logging.getLogger(__name__)


class PublicationSinkError(RuntimeError):
    """A write did not land where it was asked to."""


class PublicationSink:
    """Everything one publication writes, rooted at ``{org}/{slug}/``.

    The root is the whole tenancy story: a foreign reader is given this prefix
    and nothing above it, so cross-tenant resolution is impossible by
    construction rather than by a check somebody has to remember to write.

    ``marker_patterns`` names the paths that mean "ready" to whatever reads this
    publication — ``fnmatch`` patterns relative to the root, e.g.
    ``("latest/*", "*/complete.json")``. Those paths cannot be written through
    ``write``; they go through ``publish_markers``, which is the engine's door
    and runs after the bytes are staged. A publication that declares no patterns
    has no markers and cannot publish any.

    Usage::

        sink = PublicationSink("kenya", "forti", marker_patterns=("latest/*", "*/complete.json"))
        sink.write(f"{area}/{version}/{grid}/latitude", lat.tobytes())
        ...
        sink.publish_markers(
            [
                CompletionMarker(f"{area}/{version}/complete.json", meta_bytes),
                CompletionMarker(f"latest/{area}", str(version).encode()),
            ],
            require_staged=[f"{area}/{version}/{grid}/latitude"],
        )
    """

    def __init__(
        self,
        organisation_slug: str,
        slug: str,
        *,
        marker_patterns: Sequence[str] = (),
        bucket=None,
    ):
        if not organisation_slug or "/" in organisation_slug:
            raise ValueError(f"organisation_slug must be a single path segment, got {organisation_slug!r}")
        if not slug or "/" in slug:
            raise ValueError(f"slug must be a single path segment, got {slug!r}")

        self.organisation_slug = organisation_slug
        self.slug = slug
        self.marker_patterns = tuple(marker_patterns)
        self._bucket = bucket

    def __repr__(self):
        return f"PublicationSink({self.root!r})"

    # =========================================================================
    # Identity
    # =========================================================================

    @property
    def bucket(self):
        """Lazy so constructing a sink needs no storage backend."""
        if self._bucket is None:
            from georiva.core.storage import storage

            self._bucket = storage.publications
        return self._bucket

    @property
    def root(self) -> str:
        """The prefix a reader is pointed at. Trailing slash: it names a prefix."""
        return f"{self.organisation_slug}/{self.slug}/"

    def key(self, relpath: str) -> str:
        """Absolute bucket key for a path relative to this publication.

        Refuses anything that would leave the root — a publication that can
        write one segment up is a publication that can write into another
        organisation's prefix.
        """
        if not relpath or relpath.startswith("/"):
            raise ValueError(f"Path must be relative and non-empty, got {relpath!r}")

        normalised = posixpath.normpath(relpath)
        if normalised.startswith("..") or normalised == ".":
            raise ValueError(f"Path escapes the publication root: {relpath!r}")

        return f"{self.root}{normalised}"

    def is_marker(self, relpath: str) -> bool:
        """Whether this path is one of the publication's completion markers."""
        return any(fnmatch.fnmatch(relpath, pattern) for pattern in self.marker_patterns)

    # =========================================================================
    # Writing
    # =========================================================================

    def write(self, relpath: str, content: bytes) -> str:
        """Stage one object. Refuses completion markers.

        Returns the absolute key written.
        """
        if self.is_marker(relpath):
            raise MarkerOrderingError(
                f"{relpath!r} is a completion marker for {self.root!r}. Markers are "
                f"written by the engine after the bytes they promise, through "
                f"publish_markers() — never by the writer."
            )
        return self._put(self.key(relpath), content)

    def write_json(self, relpath: str, payload) -> str:
        """Stage one JSON object, encoded the way a foreign reader wants it:
        UTF-8, no ASCII escaping, sorted keys so an unchanged document produces
        unchanged bytes."""
        return self.write(relpath, _encode_json(payload))

    def publish_markers(
        self,
        markers: Iterable[CompletionMarker],
        *,
        require_staged: Sequence[str] = (),
    ) -> list[str]:
        """Write the completion markers, in order, after everything else.

        The engine's door, and the only one markers fit through. Each marker is
        fully written before the next is attempted, because their order is
        itself a promise: a reader that finds the pointer will follow it to the
        manifest, so the manifest goes first.

        ``require_staged`` names objects that must already exist for the markers
        to be true. Checking is a HEAD per path, so pass a representative few —
        the first and last object of the batch, say — rather than everything.
        """
        markers = list(markers)
        if not markers:
            return []

        for marker in markers:
            if not self.is_marker(marker.path):
                raise MarkerOrderingError(
                    f"{marker.path!r} is not a declared completion marker for {self.root!r} "
                    f"(patterns: {self.marker_patterns}). publish_markers writes markers "
                    f"only — stage ordinary objects with write()."
                )

        for relpath in require_staged:
            if not self.exists(relpath):
                raise MarkerOrderingError(
                    f"Refusing to publish markers for {self.root!r}: {relpath!r} is not "
                    f"staged. A marker over missing bytes is a reader loading a partial "
                    f"dataset with nothing to tell it so."
                )

        written = []
        for marker in markers:
            written.append(self._put(self.key(marker.path), marker.content))
            logger.info("published marker %s", written[-1])
        return written

    def _put(self, key: str, content: bytes) -> str:
        """Write bytes at exactly ``key``, replacing whatever was there.

        Django storage backends disagree about overwriting: S3 obeys
        ``file_overwrite`` (true for every GeoRiva bucket), while
        ``FileSystemStorage`` always renames rather than replace. A publication's
        keys are derived, not allocated — ``latest/<area>`` is rewritten every
        run and a reader polls that exact name — so a rename is a silent
        failure. Delete first where the backend needs it, and refuse the result
        if the key still moved.
        """
        storage = self.bucket.storage
        if not getattr(storage, "file_overwrite", False) and storage.exists(key):
            storage.delete(key)

        saved = self.bucket.save(key, content)
        if saved != key:
            # Do not leave the renamed copy behind: it is unreachable by name
            # and would survive every retention pass.
            self.bucket.delete(saved)
            raise PublicationSinkError(
                f"Storage wrote {saved!r} instead of {key!r}. A publication's keys are "
                f"derived and polled by name, so a rename loses the object."
            )
        return saved

    # =========================================================================
    # Reading and pruning
    # =========================================================================

    def exists(self, relpath: str) -> bool:
        return self.bucket.exists(self.key(relpath))

    def read_bytes(self, relpath: str) -> bytes:
        return self.bucket.read_bytes(self.key(relpath))

    def read_json(self, relpath: str):
        return json.loads(self.read_bytes(relpath).decode("utf-8"))

    def list_keys(self, relpath: str = "", recursive: bool = True) -> list[str]:
        """Paths under ``relpath``, relative to the publication root."""
        prefix = self.key(relpath) if relpath else self.root.rstrip("/")
        return [_strip_root(key, self.root) for key in self.bucket.list_keys(prefix, recursive=recursive)]

    def children(self, relpath: str = "") -> list[str]:
        """Immediate child names under ``relpath`` that actually hold objects.

        How a retention pass enumerates the versions of an area: the layout puts
        each version under its own path segment, so this is the version list.

        Derived from the keys rather than from a directory listing, because an
        object store has no directories — and on a backend that does, a prefix
        whose objects have all been deleted leaves an empty directory behind,
        which would then be counted as a version that still exists. One listing
        either way.
        """
        offset = len(relpath) + 1 if relpath else 0
        names = set()
        for key in self.list_keys(relpath, recursive=True):
            head, separator, _ = key[offset:].partition("/")
            if separator:
                names.add(head)
        return sorted(names)

    def delete(self, relpath: str) -> bool:
        return self.bucket.delete(self.key(relpath))

    def delete_prefix(self, relpath: str, *, include_markers: bool = False) -> int:
        """Delete everything under ``relpath``; returns the object count.

        Retention's tool, and by default it leaves completion markers alone: a
        marker names something a reader may be following right now, and removing
        one turns that reader's next poll into a missing-version error rather
        than a stale answer.

        ``include_markers=True`` deletes them too, which is what dropping a
        superseded version whole requires — its own manifest is a marker, and a
        version directory that cannot lose it never goes away. Only for a
        prefix the caller has established nothing points at any more; the
        pointer itself (and the version it names) is never that prefix.
        """
        removed = 0
        for key in self.list_keys(relpath, recursive=True):
            if self.is_marker(key) and not include_markers:
                logger.warning(
                    "delete_prefix(%r) skipped completion marker %s — a reader may still be "
                    "following it. Pass include_markers=True to drop a superseded version whole.",
                    relpath,
                    key,
                )
                continue
            if self.delete(key):
                removed += 1
        return removed


def _encode_json(payload) -> bytes:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")


def _strip_root(key: str, root: str) -> str:
    return key[len(root) :] if key.startswith(root) else key
