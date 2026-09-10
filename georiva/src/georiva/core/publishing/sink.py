"""One publication's share of the publications bucket."""

import fnmatch
import json
import logging
import posixpath
from collections.abc import Iterable, Sequence

from georiva.organisations.validators import ORG_SLUG_RE

from .markers import CompletionMarker, MarkerOrderingError

logger = logging.getLogger(__name__)


class PublicationSinkError(RuntimeError):
    """A write did not land where it was asked to."""


class PublicationSink:
    """Everything one publication writes, rooted at a prefix and unable to leave it.

    Two roots exist, and they differ in exactly one thing: whether the root is
    also the tenancy boundary.

    **``{org}/{slug}/`` — one organisation's publication.** The root is the whole
    tenancy story. A foreign reader is given this prefix and nothing above it, so
    cross-tenant resolution is impossible by construction rather than by a check
    somebody has to remember to write. Build it with the constructor.

    **``{root}/`` — one publication for the whole instance.** For a reader that
    is meant to hold several organisations' data at once, which is a real thing
    to want: a reader with a per-request filter answers for every tenant from one
    process, and giving it a prefix per organisation would mean a process per
    organisation. Build it with :meth:`instance_wide`.

    The second form gives up the sentence above, and nothing here can give it
    back: the prefix spans tenants on purpose, so the boundary moves out to
    whatever decides which tenant a request may be answered from, and that is the
    caller's to enforce and to test. Two weaker guarantees remain, and they are
    the reason this is a constructor rather than a bare prefix string:

    * The root cannot be spelled as an organisation slug, so it can neither
      shadow a tenant's prefix nor be silently inherited by a tenant created
      later. ``_shared`` is safe because ``_`` is outside the slug grammar.
    * ``delete_prefix`` refuses to take the whole root, which under this form is
      every organisation's data rather than one publication's.

    Everything else — key derivation, the escape check, the marker rules — is
    written against ``root`` and behaves identically under both.

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
        organisation_slug: str | None,
        slug: str,
        *,
        marker_patterns: Sequence[str] = (),
        bucket=None,
    ):
        """``organisation_slug`` of ``None`` roots the sink at ``slug`` alone.

        Prefer :meth:`instance_wide` for that: it says so at the call site, where
        a bare ``None`` reads like an oversight. The rule it enforces lives here
        rather than there, because a constructor nothing validates is a way round
        it.
        """
        if organisation_slug is None:
            _validate_instance_root(slug)
        else:
            _validate_organisation_slug(organisation_slug)
        _validate_segment(slug, "slug")

        self._organisation_slug = organisation_slug
        self._slug = slug
        # Derived once, at the only point the parts are checked. Deriving it on
        # each access would leave the guarantee resting on the attributes
        # staying as they were validated, and `sink.organisation_slug = None`
        # would silently turn an organisation's publication into an
        # instance-wide one rooted at its publication slug.
        self._root = f"{slug}/" if organisation_slug is None else f"{organisation_slug}/{slug}/"

        self.marker_patterns = tuple(marker_patterns)
        self._bucket = bucket

    @classmethod
    def instance_wide(
        cls,
        root: str,
        *,
        marker_patterns: Sequence[str] = (),
        bucket=None,
    ) -> "PublicationSink":
        """A sink rooted at ``{root}/``, holding every organisation's share.

        See the class docstring for what this gives up. ``root`` must be a name
        no organisation can have.
        """
        return cls(None, root, marker_patterns=marker_patterns, bucket=bucket)

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
    def organisation_slug(self) -> str | None:
        """The owning organisation, or None on an instance-wide sink.

        Read-only, with `slug` and `root`: together they are the validated
        identity, and a sink that could be re-rooted after construction would
        have been validated as something it no longer is.
        """
        return self._organisation_slug

    @property
    def slug(self) -> str:
        return self._slug

    @property
    def is_instance_wide(self) -> bool:
        """Whether this root spans organisations rather than bounding one."""
        return self._organisation_slug is None

    @property
    def root(self) -> str:
        """The prefix a reader is pointed at. Trailing slash: it names a prefix."""
        return self._root

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

        That reading holds for a ``relpath`` the caller knows names an area. It
        does **not** hold at the root of an instance-wide sink, where the
        children are whatever the publisher put there — every organisation's
        areas, and any documents it keeps beside them. Core cannot tell those
        apart, because which names are areas is the publisher's grammar and not
        core's; a caller enumerating areas must know its own layout.

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

        An empty ``relpath`` means the root. On an org-rooted sink that is one
        publication, which is a thing a caller may legitimately drop. On an
        instance-wide one it is every organisation's, which no retention pass
        wants and which nothing downstream would report — the readers would
        simply stop finding data.

        That refusal is a backstop and not a boundary. On an instance-wide sink
        *any* prefix may be shared — a publisher's pointer directory or its
        config is as instance-wide as the root itself — and core cannot tell
        which, because the layout under the root is the publisher's. What used
        to cost one organisation now costs all of them, and only the caller
        knows the difference.
        """
        if not relpath and self.is_instance_wide:
            raise ValueError(
                f"Refusing to delete the whole of {self.root!r}: this root is shared by every "
                f"organisation publishing here, so an empty relpath is all of their data and "
                f"not one publication's. Name the area or version to drop."
            )

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


def _validate_segment(value: str, what: str) -> None:
    if not value or "/" in value:
        raise ValueError(f"{what} must be a single path segment, got {value!r}")


def _validate_organisation_slug(organisation_slug: str) -> None:
    """Refuse an organisation segment that no organisation could be called.

    The mirror of the rule below, and needed for the same reason read the other
    way: without it ``PublicationSink("_shared", "central")`` roots a supposedly
    org-owned publication *inside* the instance-wide prefix, where a shared
    reader would serve it as though somebody had published it there.

    Grammar only, again: the reserved-name list can grow, and an organisation
    that already holds a name must not lose its sink the day that name is
    reserved.
    """
    _validate_segment(organisation_slug, "organisation_slug")
    if not ORG_SLUG_RE.match(organisation_slug):
        raise ValueError(
            f"organisation_slug {organisation_slug!r} is not a name an organisation can have "
            f"(grammar {ORG_SLUG_RE.pattern!r}). Roots outside that grammar are instance-wide; "
            f"build one with PublicationSink.instance_wide()."
        )


def _validate_instance_root(root: str) -> None:
    """Refuse an instance-wide root that an organisation could also be called.

    This is what is left of ADR 0027's construction argument once the root stops
    being one organisation's. A root of ``kenya`` *is* organisation ``kenya``'s
    prefix: a publication rooted there writes into a tenant's own space today,
    and a tenant registered tomorrow inherits a prefix already full of somebody
    else's data — in both directions silently, since neither side is looking.

    Tested against the slug grammar rather than against the organisations that
    happen to exist, because the collision that matters is with the one created
    after this root was chosen. The reserved-name list is deliberately not
    consulted: it can shrink, and a root that was safe must not stop being so.
    """
    _validate_segment(root, "root")
    if ORG_SLUG_RE.match(root):
        raise ValueError(
            f"{root!r} can be an organisation slug, so an instance-wide root of that name is "
            f"some organisation's own prefix — one that exists now, or one created later. "
            f"Choose a name outside the slug grammar {ORG_SLUG_RE.pattern!r}; a leading "
            f"underscore (e.g. '_shared') is the obvious way."
        )


def _encode_json(payload) -> bytes:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")


def _strip_root(key: str, root: str) -> str:
    return key[len(root) :] if key.startswith(root) else key
