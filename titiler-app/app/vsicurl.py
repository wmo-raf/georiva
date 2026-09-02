"""GDAL's memory of a failed read, and how to make it forget one.

A ``/vsicurl/`` read that fails is not merely a failed read. GDAL records the
outcome — *this object does not exist*, *this object answered 503* — in a
per-process cache of file properties, and answers every later open of the same
URL out of it without going near storage. For an archive of objects that were
written long ago that is the right economy. For ours it is exactly wrong: the
object a request races is written seconds later, and until then every worker
that served one such request answers for that path from a lie it is holding,
for the life of the process (#400).

GDAL offers no way to bound how long such an entry lives, and the one option
that would stop it being written — ``CPL_VSIL_CURL_NON_CACHED`` — also disables
the chunk cache that makes tiling affordable. What it does offer is a C entry
point that drops the entries under one prefix, and this module is the whole of
what it takes to reach it: everywhere else says ``vsicurl.forget(url)`` and
knows nothing of the mechanism.

The complement to this is ``GDAL_DISABLE_READDIR_ON_OPEN``, pinned in
``app.config`` and depended on by ``app.reader``: without it GDAL lists the
*directory* on every open and caches that listing too, which is a second way
for a sibling written afterwards to stay invisible.
"""

import ctypes
import functools
import logging
import pathlib
import re

logger = logging.getLogger(__name__)

#: A path in ``/proc/self/maps`` naming a mapped libgdal. Anchored on the
#: leading slash so it cannot start mid-token: every field before the path on
#: such a line is separated from it by whitespace.
_MAPPED_LIBGDAL = re.compile(r"/\S*libgdal\S*")


def _candidate_libraries():
    """Where the libgdal this process actually reads through might be found.

    Order matters, and so does *not* falling back to a plain ``libgdal.so``.
    An image can easily carry a second GDAL — the system one under
    ``/usr/lib``, or the one behind ``osgeo`` — whose caches nothing here reads.
    Clearing that one would leave the bug in place and the logs quiet, which is
    worse than not trying, so the library is located by where it is already
    mapped and never by name alone.
    """
    # Nothing below can find a library that is not loaded yet, and importing
    # rasterio is what loads it. Cheap after the first time, and the ``None``
    # candidate depends on it just as much as the mapped ones do.
    import rasterio  # noqa: F401

    # Symbols already in the process's global table: macOS's flat namespace,
    # or a GDAL linked into the interpreter. Cheapest, and correct when it
    # works.
    yield None

    # Linux: CPython opens extension modules RTLD_LOCAL, so the libgdal
    # bundled in rasterio's wheel is invisible above. It is however mapped,
    # and re-opening a mapped file by its own path hands back the handle that
    # already holds its state rather than a second copy of it.
    try:
        maps = pathlib.Path("/proc/self/maps").read_text()
    except OSError:
        return
    for path in sorted(set(_MAPPED_LIBGDAL.findall(maps))):
        if pathlib.Path(path).is_file():
            yield path


@functools.cache
def _partial_clear_cache():
    """``VSICurlPartialClearCache``, bound to the right libgdal, or ``None``.

    Resolved once. A build this cannot reach is a build where a poisoned path
    stays poisoned until the worker recycles, so it is reported at ``error``
    once rather than per request — quiet degradation is what made #400 take a
    verification run to notice.
    """
    for library in _candidate_libraries():
        try:
            symbol = ctypes.CDLL(library).VSICurlPartialClearCache
        except (OSError, AttributeError):
            continue
        symbol.argtypes = [ctypes.c_char_p]
        symbol.restype = None
        logger.info(
            "vsicurl cache recovery armed against %s",
            library or "the process symbol table",
        )
        return symbol

    logger.error(
        "VSICurlPartialClearCache is unreachable — a storage read that fails "
        "will stay failed for this worker's lifetime (see #400)"
    )
    return None


def _vsi_name(path: str) -> str | None:
    """``path`` as GDAL filed it, or ``None`` if GDAL caches nothing for it.

    rasterio hands GDAL ``/vsicurl/`` + the URL, and the cache is keyed on that
    whole string. A local filesystem path — what the test harness reads through
    — is not something GDAL can be holding a stale answer about, so it is
    reported as nothing to do rather than as a failure.
    """
    if path.startswith(("http://", "https://")):
        return f"/vsicurl/{path}"
    return None


def forget(path: str) -> bool:
    """Drop what GDAL remembers about ``path``, so the next read asks storage.

    ``path`` is the dataset address as this app hands it to rasterio. Returns
    whether GDAL was actually asked to forget something: ``False`` means there
    was nothing cached to drop (a local path) or no way to drop it, and either
    way the caller should not expect a retry to answer differently.
    """
    name = _vsi_name(path)
    if name is None:
        return False

    clear = _partial_clear_cache()
    if clear is None:
        return False

    clear(name.encode())
    logger.debug("Dropped GDAL's cached lookup for %s", name)
    return True
