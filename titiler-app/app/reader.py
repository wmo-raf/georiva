"""How this service opens a COG, and what it makes of a failure to open one.

Every raster route here — the tile factory's, the encoded texture, the point
read GetFeatureInfo goes through — reaches storage by opening a URL this
service built from the request path (see ``dependencies.build_cog_url``). No
Asset row is consulted on the way, so a client is free to ask for a timestep
that has not landed yet, and clients do: a viewer prefetches the next step, a
verification script follows a STAC link the instant it appears.

That is a fine thing to answer *not yet* to. What it must not do is make the
answer permanent, which is what happens when GDAL is left to cache the failed
lookup (see :mod:`app.vsicurl`). So a failed open is always followed by
dropping GDAL's memory of it, and the reader here is the one place that
happens — it is the seam every route already shares.
"""

import logging

from rasterio.errors import RasterioIOError
from rio_tiler.io import Reader

from app import vsicurl

# Not merely a setting this module happens to read: no read below is correct
# without it, because GDAL would otherwise cache a listing of the directory
# each COG sits in and never see a sibling written afterwards — the other half
# of #400. Importing the name is how that dependency is stated, and importing
# this module is what every read in this service does.
from app.config import GDAL_DISABLE_READDIR_ON_OPEN  # noqa: F401

logger = logging.getLogger(__name__)

#: How storage spells *there is nothing at this address*. MinIO answers a
#: missing object with HTTP 404; the local-filesystem backend the tests read
#: through says ENOENT. Matched on the whole phrase and never on the bare
#: number, which appears in perfectly ordinary asset paths — ``t_140400.tif``
#: contains "404" and has nothing to do with one.
_NOT_FOUND_PHRASES = (
    "HTTP response code: 404",
    "No such file or directory",
    "does not exist in the file system",
)


def is_not_found(exc: Exception) -> bool:
    """Whether ``exc`` says the object is absent, rather than unreadable.

    The distinction decides both what the client is told and whether a retry
    could possibly help: absent is an honest answer to give right now, while
    anything else — a 503 from a busy MinIO, a connection reset — is a claim
    about this moment that the very next moment may contradict.
    """
    message = str(exc)
    return any(phrase in message for phrase in _NOT_FOUND_PHRASES)


class ResilientReader(Reader):
    """A rio-tiler reader that never lets a failed open become permanent.

    On failure it always drops GDAL's cached lookup for the path, so that a
    COG landing a second later is visible to the *next* request rather than
    only to the next process. Whether it also retries the open immediately
    depends on what failed:

    - **Absent** — no retry. Storage has answered, and asking twice would only
      double the cost of the very common case where a client is genuinely
      ahead of the data. The cache is still dropped, which is what turns "this
      timestep is not written yet" back into a question rather than a verdict.
    - **Anything else** — one retry. A 503 is a statement about how busy MinIO
      was a millisecond ago, and the request in hand can still be answered
      correctly; that is precisely the failure #400 was reported for.

    Opening is deliberately the whole of it. What GDAL caches per path is the
    *file property* — does this object exist, how big is it — which is settled
    at open; a range read that fails afterwards fails this request and leaves
    nothing behind to poison the next one.
    """

    def __attrs_post_init__(self):
        try:
            super().__attrs_post_init__()
            return
        except RasterioIOError as exc:
            dropped = vsicurl.forget(self.input)
            if is_not_found(exc) or not dropped:
                raise
            logger.warning("Retrying %s after a transient storage failure: %s", self.input, exc)

        super().__attrs_post_init__()
