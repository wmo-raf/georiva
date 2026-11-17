"""Each worker opens the stores of published Store assets as it starts (#684).

Opening an Upstream store is the dear part of drawing from it: 3.5 s for the
repository and 16–26 s for the first array on its snapshot, measured on GFS
(ADR 0044). A worker pays it once and keeps the store open, but every worker
pays it — 4 in production, and again whenever one is replaced — and until now
the first user to ask a worker for a store's map paid it for them.

So a worker starting up opens them first, in a thread of its own, without
holding up anything else: COG tiles and the healthcheck are answered from the
first moment. Which stores to open is read where a tile reads it, the
remote-assets hashes Django publishes, and each is opened through the same
:func:`app.store.opened_store` under the same pins a tile names, so the store
warmed is the one the first tile finds. A tile that arrives while its store is
still opening waits on that open rather than starting its own.

One store at a time, in no particular order. For each, every array published
from it is opened and its coordinates read; no field is decoded. The open is
not given ``STORE_READ_TIMEOUT`` in all — nobody is waiting on it — only the
library's own bound on each request.

Warming is an optimisation and nothing more. A store or array that fails to
warm is logged and passed over, and is opened by its first tile exactly as it
would have been without this; nothing is retried. A store first published after
the worker started is likewise opened by its first tile.
"""

from __future__ import annotations

import json
import logging
import threading
from time import monotonic

from app import dependencies, store
from app.config import REMOTE_ASSETS_KEY_PREFIX, REMOTE_ASSETS_PUBLISHED_FIELD, STORE_WARM

logger = logging.getLogger(__name__)

THREAD_NAME = "store-warm"


def start() -> None:
    """Begin warming this worker's stores in the background, unless switched off."""
    if not STORE_WARM:
        logger.info("Not warming upstream stores: GEORIVA_STORE_WARM is off")
        return
    threading.Thread(target=warm_stores, name=THREAD_NAME, daemon=True).start()


def warm_stores() -> None:
    """Open every published store with every array published from it. Never raises."""
    started = monotonic()
    try:
        stores = published_stores()
    except Exception as exc:
        logger.warning("Not warming upstream stores: the published assets could not be read: %s", exc)
        return

    warmed = 0
    for (href, chunk_locations), arrays in stores.items():
        try:
            opened = store.opened_store(href, chunk_locations)
        except Exception as exc:
            logger.warning("Could not warm upstream store %s: %s", href, _first_line(exc))
            continue
        for array in sorted(arrays):
            try:
                opened.warm(array)
            except Exception as exc:
                logger.warning("Could not warm array %s of upstream store %s: %s", array, href, _first_line(exc))
        warmed += 1

    logger.info("Warmed %d of %d upstream store(s) in %.1f s", warmed, len(stores), monotonic() - started)


def published_stores() -> dict[tuple[str, tuple[tuple[str, str], ...]], set[str]]:
    """Each published store, by (address, pins), with the arrays published from it.

    Read from every Variable's remote-assets hash. A Remote COG's href is not a
    store and is passed over, as is an entry not in Django's shape.
    """
    redis_client = dependencies.redis_client
    stores: dict[tuple[str, tuple[tuple[str, str], ...]], set[str]] = {}
    seen: set[str] = set()
    for key in redis_client.scan_iter(match=f"{REMOTE_ASSETS_KEY_PREFIX}:*", count=500):
        try:
            entries = redis_client.hgetall(key)
        except Exception as exc:  # one key that is not a hash costs that key, not the warm-up
            logger.debug("Not warming from %s: %s", key, exc)
            continue
        for field, value in entries.items():
            # Most timesteps of a Variable repeat one entry: each is read once.
            if field == REMOTE_ASSETS_PUBLISHED_FIELD or not value.startswith("{") or value in seen:
                continue
            seen.add(value)
            try:
                href, array, chunk_locations = store.published_store(json.loads(value))
            except (ValueError, store.StoreError) as exc:
                logger.debug("Not warming a published entry in %s: %s", key, exc)
                continue
            stores.setdefault((href, chunk_locations), set()).add(array)
    return stores


def _first_line(exc: Exception) -> str:
    said = str(exc)
    return said.splitlines()[0] if said else type(exc).__name__
