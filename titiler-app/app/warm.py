"""Each worker opens the stores of published Store assets ahead of their first tile (#684, #706).

Opening an Upstream store is the dear part of drawing from it: 3.5 s for the
repository and 16–26 s for the first array on its snapshot, measured on GFS
(ADR 0044). A worker pays it once and keeps the store open, but every worker
pays it — 4 in production, and again whenever one is replaced — and until #684
the first user to ask a worker for a store's map paid it for them.

So a worker opens them first, in a thread of its own, without holding up
anything else: COG tiles and the healthcheck are answered from the first
moment. Which stores to open is read where a tile reads it, the remote-assets
hashes Django publishes, and each is opened through the same
:func:`app.store.opened_store` under the same pins a tile names, so the store
warmed is the one the first tile finds. A tile that arrives while its store is
still opening waits on that open rather than starting its own.

**It keeps up while the worker runs** (#706). A worker keys an open store by
(address, pins), so a store harvest added after it started, a re-check that
changes a feed's Pinned locations, or an adopted address all name a key it has nothing open
under. Django says so on ``STORES_CHANNEL`` whenever the stores it publishes
change, and the thread listens there for as long as the worker runs. Each time
it hears, it reads the hashes again — :func:`warm_stores` — opening what it has
not and letting go of each store no published asset names any more, with the
fields kept from it, so a store under Pinned locations a re-check replaced does not hold
its memory until a restart. Only a store some reading found named is let go:
one a tile opened from Django's own answer, with no hash to name it, is the
tile's to keep. A burst of announcements — a harvest publishes each
of its Variables — is heard out for ``SETTLE_SECONDS`` and read once.

The message is a nudge, not the news, so one that is lost costs only the
warm-up it would have started: the thread reads the hashes as it subscribes,
which is at start and again after every lost connection, retried with a delay
growing to ``RECONNECT_LAST_SECONDS``. The listener has a connection of its own
that is pinged while idle, so a connection lost without a word — a half-open
socket — is noticed and listened to again rather than heard from never. A store opened by a tile while the
hashes are being read is not let go by that reading.

One store at a time, in no particular order. For each, every array published
from it is opened and its coordinates read; no field is decoded. The open is
not given ``STORE_READ_TIMEOUT`` in all — nobody is waiting on it — only the
library's own bound on each request.

Warming is an optimisation and nothing more. A store or array that fails to
warm is logged and passed over, and is opened by its first tile exactly as it
would have been without this; it is tried again at the next announcement.
Hashes that cannot be read open nothing and let nothing go.
"""

from __future__ import annotations

import json
import logging
import threading
from time import monotonic

import redis

from app import dependencies, store
from app.config import (
    REDIS_URL,
    REMOTE_ASSETS_KEY_PREFIX,
    REMOTE_ASSETS_PUBLISHED_FIELD,
    STORE_WARM,
    STORES_CHANNEL,
)

logger = logging.getLogger(__name__)

THREAD_NAME = "store-warm"

#: How long an announcement is heard out for others before the hashes are read.
SETTLE_SECONDS = 1.0
#: The most an announcement is heard out for, should they never pause.
SETTLE_AT_MOST_SECONDS = 10.0
#: How long the listener waits for a message before looking whether it is to stop.
LISTEN_SECONDS = 1.0
#: The wait before listening again after a lost connection, doubling up to the last.
RECONNECT_FIRST_SECONDS = 1.0
RECONNECT_LAST_SECONDS = 30.0
#: How long the listener's connection may sit idle before it is pinged, so a dead one is noticed.
HEALTH_CHECK_SECONDS = 30

#: The listener's own connection: a subscription holds its connection for as
#: long as it lives, and is pinged while idle, which a tile's reads are not.
listener_client = redis.Redis.from_url(
    REDIS_URL,
    decode_responses=True,
    health_check_interval=HEALTH_CHECK_SECONDS,
    socket_keepalive=True,
)

#: How long a stopping worker waits for the listener to finish what it is doing.
STOP_WAIT_SECONDS = 5.0

#: The listener running in this process, if one is, and its stop.
_listener: threading.Thread | None = None
_stopping: threading.Event | None = None

#: Every (address, pins) a reading of the hashes has found named in this
#: process. Only these are let go when no hash names them any more.
_named: set[tuple[str, tuple[tuple[str, str], ...]]] = set()


def start() -> None:
    """Begin warming this worker's stores in the background, and keep them warm, unless switched off."""
    global _listener, _stopping
    if not STORE_WARM:
        logger.info("Not warming upstream stores: GEORIVA_STORE_WARM is off")
        return
    _stopping = threading.Event()
    _listener = threading.Thread(target=listen, args=(_stopping,), name=THREAD_NAME, daemon=True)
    _listener.start()


def stop() -> None:
    """Stop listening, and wait up to ``STOP_WAIT_SECONDS`` for the listener to end.

    It ends between stores, so the wait is at most the open under way. One
    that outlasts it is left to finish on its own — the thread is a daemon and
    holds nothing a stopping worker needs — and nothing more is opened after it.
    """
    global _listener, _stopping
    if _stopping is None or _listener is None:
        return
    _stopping.set()
    _listener.join(timeout=STOP_WAIT_SECONDS)
    if _listener.is_alive():
        logger.info("Stopped listening for published upstream stores; an open under way is left to finish")
    _listener = _stopping = None


def listen(stopping: threading.Event) -> None:
    """Warm the published stores, then again each time Django announces a change, until ``stopping``.

    Never raises: a lost connection is logged and listened to again.
    """
    delay = RECONNECT_FIRST_SECONDS
    while not stopping.is_set():
        subscription = None
        try:
            subscription = listener_client.pubsub(ignore_subscribe_messages=True)
            subscription.subscribe(STORES_CHANNEL)
            delay = RECONNECT_FIRST_SECONDS
            # What was published before this worker listened, or while it could not.
            warm_stores(stopping)
            while not stopping.is_set():
                if _heard(subscription, LISTEN_SECONDS):
                    _hear_out(subscription)
                    warm_stores(stopping)
        except Exception as exc:
            logger.warning(
                "Not listening for published upstream stores; trying again in %g s: %s", delay, _first_line(exc)
            )
            stopping.wait(delay)
            delay = min(delay * 2, RECONNECT_LAST_SECONDS)
        finally:
            _close(subscription)


def _heard(subscription, timeout: float) -> bool:
    """Whether an announcement arrives within ``timeout`` seconds."""
    message = subscription.get_message(ignore_subscribe_messages=True, timeout=timeout)
    if message is None or message.get("type") != "message":
        return False
    logger.debug("Published upstream stores changed: %s", message.get("data"))
    return True


def _hear_out(subscription) -> None:
    """Take every announcement that follows within ``SETTLE_SECONDS`` of the last.

    Stops after ``SETTLE_AT_MOST_SECONDS`` however many keep coming.
    """
    until = monotonic() + SETTLE_AT_MOST_SECONDS
    while monotonic() < until and _heard(subscription, SETTLE_SECONDS):
        pass


def _close(subscription) -> None:
    if subscription is None:
        return
    try:
        subscription.close()
    except Exception as exc:
        logger.debug("Could not close the upstream store subscription: %s", exc)


def warm_stores(stopping: threading.Event | None = None) -> None:
    """Open every published store with every array published from it, and let
    go of each open store no published asset names. Never raises."""
    started = monotonic()
    # Taken before the hashes are read: a store a tile opens meanwhile is not this reading's to let go.
    already_open = store.open_keys()
    try:
        stores = published_stores()
    except Exception as exc:
        logger.warning("Not warming upstream stores: the published assets could not be read: %s", exc)
        return

    _named.update(stores)
    # A store a reading named once, and none names now; not one a tile opened with no hash to name it.
    gone = (already_open & _named) - stores.keys()
    for href, chunk_locations in gone:
        logger.info("Letting go of upstream store %s: no published asset names it under these Pinned locations", href)
        store.forget(href, chunk_locations)

    warmed = failed = opened_now = 0
    for (href, chunk_locations), arrays in stores.items():
        if stopping is not None and stopping.is_set():
            break
        try:
            opened = store.opened_store(href, chunk_locations)
        except Exception as exc:
            logger.warning("Could not warm upstream store %s: %s", href, _first_line(exc))
            failed += 1
            continue
        if (href, chunk_locations) not in already_open:
            opened_now += 1
        for array in sorted(arrays):
            try:
                opened.warm(array)
            except Exception as exc:
                logger.warning("Could not warm array %s of upstream store %s: %s", array, href, _first_line(exc))
        warmed += 1

    log = logger.info if opened_now or failed or gone else logger.debug
    log(
        "Warmed %d of %d upstream store(s), %d newly opened, %d let go, in %.1f s",
        warmed,
        len(stores),
        opened_now,
        len(gone),
        monotonic() - started,
    )


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
            if field == REMOTE_ASSETS_PUBLISHED_FIELD or not store.is_store_entry(value) or value in seen:
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
