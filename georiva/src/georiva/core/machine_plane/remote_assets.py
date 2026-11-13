"""Where Titiler finds a Remote asset's bytes (ADR 0043 §3, ADR 0044 §9).

Titiler derives a local COG's address from the request path alone: the key
grammar is fixed, so it consults no Asset row. A **Remote asset** has no such
address. Its bytes are wherever the Upstream put them, and only its row knows
where. That one fact has to reach the tile server, and it travels the way the
rendering config does (``palette_cache``):

*Django publishes it to Redis*, one hash per Variable, timestep → upstream href.
Titiler reads the hash directly, for a collection of any tier, so a ``private``
harvested collection tiles for the callers the gateway lets through.

A **Store asset** needs more than an href: its value in the hash is a JSON
object, :func:`store_entry` — the store's address, the array's path, and the
Pinned locations of the feed that harvested it, the only places Titiler may
follow the store's data into. The run and step are the field itself: Titiler
selects them by value among the store's own coordinates, so nothing here goes
stale when the store's coordinates grow (they only grow at the end). A row whose
feed is gone is published with no pins, so its data is followed nowhere.

*The tile-config callback answers it on a miss*, from the row, for ``public``
collections only, for the reasons ``config_view`` gives.

The hash also carries :data:`PUBLISHED_FIELD`. While it is there the hash is
Django's whole word on the Variable, and a timestep missing from it has no
Remote asset. Without it (Redis was flushed, or the Variable has not been warmed
yet) Titiler cannot tell "local" from "not told", and asks the callback.

Key:    ``georiva:remote-assets:{org}:{catalog}:{collection}:{variable}``
Fields: ``{time}`` or ``{time}/{reference_time}``, each ``YYYYMMDDTHHMMSS`` UTC
Values: a Remote COG's href, or a Store asset's :func:`store_entry` as JSON

Which assets are Remote is asked of ``core.storage.asset_location``, never
decided here.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from django.db.models import Q

from georiva.core.storage.asset_location import remote_q, store_q

logger = logging.getLogger(__name__)

REMOTE_ASSETS_KEY_PREFIX = "georiva:remote-assets"

#: Marks a hash Django wrote whole. Timestep fields open with a digit, so it cannot be one.
PUBLISHED_FIELD = "published"

_STAMP = "%Y%m%dT%H%M%S"


def timestep_field(time: datetime, reference_time: datetime | None = None) -> str:
    """The hash field naming one timestep. Titiler spells the same field from a
    request's ``time`` and ``reftime`` (``titiler-app/app/dependencies.py``)."""
    field = time.astimezone(UTC).strftime(_STAMP)
    if reference_time is not None:
        field = f"{field}/{reference_time.astimezone(UTC).strftime(_STAMP)}"
    return field


def remote_assets_key(org_slug: str, catalog_slug: str, collection_slug: str, variable_slug: str) -> str:
    return f"{REMOTE_ASSETS_KEY_PREFIX}:{org_slug}:{catalog_slug}:{collection_slug}:{variable_slug}"


def variable_key(variable) -> str:
    from georiva.core.machine_plane import org_slug_of

    collection = variable.collection
    return remote_assets_key(org_slug_of(collection), collection.catalog.slug, collection.slug, variable.slug)


def _published_assets():
    """The rows a tile is drawn from upstream: a Remote COG, or a Store asset.
    A timestep has at most one of either per Variable."""
    from georiva.core.models import Asset

    return Asset.objects.filter((remote_q() & Q(format=Asset.Format.COG)) | store_q())


def store_entry(href: str, array: str, chunk_locations: dict) -> dict:
    """What Titiler is told of a Store asset: the store, the array, and where
    the store's data may be followed (ADR 0044 §9, §11). Titiler reads the same
    keys (``titiler-app/app/store.py``)."""
    return {"store": href, "array": array, "chunk_locations": dict(chunk_locations or {})}


def _chunk_locations(feed_pks) -> dict[int, dict]:
    """Each harvest feed's Pinned locations for a store's data, by feed.

    Looked up by model label: the feed is a harvest model, and core is not
    built on the harvest app. A feed that is gone pins nothing.
    """
    from django.apps import apps

    feed_pks = {pk for pk in feed_pks if pk is not None}
    if not feed_pks:
        return {}
    try:
        feeds = apps.get_model("georivaharvest", "HarvestFeed")
    except LookupError:
        return {}
    return dict(feeds.objects.filter(pk__in=feed_pks).values_list("pk", "pinned_chunk_locations"))


def _values(rows) -> dict[tuple, str]:
    """``(variable, field) → the hash value`` for rows of
    ``(variable_id, time, reference_time, format, href, store_array, harvested_by_id)``."""
    from georiva.core.models import Asset

    rows = list(rows)
    pins = _chunk_locations(
        harvested_by for *_, asset_format, _, _, harvested_by in rows if asset_format == Asset.Format.ZARR
    )
    values = {}
    for variable_pk, time, reference_time, asset_format, href, store_array, harvested_by in rows:
        if asset_format == Asset.Format.ZARR:
            href = json.dumps(store_entry(href, store_array, pins.get(harvested_by, {})))
        values[variable_pk, timestep_field(time, reference_time)] = href
    return values


_ROW = ("variable_id", "item__time", "item__reference_time", "format", "href", "store_array", "harvested_by_id")


def remote_source(variable, time: datetime, reference_time: datetime | None = None) -> str | dict | None:
    """What ``variable``'s asset at this timestep is read from upstream: a
    Remote COG's href, a Store asset's :func:`store_entry`, or ``None`` when the
    asset there is local or there is none. Reads the rows; writes nothing."""
    rows = (
        _published_assets()
        .filter(variable=variable, item__time=time, item__reference_time=reference_time)
        .values_list(*_ROW)[:1]
    )
    for value in _values(rows).values():
        return json.loads(value) if value.startswith("{") else value
    return None


def publish_variable(variable) -> str | None:
    """Rewrite ``variable``'s hash whole from its rows. Returns the key written,
    or ``None`` if Redis could not be written."""
    try:
        key = variable_key(variable)
        values = _values(_published_assets().filter(variable=variable).values_list(*_ROW))
        _write_hash(key, {field: value for (_, field), value in values.items()})
        return key
    except Exception as e:
        logger.warning("Failed to publish remote assets for variable %s: %s", getattr(variable, "slug", "?"), e)
        return None


def publish_all() -> None:
    """Publish every active Variable's hash and drop the hashes no Variable
    answers to any more. Called wherever the palette cache is warmed whole.

    One query finds every Remote asset on the instance, rather than one per
    Variable: most Variables have none, and still need their (empty) hash so
    Titiler knows it has been told.
    """
    from georiva.core.models import Variable

    from .palette_cache import prune_stale_keys

    hrefs: dict[int, dict[str, str]] = {}
    rows = _published_assets().filter(variable__is_active=True).values_list(*_ROW)
    for (variable_pk, field), value in _values(rows.iterator()).items():
        hrefs.setdefault(variable_pk, {})[field] = value

    live_keys = set()
    for variable in Variable.objects.filter(is_active=True).select_related("collection__catalog__organisation"):
        try:
            key = variable_key(variable)
            _write_hash(key, hrefs.get(variable.pk, {}))
            live_keys.add(key)
        except Exception as e:
            logger.warning("Failed to publish remote assets for variable %s: %s", variable.slug, e)

    pruned = prune_stale_keys(live_keys, prefix=REMOTE_ASSETS_KEY_PREFIX)
    logger.info(
        "Published remote assets for %d variables (%d with any), pruned %d stale key(s)",
        len(live_keys),
        len(hrefs),
        pruned,
    )


def _write_hash(key: str, fields: dict[str, str]) -> None:
    """Replace the hash at ``key``. In one transaction, so a reader sees the old
    hash or the new one and never a half-filled one that claims to be whole."""
    from django_redis import get_redis_connection

    pipeline = get_redis_connection("default").pipeline(transaction=True)
    pipeline.delete(key)
    pipeline.hset(key, mapping={**fields, PUBLISHED_FIELD: "1"})
    pipeline.execute()


def withdraw_variable(variable) -> None:
    """Drop ``variable``'s hash, for its deletion. Best-effort, like ``palette_cache.prune_variable``."""
    from django_redis import get_redis_connection

    try:
        get_redis_connection("default").delete(variable_key(variable))
    except Exception as e:
        logger.warning("Failed to withdraw remote assets for variable %s: %s", getattr(variable, "slug", "?"), e)


def publish_asset(asset) -> None:
    """Bring one saved asset's timestep up to date in its Variable's hash.

    A Remote COG or a Store asset is written. A harvested COG that is no longer
    Remote was Materialized (ADR 0042 §8), and its timestep is withdrawn so
    tiles read the local copy. Any other asset was never in the hash and costs
    no Redis call.
    """
    from georiva.core.models import Asset

    location = asset.location
    if location.is_store:
        pins = _chunk_locations([asset.harvested_by_id]).get(asset.harvested_by_id, {})
        _write_timestep(asset, json.dumps(store_entry(asset.href, asset.store_array, pins)))
    elif asset.format != Asset.Format.COG:
        return
    elif location.is_remote:
        _write_timestep(asset, asset.href)
    elif asset.harvested_by_id is not None:
        _write_timestep(asset, None)


def withdraw_asset(asset) -> None:
    """Drop one deleted Remote asset's timestep from its Variable's hash."""
    from georiva.core.models import Asset

    location = asset.location
    if location.is_store or (asset.format == Asset.Format.COG and location.is_remote):
        _write_timestep(asset, None)


def _write_timestep(asset, href: str | None) -> None:
    from django_redis import get_redis_connection

    try:
        # Mid-cascade the item or variable may already be gone; then there is
        # no key to name, and the next whole publish or sweep settles it.
        item = asset.item
        key = variable_key(asset.variable)
        field = timestep_field(item.time, item.reference_time)
        redis_conn = get_redis_connection("default")
        if href is None:
            redis_conn.hdel(key, field)
        else:
            redis_conn.hset(key, field, href)
    except Exception as e:
        logger.warning("Failed to update the remote asset hash for asset %s: %s", getattr(asset, "pk", "?"), e)
