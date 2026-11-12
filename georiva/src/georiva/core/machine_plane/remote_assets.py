"""Where Titiler finds a Remote asset's bytes (ADR 0043 §3).

Titiler derives a local COG's address from the request path alone: the key
grammar is fixed, so it consults no Asset row. A **Remote asset** has no such
address. Its bytes are wherever the Upstream put them, and only its row knows
where. That one fact has to reach the tile server, and it travels the way the
rendering config does (``palette_cache``):

*Django publishes it to Redis*, one hash per Variable, timestep → upstream href.
Titiler reads the hash directly, for a collection of any tier, so a ``private``
harvested collection tiles for the callers the gateway lets through.

*The tile-config callback answers it on a miss*, from the row, for ``public``
collections only, for the reasons ``config_view`` gives.

The hash also carries :data:`PUBLISHED_FIELD`. While it is there the hash is
Django's whole word on the Variable, and a timestep missing from it has no
Remote asset. Without it (Redis was flushed, or the Variable has not been warmed
yet) Titiler cannot tell "local" from "not told", and asks the callback.

Key:    ``georiva:remote-assets:{org}:{catalog}:{collection}:{variable}``
Fields: ``{time}`` or ``{time}/{reference_time}``, each ``YYYYMMDDTHHMMSS`` UTC

Which assets are Remote is asked of ``core.storage.asset_location``, never
decided here.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from georiva.core.storage.asset_location import remote_q

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


def _remote_cogs(variable):
    from georiva.core.models import Asset

    # The COG is what a tile is drawn from, and a timestep has at most one per Variable.
    return Asset.objects.filter(remote_q(), variable=variable, format=Asset.Format.COG)


def remote_href(variable, time: datetime, reference_time: datetime | None = None) -> str | None:
    """The upstream href of ``variable``'s asset at this timestep, or ``None``
    when the asset there is local or there is none. Reads the row; writes nothing."""
    return (
        _remote_cogs(variable)
        .filter(item__time=time, item__reference_time=reference_time)
        .values_list("href", flat=True)
        .first()
    )


def publish_variable(variable) -> str | None:
    """Rewrite ``variable``'s hash whole from its rows. Returns the key written,
    or ``None`` if Redis could not be written."""
    try:
        key = variable_key(variable)
        rows = _remote_cogs(variable).values_list("item__time", "item__reference_time", "href")
        _write_hash(key, {timestep_field(time, reference_time): href for time, reference_time, href in rows})
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
    from georiva.core.models import Asset, Variable

    from .palette_cache import prune_stale_keys

    hrefs: dict[int, dict[str, str]] = {}
    rows = Asset.objects.filter(remote_q(), format=Asset.Format.COG, variable__is_active=True).values_list(
        "variable_id", "item__time", "item__reference_time", "href"
    )
    for variable_pk, time, reference_time, href in rows.iterator():
        hrefs.setdefault(variable_pk, {})[timestep_field(time, reference_time)] = href

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

    A Remote COG is written. A harvested COG that is no longer Remote was
    Materialized (ADR 0042 §8), and its timestep is withdrawn so tiles read the
    local copy. Any other asset was never in the hash and costs no Redis call.
    """
    from georiva.core.models import Asset

    if asset.format != Asset.Format.COG:
        return
    if asset.location.is_remote:
        _write_timestep(asset, asset.href)
    elif asset.harvested_by_id is not None:
        _write_timestep(asset, None)


def withdraw_asset(asset) -> None:
    """Drop one deleted Remote asset's timestep from its Variable's hash."""
    from georiva.core.models import Asset

    if asset.format == Asset.Format.COG and asset.location.is_remote:
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
