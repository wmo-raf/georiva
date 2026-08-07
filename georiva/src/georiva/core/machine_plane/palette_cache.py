"""
Palette cache utilities for Titiler tile rendering.

Django writes variable rendering config to Redis at startup and on save.
Titiler reads these keys directly (bypassing Django's cache framework prefix)
to apply server-side colormaps when serving XYZ tiles.

Key format:  georiva:palette:{org_slug}:{catalog_slug}:{collection_slug}:{variable_slug}

The organisation leads the key for the same reason it leads a storage path and a
Titiler route: catalog slugs are unique only *within* an organisation (#267), so
without it two institutions each running a ``forecast`` catalog share one cache
entry and whichever wrote last decides how both render.
Value format (JSON):
  With a default style: {"vmin": -10.0, "vmax": 40.0, "scale_type": "linear", "colormap": {"0": [r,g,b,a], ...}}
  Without one:          {"vmin": -10.0, "vmax": 40.0, "scale_type": "linear"}
"""

import json
import logging

logger = logging.getLogger(__name__)

PALETTE_KEY_PREFIX = "georiva:palette"


def get_palette_cache_key(
    org_slug: str, catalog_slug: str, collection_slug: str, variable_slug: str,
) -> str:
    return f"{PALETTE_KEY_PREFIX}:{org_slug}:{catalog_slug}:{collection_slug}:{variable_slug}"


def _ensure_rgba(color: list) -> list:
    """Ensure color has 4 components [r, g, b, a]."""
    if len(color) == 3:
        return color + [255]
    return list(color[:4])


def build_colormap_256(palette_stops: list, vmin: float, vmax: float) -> dict:
    """
    Interpolate [[value, [r,g,b]] or [value, [r,g,b,a]], ...] stops to a
    256-entry dict {0: [r,g,b,a], ..., 255: [r,g,b,a]}.

    Clamps values outside the stop range to the nearest stop color.
    Returns a grayscale fallback if stops are empty or range is degenerate.
    """
    if not palette_stops:
        return {i: [i, i, i, 255] for i in range(256)}

    val_range = vmax - vmin
    if val_range == 0:
        color = _ensure_rgba(palette_stops[0][1])
        return {i: color for i in range(256)}

    stops = sorted(palette_stops, key=lambda s: s[0])
    positions = [(s[0] - vmin) / val_range * 255 for s in stops]
    colors = [_ensure_rgba(s[1]) for s in stops]

    result = {}
    for i in range(256):
        if i <= positions[0]:
            result[i] = colors[0]
        elif i >= positions[-1]:
            result[i] = colors[-1]
        else:
            for j in range(len(positions) - 1):
                if positions[j] <= i <= positions[j + 1]:
                    span = positions[j + 1] - positions[j]
                    t = (i - positions[j]) / span if span > 0 else 0
                    result[i] = [
                        round(colors[j][k] + t * (colors[j + 1][k] - colors[j][k]))
                        for k in range(4)
                    ]
                    break

    return result


def build_variable_payload(variable) -> dict:
    """Build the rendering payload dict for a Variable."""
    payload = {
        "vmin": variable.value_min,
        "vmax": variable.value_max,
        "scale_type": variable.scale_type or "linear",
    }

    style = variable.default_style
    if style:
        stops = style.as_weatherlayers_palette()
        payload["colormap"] = build_colormap_256(stops, variable.value_min, variable.value_max)

    return payload


def variable_cache_key(variable) -> str:
    """The Redis key holding ``variable``'s rendering config."""
    from georiva.core.machine_plane import org_slug_of

    collection = variable.collection
    return get_palette_cache_key(
        org_slug_of(collection), collection.catalog.slug,
        collection.slug, variable.slug,
    )


def warm_variable(variable):
    """Write one Variable's rendering config to Redis, returning the key used.

    Returns ``None`` if the write failed, which is also what tells
    :func:`warm_all` not to treat the key as live: a key it could not write is
    not one it should protect from the sweep.
    """
    from django_redis import get_redis_connection

    try:
        key = variable_cache_key(variable)
        payload = build_variable_payload(variable)
        redis_conn = get_redis_connection("default")
        redis_conn.set(key, json.dumps(payload))
        return key
    except Exception as e:
        logger.warning("Failed to warm palette cache for variable %s: %s", getattr(variable, 'slug', '?'), e)
        return None


def prune_variable(variable) -> None:
    """Drop one Variable's rendering config from Redis, for its deletion.

    Best-effort like :func:`warm_variable`: the variable may be mid-cascade
    with its collection already gone, in which case the key cannot even be
    named — the periodic sweep collects it instead.
    """
    from django_redis import get_redis_connection

    try:
        key = variable_cache_key(variable)
        get_redis_connection("default").delete(key)
    except Exception as e:
        logger.warning(
            "Failed to prune palette cache for variable %s: %s",
            getattr(variable, 'slug', '?'), e,
        )


def prune_stale_keys(live_keys) -> int:
    """Delete every palette key not in ``live_keys``. Returns the count removed.

    Titiler reads these keys directly, so nothing else ever expires them: a key
    written under a slug that has since been renamed, or for a variable since
    deleted or deactivated, would otherwise sit there indefinitely. Sweeping on
    every warm keeps the cache a mirror of the database rather than an
    accumulation of its history — which also clears the keys left behind by the
    org segment being added to the format (#272).

    Deleting a key a request needs costs one Django fallback, which is the
    documented miss path; keeping a key a request must not have is a stale
    palette nobody can flush without a redeploy.
    """
    from django_redis import get_redis_connection

    try:
        redis_conn = get_redis_connection("default")
        stale = [
            key for key in redis_conn.scan_iter(match=f"{PALETTE_KEY_PREFIX}:*")
            if _as_text(key) not in live_keys
        ]
        if stale:
            redis_conn.delete(*stale)
        return len(stale)
    except Exception as e:
        logger.warning("Failed to prune stale palette cache keys: %s", e)
        return 0


def _as_text(key) -> str:
    return key.decode() if isinstance(key, bytes) else key


def warm_all() -> None:
    """Warm all active Variables. Called on Django startup."""
    from georiva.core.models import Variable

    qs = (
        Variable.objects
        .filter(is_active=True)
        .select_related('collection__catalog__organisation')
        .prefetch_related('styles')
    )

    live_keys = {key for key in (warm_variable(v) for v in qs) if key}

    pruned = prune_stale_keys(live_keys)
    logger.info(
        "Warmed palette cache for %d variables, pruned %d stale key(s)",
        len(live_keys), pruned,
    )
