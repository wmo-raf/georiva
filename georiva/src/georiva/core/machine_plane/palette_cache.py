"""
Palette cache utilities for Titiler tile rendering.

Django writes variable rendering config to Redis at startup and on save.
Titiler reads these keys directly (bypassing Django's cache framework prefix)
to apply server-side colormaps when serving XYZ tiles.

Key format:  georiva:palette:{org_slug}:{catalog_slug}:{collection_slug}:{variable_slug}[:{style_slug}]

The organisation leads the key for the same reason it leads a storage path and a
Titiler route: catalog slugs are unique only *within* an organisation (#267), so
without it two institutions each running a ``forecast`` catalog share one cache
entry and whichever wrote last decides how both render.

The trailing style segment is present on one key per style; the styleless key
stays as the *default style's alias* (ADR 0023), so a request that names no
style and a consumer written before styles existed both keep reading it.
Warming a variable always rewrites the alias from whichever style is default
at that moment — if the alias ever diverges from the default's own key, that
is a bug in the default-flip signal, not a feature.

Value format (JSON):
  With a style:    {"vmin": -10.0, "vmax": 40.0, "scale_type": "linear", "colormap": {"0": [r,g,b,a], ...}}
  Without one:     {"vmin": -10.0, "vmax": 40.0, "scale_type": "linear"}
"""

import json
import logging

logger = logging.getLogger(__name__)

PALETTE_KEY_PREFIX = "georiva:palette"


def get_palette_cache_key(
    org_slug: str, catalog_slug: str, collection_slug: str, variable_slug: str,
    style_slug: str = None,
) -> str:
    key = f"{PALETTE_KEY_PREFIX}:{org_slug}:{catalog_slug}:{collection_slug}:{variable_slug}"
    if style_slug:
        key = f"{key}:{style_slug}"
    return key


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


def build_variable_payload(variable, style=None) -> dict:
    """Build the rendering payload dict for a Variable.

    ``style`` selects which of the variable's styles colors it; ``None`` means
    the default, which keeps this the payload the styleless alias key holds.
    Range and scale always come from the variable — styles differ only in
    their colormap (ADR 0023).
    """
    payload = {
        "vmin": variable.value_min,
        "vmax": variable.value_max,
        "scale_type": variable.scale_type or "linear",
    }

    style = style if style is not None else variable.default_style
    if style:
        stops = style.as_weatherlayers_palette()
        payload["colormap"] = build_colormap_256(stops, variable.value_min, variable.value_max)

    return payload


def variable_cache_key(variable, style=None) -> str:
    """The Redis key holding ``variable``'s rendering config — the styleless
    alias, or ``style``'s own segmented key when one is given."""
    from georiva.core.machine_plane import org_slug_of

    collection = variable.collection
    return get_palette_cache_key(
        org_slug_of(collection), collection.catalog.slug,
        collection.slug, variable.slug,
        style.slug if style is not None else None,
    )


def warm_variable(variable):
    """Write one Variable's rendering config to Redis: the styleless alias
    plus one segmented key per style. Returns the list of keys written.

    Returns ``None`` if the write failed, which is also what tells
    :func:`warm_all` not to treat the keys as live: keys it could not write
    are not ones it should protect from the sweep.
    """
    from django_redis import get_redis_connection

    try:
        entries = {variable_cache_key(variable): build_variable_payload(variable)}
        for style in variable.styles.all():
            entries[variable_cache_key(variable, style)] = (
                build_variable_payload(variable, style)
            )
        redis_conn = get_redis_connection("default")
        for key, payload in entries.items():
            redis_conn.set(key, json.dumps(payload))
        return list(entries)
    except Exception as e:
        logger.warning("Failed to warm palette cache for variable %s: %s", getattr(variable, 'slug', '?'), e)
        return None


def prune_variable(variable) -> None:
    """Drop one Variable's rendering config from Redis, for its deletion —
    the styleless alias and every per-style key under it.

    Best-effort like :func:`warm_variable`: the variable may be mid-cascade
    with its collection already gone, in which case the keys cannot even be
    named — the periodic sweep collects them instead. The style keys are found
    by pattern rather than by row because the same cascade may have taken the
    style rows first. ``{key}:*`` cannot overreach: the style segment is
    colon-separated, so a neighbouring ``{variable}2`` never matches.
    """
    from django_redis import get_redis_connection

    try:
        key = variable_cache_key(variable)
        redis_conn = get_redis_connection("default")
        style_keys = list(redis_conn.scan_iter(match=f"{key}:*"))
        redis_conn.delete(key, *style_keys)
    except Exception as e:
        logger.warning(
            "Failed to prune palette cache for variable %s: %s",
            getattr(variable, 'slug', '?'), e,
        )


def prune_style(style) -> None:
    """Drop one style's segmented key, for its deletion.

    Only the deleted style's own key: the alias and the sibling keys are
    re-warmed by the same signal that calls this, and when the deletion is a
    cascade from the variable, :func:`prune_variable` sweeps everything anyway.
    """
    from django_redis import get_redis_connection

    try:
        key = variable_cache_key(style.variable, style)
        get_redis_connection("default").delete(key)
    except Exception as e:
        logger.warning(
            "Failed to prune palette cache for style %s: %s",
            getattr(style, 'slug', '?'), e,
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

    warmed = 0
    live_keys = set()
    for variable in qs:
        keys = warm_variable(variable)
        if keys:
            warmed += 1
            live_keys.update(keys)

    pruned = prune_stale_keys(live_keys)
    logger.info(
        "Warmed palette cache for %d variables (%d keys), pruned %d stale key(s)",
        warmed, len(live_keys), pruned,
    )
