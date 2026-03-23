"""
Palette cache utilities for Titiler tile rendering.

Django writes variable rendering config to Redis at startup and on save.
Titiler reads these keys directly (bypassing Django's cache framework prefix)
to apply server-side colormaps when serving XYZ tiles.

Key format:  georiva:palette:{catalog_slug}:{collection_slug}:{variable_slug}
Value format (JSON):
  With palette:    {"vmin": -10.0, "vmax": 40.0, "scale_type": "linear", "colormap": {"0": [r,g,b,a], ...}}
  Without palette: {"vmin": -10.0, "vmax": 40.0, "scale_type": "linear"}
"""

import json
import logging

logger = logging.getLogger(__name__)

PALETTE_KEY_PREFIX = "georiva:palette"


def get_palette_cache_key(catalog_slug: str, collection_slug: str, variable_slug: str) -> str:
    return f"{PALETTE_KEY_PREFIX}:{catalog_slug}:{collection_slug}:{variable_slug}"


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

    if variable.palette:
        stops = variable.palette.as_weatherlayers_palette()
        payload["colormap"] = build_colormap_256(stops, variable.value_min, variable.value_max)

    return payload


def warm_variable(variable) -> None:
    """Write one Variable's rendering config to Redis."""
    from django_redis import get_redis_connection

    try:
        catalog_slug = variable.collection.catalog.slug
        collection_slug = variable.collection.slug
        key = get_palette_cache_key(catalog_slug, collection_slug, variable.slug)
        payload = build_variable_payload(variable)
        redis_conn = get_redis_connection("default")
        redis_conn.set(key, json.dumps(payload))
    except Exception as e:
        logger.warning("Failed to warm palette cache for variable %s: %s", getattr(variable, 'slug', '?'), e)


def warm_all() -> None:
    """Warm all active Variables. Called on Django startup."""
    from georiva.core.models import Variable

    qs = (
        Variable.objects
        .filter(is_active=True)
        .select_related('collection__catalog', 'palette')
        .prefetch_related('palette__stops')
    )

    count = 0
    for variable in qs:
        warm_variable(variable)
        count += 1

    logger.info("Warmed palette cache for %d variables", count)
