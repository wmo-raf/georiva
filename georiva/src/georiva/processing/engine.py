"""
The Derivation Engine — the generic, domain-agnostic run loop.

Owns: enumerate units → take the DerivationRun lock → resolve inputs →
idempotency check → readiness → compute (recipe.transform) → write asset →
register Published item/asset → write DerivationLinks → emit event.

Recipes declare; the engine executes. Nothing here knows about climate
semantics. See docs/adr/0005-generic-derivation-engine.md.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from .recipe import (
    BaseRecipe,
    ProductionUnit,
    compute_input_hash,
    unit_hash,
    unit_to_canonical_json,
)

logger = logging.getLogger(__name__)


@dataclass
class UnitResult:
    status: str               # completed | skipped | not_ready | locked | failed
    item_id: int | None = None
    input_hash: str = ""


def _asset_output_path(item, variable, fmt: str) -> str:
    ext = {"cog": "tif", "geotiff": "tif", "png": "png"}.get(fmt, "tif")
    t = item.time
    return (
        f"{item.collection.catalog.slug}/{item.collection.slug}/{variable.slug}/"
        f"{t:%Y/%m/%d}/{variable.slug}_{t:%Y%m%dT%H%M%S}.{ext}"
    )


def _register_item(out_item, recipe: BaseRecipe, input_hash: str):
    """Create or update (overwrite-in-place) the Published Item for a unit."""
    from georiva.core.models import Item
    from georiva.ingestion.utils import ensure_utc  # tz-normalise

    time = ensure_utc(out_item.time)
    ref = ensure_utc(out_item.reference_time) if out_item.reference_time else None

    item, _ = Item.objects.get_or_create(
        collection=out_item.collection,
        time=time,
        reference_time=ref,
        defaults={
            "bounds": out_item.bounds,
            "crs": out_item.crs,
            "width": out_item.width,
            "height": out_item.height,
        },
    )

    # Overwrite-in-place: refresh spatial metadata + derivation provenance.
    item.bounds = out_item.bounds if out_item.bounds is not None else item.bounds
    item.crs = out_item.crs or item.crs
    item.width = out_item.width if out_item.width is not None else item.width
    item.height = out_item.height if out_item.height is not None else item.height
    props = dict(item.properties or {})
    props.update(out_item.properties or {})
    props["derivation"] = {
        "recipe": recipe.type,
        "version": recipe.version,
        "input_hash": input_hash,
    }
    item.properties = props
    item.save()
    return item


def _register_asset(item, oa, writer):
    """Write the asset bytes (COG or passthrough copy) and upsert the Asset row."""
    from georiva.core.storage import storage
    from georiva.core.models import Asset

    path = _asset_output_path(item, oa.variable, oa.format)

    if oa.array is not None:
        if oa.format == "png":
            # Encode the data array to RGBA and write a PNG (the visual asset),
            # mirroring ingestion's COG+PNG output for served items.
            from georiva.ingestion.encoder import VariableEncoder
            rgba = VariableEncoder().encode_to_rgba(oa.array, oa.variable)
            href = writer.write_png(rgba, path)
        else:
            href = writer.write_cog(
                oa.array, path, tuple(oa.bounds) if oa.bounds else None, oa.crs,
            )
    elif oa.passthrough is not None:
        src_bucket_type, src_href = oa.passthrough
        data = storage.bucket(src_bucket_type).read_bytes(src_href)
        href = writer.bucket.save(path, data)
    else:
        raise ValueError("OutputAsset needs either `array` or `passthrough`")

    stats = oa.stats or {}
    asset, _ = Asset.objects.update_or_create(
        item=item,
        variable=oa.variable,
        format=oa.format,
        defaults={
            "href": href,
            "roles": list(oa.roles),
            "checksum": oa.checksum,
            "width": oa.width,
            "height": oa.height,
            "stats_min": stats.get("min"),
            "stats_max": stats.get("max"),
            "stats_mean": stats.get("mean"),
            "stats_std": stats.get("std"),
        },
    )
    return asset


def _write_links(item, resolved, recipe: BaseRecipe, input_hash: str):
    """Record DerivationLink edges from the new Item to each input item."""
    from georiva.core.models import Item as PublishedItem
    from georiva.staging.models import DerivationLink, StagingItem

    # Replace this item's lineage (overwrite-in-place semantics).
    DerivationLink.objects.filter(derived_item=item).delete()

    seen = set()
    for ri in resolved.values():
        for src in ri.items:
            key = (type(src).__name__, src.pk)
            if key in seen:
                continue
            seen.add(key)
            kwargs = {
                "derived_item": item,
                "recipe_id": recipe.type,
                "recipe_version": recipe.version,
                "input_hash": input_hash,
            }
            if isinstance(src, StagingItem):
                kwargs["source_staging_item"] = src
            elif isinstance(src, PublishedItem):
                kwargs["source_published_item"] = src
            else:
                continue
            DerivationLink.objects.create(**kwargs)


def _is_current(out_item, recipe: BaseRecipe, input_hash: str) -> bool:
    """True if a Published Item for this unit already records this exact hash."""
    from georiva.core.models import Item
    from georiva.ingestion.utils import ensure_utc

    time = ensure_utc(out_item.time)
    ref = ensure_utc(out_item.reference_time) if out_item.reference_time else None
    existing = Item.objects.filter(
        collection=out_item.collection, time=time, reference_time=ref,
    ).first()
    if not existing:
        return False
    d = (existing.properties or {}).get("derivation", {})
    return d.get("input_hash") == input_hash and d.get("version") == recipe.version


def run_unit(recipe: BaseRecipe, unit: ProductionUnit, *, writer=None, worker_id="", origin=None) -> UnitResult:
    """
    Execute one ProductionUnit end to end, under the DerivationRun lock.

    Idempotent: an unchanged unit is a no-op; changed inputs (or recipe
    version) recompute and overwrite the Published Item in place.

    ``origin`` is an opaque grouping key passed straight to the DerivationRun
    (ADR-0008); the engine never interprets it.
    """
    from django.db import transaction

    from .models import DerivationRun

    uhash = unit_hash(unit)
    run = DerivationRun.acquire(
        recipe_type=recipe.type,
        recipe_version=recipe.version,
        unit_key=unit,
        unit_hash=uhash,
        worker_id=worker_id,
        origin=origin,
    )
    if run is None:
        logger.info("Unit already locked: %s %s", recipe.type, uhash[:8])
        return UnitResult(status="locked")

    try:
        resolved = recipe.resolve_inputs(unit)
        ihash = compute_input_hash(resolved, recipe.version)
        out_item = recipe.outputs(unit)

        if _is_current(out_item, recipe, ihash):
            run.mark_skipped(input_hash=ihash)
            return UnitResult(status="skipped", input_hash=ihash)

        if not recipe.readiness(unit, resolved):
            run.mark_not_ready()
            return UnitResult(status="not_ready")

        if writer is None:
            from georiva.core.storage import storage
            from georiva.ingestion.asset_writer import AssetWriter
            writer = AssetWriter(storage.assets)

        out_assets = recipe.transform(unit, resolved)

        with transaction.atomic():
            item = _register_item(out_item, recipe, ihash)
            for oa in out_assets:
                _register_asset(item, oa, writer)
            _write_links(item, resolved, recipe, ihash)
            run.mark_completed(produced_item=item, input_hash=ihash)

        _emit_event(recipe, unit, item, ihash)
        return UnitResult(status="completed", item_id=item.pk, input_hash=ihash)

    except Exception as exc:
        logger.exception("Derivation failed: %s %s", recipe.type, uhash[:8])
        run.mark_failed(str(exc))
        raise


def _emit_event(recipe, unit, item, input_hash):
    try:
        from georiva.ingestion.events import publish_event
        publish_event({
            "type": "derivation.completed",
            "recipe": recipe.type,
            "version": recipe.version,
            "unit": unit_to_canonical_json(unit),
            "item_id": item.pk,
            "input_hash": input_hash,
        })
    except Exception as e:  # events are best-effort
        logger.warning("Derivation event publish failed: %s", e)


def run(recipe: BaseRecipe, selector, *, dispatch: bool = True, worker_id="", origin=None) -> list:
    """
    Enumerate candidate units for a selector and run each.

    ``dispatch=True`` fans out one Celery task per unit on the processing queue;
    ``dispatch=False`` runs inline (used by tests and small synchronous runs).
    Backfill and streaming both call this — they differ only in selector width:
    a wide range selector enumerates many units; a narrow arriving-input trigger
    maps to just the units that input feeds. Both go through
    ``candidate_units`` (whose default is ``enumerate_units``), so this one
    primitive serves event-driven, scheduled/backfill, and manual invocation.
    """
    units = list(recipe.candidate_units(selector))

    if dispatch:
        from .tasks import run_unit_task
        for unit in units:
            run_unit_task.delay(recipe_type=recipe.type, unit=unit, origin=origin)
        return [UnitResult(status="dispatched") for _ in units]

    return [run_unit(recipe, unit, worker_id=worker_id, origin=origin) for unit in units]
