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
import time
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
    """The stored path for a derived asset — the *same* scheme ingestion writes
    (``ingestion.handlers.asset_handler``): a time-partitioned directory from the
    shared ``storage.build_asset_path`` plus a ``{variable}_{HHMMSS}`` filename
    (the date lives in the path), suffixed ``__ref{reftime}`` for a forecast item.
    Reusing the shared builder keeps derived and ingested assets resolvable by the
    same href-agnostic consumers (map layers, Titiler), instead of the divergent
    ``{variable}_{YYYYMMDDTHHMMSS}`` this used to emit."""
    from georiva.core.storage import storage

    ext = {"cog": "tif", "geotiff": "tif", "png": "png"}.get(fmt, "tif")
    t = item.time
    if item.reference_time:
        ref_str = item.reference_time.strftime("%Y%m%dT%H%M%S")
        name = f"{variable.slug}_{t:%H%M%S}__ref{ref_str}.{ext}"
    else:
        name = f"{variable.slug}_{t:%H%M%S}.{ext}"
    return storage.build_asset_path(
        catalog=item.collection.catalog.slug,
        collection=item.collection.slug,
        variable=variable.slug,
        timestamp=t,
        filename=name,
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


def _pos(unit_index, unit_total) -> str:
    """A compact ``i/N`` ordinal for logs (``?`` when unknown, e.g. inline runs)."""
    if unit_index and unit_total:
        return f"{unit_index}/{unit_total}"
    if unit_index:
        return str(unit_index)
    return "?"


def _log_progress(origin, unit_total) -> None:
    """Emit a live 'X of N remaining' summary for a product/origin batch.

    Units fan out to independent Celery tasks across worker processes, so no
    single process can count progress in memory. This aggregates the batch's
    ``DerivationRun`` rows by ``origin`` instead — committed DB state, so the
    tally is correct across every concurrent worker. Skipped when there is no
    origin (engine-internal/manual reruns aren't a tracked batch).
    """
    if not origin:
        return
    from django.db.models import Count

    from .models import DerivationRun

    counts = {
        row["status"]: row["n"]
        for row in DerivationRun.objects.filter(origin=origin)
        .values("status")
        .annotate(n=Count("id"))
    }
    completed = counts.get("completed", 0)
    skipped = counts.get("skipped", 0)
    failed = counts.get("failed", 0)
    not_ready = counts.get("not_ready", 0)
    running = counts.get("running", 0)
    pending = counts.get("pending", 0)
    total = unit_total or sum(counts.values())
    done = completed + skipped
    remaining = max(total - done - failed, 0)
    logger.info(
        "[progress] origin=%s — %d/%d done, %d remaining "
        "(completed=%d skipped=%d failed=%d not_ready=%d running=%d pending=%d)",
        origin, done, total, remaining,
        completed, skipped, failed, not_ready, running, pending,
    )


def run_unit(recipe: BaseRecipe, unit: ProductionUnit, *, writer=None, worker_id="", origin=None, unit_index=None, unit_total=None) -> UnitResult:
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
    pos = _pos(unit_index, unit_total)
    tag = f"{recipe.type}[{uhash[:8]}]"
    t_start = time.monotonic()
    logger.info(
        "[unit %s] %s START — recipe=%s v%s origin=%s unit=%s",
        pos, tag, recipe.type, recipe.version, origin, unit_to_canonical_json(unit),
    )

    run = DerivationRun.acquire(
        recipe_type=recipe.type,
        recipe_version=recipe.version,
        unit_key=unit,
        unit_hash=uhash,
        worker_id=worker_id,
        origin=origin,
    )
    if run is None:
        logger.info(
            "[unit %s] %s LOCKED by another worker — another task holds this unit, skipping",
            pos, tag,
        )
        return UnitResult(status="locked")
    logger.info("[unit %s] %s lock acquired (run_id=%s worker=%s)", pos, tag, run.pk, worker_id or "-")

    try:
        logger.info("[unit %s] %s step 1/6 — resolving inputs…", pos, tag)
        resolved = recipe.resolve_inputs(unit)
        n_roles = len(resolved)
        n_items = sum(len(ri.items) for ri in resolved.values())
        n_assets = sum(len(ri.assets) for ri in resolved.values())
        logger.info(
            "[unit %s] %s resolved %d input role(s): %d source item(s), %d asset(s) [%s]",
            pos, tag, n_roles, n_items, n_assets,
            ", ".join(f"{ri.name}={len(ri.items)}" for ri in resolved.values()) or "none",
        )

        ihash = compute_input_hash(resolved, recipe.version)
        out_item = recipe.outputs(unit)
        logger.debug("[unit %s] %s input_hash=%s output→%s@%s", pos, tag, ihash[:12],
                     getattr(out_item.collection, "slug", "?"), out_item.time)

        logger.info("[unit %s] %s step 2/6 — idempotency check…", pos, tag)
        if _is_current(out_item, recipe, ihash):
            logger.info(
                "[unit %s] %s SKIP — output already current for this input_hash (no recompute)",
                pos, tag,
            )
            run.mark_skipped(input_hash=ihash)
            _log_progress(origin, unit_total)
            return UnitResult(status="skipped", input_hash=ihash)

        logger.info("[unit %s] %s step 3/6 — readiness check…", pos, tag)
        if not recipe.readiness(unit, resolved):
            missing = [ri.name for ri in resolved.values() if ri.required and not ri.present]
            logger.info(
                "[unit %s] %s NOT READY — required input(s) absent: %s "
                "(will retry via the 5-min sweep when inputs arrive)",
                pos, tag, ", ".join(missing) or "unknown",
            )
            run.mark_not_ready()
            _log_progress(origin, unit_total)
            return UnitResult(status="not_ready")

        if writer is None:
            from georiva.core.storage import storage
            from georiva.ingestion.asset_writer import AssetWriter
            writer = AssetWriter(storage.assets)

        logger.info("[unit %s] %s step 4/6 — computing (recipe.transform)…", pos, tag)
        t_tx = time.monotonic()
        out_assets = recipe.transform(unit, resolved)
        logger.info(
            "[unit %s] %s transform produced %d output asset(s) in %.1fs",
            pos, tag, len(out_assets), time.monotonic() - t_tx,
        )

        logger.info("[unit %s] %s step 5/6 — writing %d asset(s) + registering item…",
                    pos, tag, len(out_assets))
        with transaction.atomic():
            item = _register_item(out_item, recipe, ihash)
            for j, oa in enumerate(out_assets, 1):
                logger.info(
                    "[unit %s] %s   asset %d/%d — variable=%s format=%s roles=%s",
                    pos, tag, j, len(out_assets),
                    getattr(oa.variable, "slug", "?"), oa.format, ",".join(oa.roles),
                )
                _register_asset(item, oa, writer)
            _write_links(item, resolved, recipe, ihash)
            run.mark_completed(produced_item=item, input_hash=ihash)
        logger.info("[unit %s] %s registered item id=%s + %d asset(s), lineage links written",
                    pos, tag, item.pk, len(out_assets))

        logger.info("[unit %s] %s step 6/6 — emitting completion event…", pos, tag)
        _emit_event(recipe, unit, item, ihash)

        logger.info(
            "[unit %s] %s COMPLETED — item id=%s in %.1fs total",
            pos, tag, item.pk, time.monotonic() - t_start,
        )
        _log_progress(origin, unit_total)
        return UnitResult(status="completed", item_id=item.pk, input_hash=ihash)

    except Exception as exc:
        logger.exception(
            "[unit %s] %s FAILED after %.1fs — %s",
            pos, tag, time.monotonic() - t_start, exc,
        )
        run.mark_failed(str(exc))
        _log_progress(origin, unit_total)
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
    total = len(units)
    logger.info(
        "[run] recipe=%s v%s origin=%s — enumerated %d unit(s) (mode=%s)",
        recipe.type, recipe.version, origin, total,
        "dispatch→celery" if dispatch else "inline",
    )
    if total == 0:
        logger.info(
            "[run] recipe=%s origin=%s — no candidate units for this selector; nothing to do",
            recipe.type, origin,
        )

    if dispatch:
        from .tasks import run_unit_task
        for i, unit in enumerate(units, 1):
            logger.info(
                "[run] recipe=%s origin=%s — queuing unit %d/%d hash=%s to georiva-processing",
                recipe.type, origin, i, total, unit_hash(unit)[:8],
            )
            run_unit_task.delay(
                recipe_type=recipe.type, unit=unit, origin=origin,
                unit_index=i, unit_total=total,
            )
        logger.info(
            "[run] recipe=%s origin=%s — dispatched %d unit task(s); watch '[unit i/%d]' "
            "and '[progress]' lines in the processing-worker logs",
            recipe.type, origin, total, total,
        )
        return [UnitResult(status="dispatched") for _ in units]

    return [
        run_unit(recipe, unit, worker_id=worker_id, origin=origin,
                 unit_index=i, unit_total=total)
        for i, unit in enumerate(units, 1)
    ]
