"""
Shared low-level builders for provisioning core Variables.

Both provisioning paths — the DataFeed setup path (definition-is-truth) and the
Manual Upload Setup Wizard (operator-is-truth) — and any variable editor built
on top of them create the same underlying records: Units resolved from loose
symbols, and Variable ``sources`` StreamField blocks. These helpers are that
common mechanical layer.

Deliberately NOT here: the upsert semantics. The DataFeed path re-provisions
with update_or_create (the plugin definition is the source of truth); the
manual path uses get_or_create (the operator is the source of truth and
hand-tuned Variables are never clobbered). Those contracts live with their
services and must stay distinct.
"""
import logging

logger = logging.getLogger(__name__)


def resolve_unit(symbol: str):
    """
    Resolve a Unit from a loose symbol, creating it if necessary.

    Matching is deliberately forgiving: case-insensitive on symbol (plugins may
    say "Dimensionless" where the DB stores "dimensionless"), then
    case-insensitive on display name (some plugins pass the name rather than
    the pint symbol). Safe under concurrent creation.
    """
    from django.db import IntegrityError

    from georiva.core.models import Unit

    unit = Unit.objects.filter(symbol__iexact=symbol).first()
    if unit:
        return unit

    unit = Unit.objects.filter(name__iexact=symbol).first()
    if unit:
        return unit

    try:
        unit = Unit.objects.create(symbol=symbol, name=symbol)
        logger.info("Created Unit: %s", symbol)
        return unit
    except IntegrityError:
        # Concurrent worker created the same unit; re-fetch it.
        return (
            Unit.objects.filter(symbol__iexact=symbol).first()
            or Unit.objects.filter(name__iexact=symbol).first()
        )


def build_source_block(block_type: str, source_name: str,
                       vertical_dimension: str = "", vertical_value=None) -> dict:
    """One Variable ``sources`` StreamField block in its canonical dict form."""
    return {
        "type": block_type,
        "value": {
            "source_name": source_name,
            "vertical_dimension": vertical_dimension,
            "vertical_value": vertical_value,
        },
    }


def passthrough_sources(source_name: str) -> list:
    """The ``sources`` value for a passthrough Variable: one primary block."""
    return [build_source_block("primary", source_name)]
