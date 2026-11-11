"""A derived item follows its inputs out (ADR 0041).

Deleting an input — a StagingItem, or a published Item that fed a further
derivation — cascades its outgoing ``DerivationLink`` rows away. Nothing would
then know which derived items it fed, so the lineage is read *first*:

1. ``on_input_deleting`` (a ``pre_delete`` receiver on both input models) finds
   the ``DerivationRun`` rows whose produced item was derived from the instance
   and adds them to a buffer held on the connection for the current
   transaction. The first one registers a single ``on_commit`` flush, so a bulk
   delete of thirty dailies feeding one climatology re-runs it once; a rollback
   drops the flush, and the buffer with it.
2. The flush re-dispatches each unit whose produced item still exists, with the
   ``input_deleted`` reason. A derived item deleted in the same transaction as
   its input has nothing left to re-derive.
3. The engine re-runs the unit like any other: ready on the remaining inputs, it
   recomputes; not ready, it retracts the item (``engine._retract``). Retracting
   deletes a published Item, which lands back here — that is how the rule
   carries forward through intermediates.
"""

from __future__ import annotations

import logging

from django.db import connections, transaction

logger = logging.getLogger(__name__)

_BUFFER_ATTR = "_georiva_pending_input_deletions"


class _PendingRedispatch:
    """The DerivationRun pks one transaction's input deletes reach, re-run on its commit."""

    def __init__(self, using: str):
        self.using = using
        self.run_ids: set[int] = set()

    def flush(self):
        connection = connections[self.using]
        if getattr(connection, _BUFFER_ATTR, None) is self:
            delattr(connection, _BUFFER_ATTR)
        redispatch(self.run_ids)


def _pending_for(using: str) -> _PendingRedispatch:
    """This transaction's buffer, registering its flush on first use.

    A buffer whose transaction rolled back is still on the connection, but its
    flush is no longer pending — it is stale and replaced, not reused (the same
    discipline as ``core.storage.asset_deletion``).
    """
    connection = connections[using]
    pending = getattr(connection, _BUFFER_ATTR, None)
    if pending is not None and any(func == pending.flush for _, func, _ in connection.run_on_commit):
        return pending

    pending = _PendingRedispatch(using)
    setattr(connection, _BUFFER_ATTR, pending)
    return pending


def _runs_derived_from(instance) -> list[int]:
    from georiva.staging.models import DerivationLink, StagingItem

    from .models import DerivationRun

    source = "source_staging_item" if isinstance(instance, StagingItem) else "source_published_item"
    derived = DerivationLink.objects.filter(**{source: instance.pk}).values("derived_item_id")
    return list(DerivationRun.objects.filter(produced_item_id__in=derived).values_list("pk", flat=True))


def on_input_deleting(sender, instance, using, **kwargs):
    run_ids = _runs_derived_from(instance)
    if not run_ids:
        return
    pending = _pending_for(using)
    first = not pending.run_ids
    pending.run_ids.update(run_ids)
    # Registered after the update: outside a transaction on_commit runs at once.
    if first:
        transaction.on_commit(pending.flush, using=using)


def redispatch(run_ids) -> int:
    """Re-run each unit whose produced item outlived the delete. Returns how many."""
    from .invocation import _dispatch_unit
    from .models import DerivationRun

    runs = DerivationRun.objects.filter(pk__in=run_ids, produced_item__isnull=False)
    count = 0
    for run_rec in runs:
        _dispatch_unit(run_rec.recipe_type, run_rec.unit_key, reason=DerivationRun.RetryReason.INPUT_DELETED)
        count += 1
    if count:
        logger.info("Input deleted: re-dispatched %d derived unit(s)", count)
    return count
