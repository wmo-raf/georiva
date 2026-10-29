"""Rejecting a delivery out of ``incoming``, and sending it back (ADR 0034).

A rejected file moves to the quarantine bucket under the key it arrived with, where
neither the consumer nor the Sweep looks. Retry moves it back as a new delivery,
which the consumer checks like any other.
"""

import logging
from pathlib import Path

from django.db import transaction

from georiva.core.storage import storage
from georiva.events.context import ensure_context, use_context
from georiva.events.recording import record
from georiva.ingestion.models import Delivery, DeliveryRejected, organisation_id_for_key

logger = logging.getLogger(__name__)

NOT_A_SENDER = "not written by a sender of this feed"


def quarantine(key: str, reason: str, *, feed=None, sender=None, sender_version: str = "", size: int = 0) -> None:
    """Move ``key`` out of ``incoming`` and record why.

    With a push feed known, the delivery is recorded as rejected: a delivery still
    waiting on this key (a Retry's) is the one rejected, otherwise a new one is made.
    The event belongs to the key's organisation, or to nobody when there is none.
    """
    # The move comes first: a file that stayed behind would be revived by the Sweep.
    storage.move_between(storage.incoming, storage.quarantine, key)
    logger.warning("Quarantined %s: %s", key, reason)

    filename = Path(key).name
    with ensure_context(actor="system:bucket", organisation_id=organisation_id_for_key(key)):
        with transaction.atomic():
            delivery = None
            if feed is not None:
                delivery = _waiting_delivery(key) or Delivery.receive(
                    push_feed=feed, filename=filename, file_path=key, push_sender=sender, sender_version=sender_version
                )
                delivery.mark_rejected(reason, bytes=size)
            _record_rejection(key, reason, feed=feed, delivery=delivery, sender=sender)


def reject_unarrived(delivery: Delivery, reason: str) -> None:
    """Reject a delivery whose file never reached ``incoming``: there is nothing to move."""
    with transaction.atomic():
        delivery.mark_rejected(reason)
        _record_rejection(delivery.file_path, reason, feed=delivery.push_feed, delivery=delivery, sender=None)


def _waiting_delivery(key: str):
    return Delivery.objects.filter(file_path=key, status=Delivery.Status.RECEIVED).order_by("-id").first()


def _record_rejection(key, reason, *, feed, delivery, sender):
    # A delivery has its own chain, so its rejection joins it; otherwise the context's own.
    from georiva.events.context import new_context

    fields = dict(
        delivery=delivery,
        push_feed=feed,
        feed_name=feed.name if feed else "",
        file_path=key,
        reason=reason,
        sender=delivery.sender_name if delivery else (sender.label if sender else ""),
    )
    if delivery is None:
        record(DeliveryRejected, **fields)
        return
    context = new_context(
        actor="system:bucket",
        organisation_id=feed.catalog.organisation_id,
        correlation_id=delivery.correlation_id,
    )
    with use_context(context):
        record(DeliveryRejected, **fields)


def retry(delivery: Delivery, user) -> Delivery:
    """Send a rejected delivery's file back to ``incoming`` as a new delivery.

    The new delivery is recorded before the move, as an upload's is: the move raises a
    Bucket notification, and the consumer recognises the file as GeoRiva's own by it.
    """
    if delivery.status != Delivery.Status.REJECTED:
        raise ValueError("Only a rejected delivery can be retried.")

    if _waiting_delivery(delivery.file_path) is not None:
        raise ValueError("This file has already been sent back.")

    fresh = Delivery.receive(
        push_feed=delivery.push_feed,
        filename=delivery.original_filename,
        file_path=delivery.file_path,
        sent_by=user,
        push_sender=delivery.push_sender,
    )
    try:
        storage.move_between(storage.quarantine, storage.incoming, delivery.file_path)
    except Exception:
        fresh.delete()
        raise
    return fresh
