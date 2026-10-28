"""Deliveries a person prepared but never uploaded (ADR 0034).

A presigned upload is recorded before the browser sends a byte, so a closed tab leaves a
delivery that no file will ever answer. The consumer stops treating it as waiting once
``WAITING_WINDOW`` has passed; this rejects it, so the feed's record says what happened
and the upload session is not left active for good.
"""

from django.utils import timezone

ABANDONED_REASON = "Upload never arrived"


def reject_abandoned(now=None) -> int:
    """Reject each prepared delivery that nothing arrived for, and close its settled session; return how many."""
    from georiva.ingestion.consumer import WAITING_WINDOW
    from georiva.ingestion.models import Delivery
    from georiva.ingestion.quarantine import reject_unarrived

    now = now or timezone.now()
    stale = Delivery.objects.filter(
        status=Delivery.Status.RECEIVED,
        awaits_completion=True,
        received_at__lt=now - WAITING_WINDOW,
    ).select_related("push_feed__catalog", "session")

    sessions = {}
    rejected = 0
    for delivery in stale:
        reject_unarrived(delivery, ABANDONED_REASON)
        rejected += 1
        if delivery.session is not None:
            sessions[delivery.session_id] = delivery.session

    for session in sessions.values():
        session.close_if_settled()

    return rejected
