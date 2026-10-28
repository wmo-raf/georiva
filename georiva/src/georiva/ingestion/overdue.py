"""Overdue push feeds: one failed ``PushFeedOverdue`` event per overdue spell (ADR 0034)."""

from django.db import transaction
from django.utils import timezone

from georiva.events.context import new_context, use_context
from georiva.events.recording import record


def record_overdue_feeds(now=None) -> int:
    """Record ``PushFeedOverdue`` for each feed that has newly become overdue; return how many.

    A spell is the stretch since the feed's overdue reference (its last accepted
    delivery, else when the interval was set). An event already recorded since
    that reference means the spell is already on the record; an accepted
    delivery moves the reference past it, so the next spell records again.
    """
    from georiva.ingestion.models import PushFeed, PushFeedOverdue

    now = now or timezone.now()
    feeds = PushFeed.with_last_delivery(PushFeed.objects.filter(expected_interval__isnull=False)).select_related(
        "catalog"
    )
    recorded = 0
    for feed in feeds:
        if not feed.is_overdue(now):
            continue
        with transaction.atomic():
            # Serialise concurrent runs on the feed row so a spell is recorded once.
            PushFeed.objects.select_for_update().get(pk=feed.pk)
            if PushFeedOverdue.objects.filter(push_feed=feed, occurred_at__gte=feed.overdue_reference()).exists():
                continue
            context = new_context(actor="sweep:push_feed_overdue", organisation_id=feed.catalog.organisation_id)
            with use_context(context):
                record(
                    PushFeedOverdue,
                    push_feed=feed,
                    feed_name=feed.name,
                    catalog_slug=feed.catalog.slug,
                    expected_interval=feed.expected_interval,
                    last_delivery_at=feed.last_delivery_at(),
                    occurred_at=now,
                )
        recorded += 1
    return recorded
