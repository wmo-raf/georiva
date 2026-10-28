"""Push-feed health: the state a row on the push-feed index reports.

The pull index ranks its states in SQL because a failed run is a column. A push feed has
no run to fail: its signals are the overdue clock (derived on read, ADR 0034) and its
rejected deliveries, so the state is resolved in Python over the handful of feeds an
organisation has.
"""

from dataclasses import dataclass
from datetime import timedelta
from enum import Enum

from django.db.models import Count, Max, Q, Sum
from django.utils import timezone
from django.utils.translation import gettext_lazy as _

#: How far back a rejected delivery still counts against a feed.
REJECTION_WINDOW = timedelta(days=7)


@dataclass(frozen=True)
class _State:
    rank: int
    label: str
    color: str  # maps to a --w-color-* status token; see template_conventions.md
    icon: str  # must be a registered Wagtail icon name


class PushHealth(Enum):
    """Push-feed health states; the ladder lives in :func:`push_health`."""

    REJECTIONS = _State(10, _("Rejections"), "critical", "cross")
    OVERDUE = _State(30, _("Overdue"), "warning", "time")
    NEW = _State(80, _("No deliveries"), "grey", "time")
    OK = _State(70, _("OK"), "positive", "check")

    @property
    def rank(self):
        return self.value.rank

    @property
    def label(self):
        return self.value.label

    @property
    def color(self):
        return self.value.color

    @property
    def icon(self):
        return self.value.icon

    @classmethod
    def from_rank(cls, rank):
        try:
            return next(state for state in cls if state.rank == rank)
        except StopIteration:
            raise KeyError(f"No PushHealth state with rank {rank!r}") from None


def push_health(feed, now=None) -> PushHealth:
    """The state of one feed fetched with :func:`annotate_push_feeds`.

    Overdue outranks rejections: missing data is the more urgent signal. A feed
    with nothing accepted and no interval to be late against is merely new.
    """
    now = now or timezone.now()
    if feed.is_overdue(now):
        return PushHealth.OVERDUE
    if feed.recent_rejections:
        return PushHealth.REJECTIONS
    if feed.last_delivery_at() is None:
        return PushHealth.NEW
    return PushHealth.OK


def annotate_push_feeds(queryset, now=None):
    """Annotate what a list reads per feed in one query: the last accepted delivery,
    accepted files and bytes, and rejections inside the window."""
    now = now or timezone.now()
    return queryset.annotate(
        last_accepted=Max("deliveries__accepted_at", filter=Q(deliveries__status="accepted")),
        accepted_files=Count("deliveries", filter=Q(deliveries__status="accepted")),
        accepted_bytes=Sum("deliveries__bytes", filter=Q(deliveries__status="accepted")),
        recent_rejections=Count(
            "deliveries",
            filter=Q(deliveries__status="rejected", deliveries__received_at__gte=now - REJECTION_WINDOW),
        ),
    )
