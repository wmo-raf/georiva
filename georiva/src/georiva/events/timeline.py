"""Where a chain's Timeline page lives, once it exists (#429)."""

from django.urls import NoReverseMatch, reverse


def timeline_url(correlation_id):
    try:
        return reverse("events_timeline", args=[correlation_id])
    except NoReverseMatch:
        return None
