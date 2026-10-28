import logging
from collections import Counter

from django.contrib import messages
from django.db.models import Case, IntegerField, Value, When
from django.http import HttpResponseForbidden
from django.shortcuts import redirect, render
from django.urls import reverse, reverse_lazy
from django.utils import timezone
from django.utils.functional import cached_property
from django.utils.timesince import timesince
from django.utils.translation import gettext as _
from django.utils.translation import gettext_lazy
from wagtail.admin.ui.tables import TitleColumn
from wagtail.admin.views.generic import IndexView
from wagtail.admin.widgets import HeaderButton

from georiva.ingestion.models import PushFeed
from georiva.ingestion.push_health import PushHealth, annotate_push_feeds, push_health
from georiva.organisations.access import get_org_object_or_404
from georiva.organisations.scoping import OrgScopedViewMixin
from georiva.sources.views import _humanize_minutes

logger = logging.getLogger(__name__)


class PushFeedIndexView(OrgScopedViewMixin, IndexView):
    """Push-feed listing: the pull index's status rows, on Wagtail's generic IndexView.

    Health is resolved in Python (see ``push_health``), so the health filter and the
    health-first ordering are expressed over primary keys rather than a SQL annotation.
    """

    model = PushFeed
    paginate_by = 20
    page_title = gettext_lazy("Push feeds")
    header_icon = "upload"
    search_fields = ["name", "catalog__name"]
    index_url_name = "push_feed_list"
    index_results_url_name = "push_feed_list_results"
    template_name = "georivaingestion/push_feed_list.html"
    results_template_name = "georivaingestion/push_feed_list_results.html"

    def get_base_queryset(self):
        return annotate_push_feeds(PushFeed.objects.select_related("catalog"))

    @cached_property
    def _health_by_pk(self):
        """Every feed this organisation has, whatever the search or filter, with its state."""
        now = timezone.now()
        feeds = self.scope(annotate_push_feeds(PushFeed.objects.all(), now))
        return {feed.pk: push_health(feed, now) for feed in feeds}

    @cached_property
    def active_health(self):
        """The requested health rank, or None. Unknown values are ignored rather than 404ing."""
        try:
            rank = int(self.request.GET.get("health"))
        except (TypeError, ValueError):
            return None
        return rank if rank in {state.rank for state in PushHealth} else None

    def filter_queryset(self, queryset):
        queryset = super().filter_queryset(queryset)
        if self.active_health is not None:
            pks = [pk for pk, state in self._health_by_pk.items() if state.rank == self.active_health]
            queryset = queryset.filter(pk__in=pks)
        return queryset

    def order_queryset(self, queryset):
        # Health first, so a feed needing attention lands on page 1; pk ends it because
        # names are not unique and an unstable order repeats rows across pages.
        ranks = [When(pk=pk, then=Value(state.rank)) for pk, state in self._health_by_pk.items()]
        health_rank = (
            Case(*ranks, default=Value(PushHealth.OK.rank), output_field=IntegerField())
            if ranks
            else Value(PushHealth.OK.rank)
        )
        return queryset.annotate(health_rank=health_rank).order_by("health_rank", "name", "pk")

    def _listing_url(self, health=None):
        """A listing URL preserving search, with pagination reset."""
        params = self.request.GET.copy()
        params.pop(self.page_kwarg, None)
        if health is None:
            params.pop("health", None)
        else:
            params["health"] = health
        encoded = params.urlencode()
        return f"?{encoded}" if encoded else "?"

    def _health_chips(self):
        """Per-state counts over every feed, so a clicked chip never hides the others."""
        counts = Counter(state.rank for state in self._health_by_pk.values())
        return [
            {"state": state, "count": counts[state.rank], "url": self._listing_url(health=state.rank)}
            for state in sorted(PushHealth, key=lambda s: s.rank)
            if counts[state.rank] or state.rank == self.active_health
        ]

    def _build_row(self, feed, now):
        health = self._health_by_pk.get(feed.pk) or push_health(feed, now)
        last = feed.last_delivery_at()
        interval = feed.expected_interval
        return {
            "feed": feed,
            "health": health,
            "detail_url": reverse("push_feed_page", args=[feed.pk]),
            "delete_url": reverse("push_feed_delete", args=[feed.pk]),
            "interval_label": _humanize_minutes(int(interval.total_seconds() // 60)) if interval else "",
            # timesince returns "1 week, 5 days"; on a scan surface the leading unit is the signal.
            "last_delivery_label": timesince(last, now).split(",")[0] if last else "",
            "recent_rejections": feed.recent_rejections,
            "accepted_bytes": feed.accepted_bytes or 0,
        }

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        now = timezone.now()
        context["feed_rows"] = [self._build_row(f, now) for f in context["object_list"]]
        chips = self._health_chips()
        context["health_chips"] = chips
        context["total_count"] = sum(chip["count"] for chip in chips)
        context["all_chip_url"] = self._listing_url(health=None)
        context["active_health"] = self.active_health
        context["add_url"] = reverse("upload_wizard_step1")
        return context

    @cached_property
    def columns(self):
        return [TitleColumn("name", label=_("Push feed"), get_url=lambda f: reverse("push_feed_page", args=[f.pk]))]

    @cached_property
    def header_buttons(self):
        return [HeaderButton(label=_("New push feed"), url=reverse("upload_wizard_step1"), icon_name="plus")]

    def get_breadcrumbs_items(self):
        return [
            {"url": reverse("wagtailadmin_home"), "label": _("Home")},
            {"url": None, "label": _("Push feeds")},
        ]


push_feed_list = PushFeedIndexView.as_view()
push_feed_list_results = PushFeedIndexView.as_view(results_only=True)


def push_feed_delete(request, pk):
    from georiva.ingestion.models import PushFeed

    feed = get_org_object_or_404(request, PushFeed, pk=pk)

    if request.method == "POST":
        name = feed.name
        feed.delete()
        messages.success(request, _("Push feed '%s' deleted.") % name)
        return redirect("push_feed_list")

    return render(
        request,
        "georivaingestion/push_feed_confirm_delete.html",
        {
            "breadcrumbs_items": [
                {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
                {"url": reverse("push_feed_list"), "label": _("Push feeds")},
                {"url": None, "label": _("Delete")},
            ],
            "header_title": _("Delete push feed — %s") % feed.name,
            "header_icon": "bin",
            "feed": feed,
        },
    )


MANAGE_SENDERS = "georivaingestion.manage_push_senders"


def can_manage_senders(user) -> bool:
    return user.has_perm(MANAGE_SENDERS)


def _forbidden_unless_manager(request):
    """A plain 403: Wagtail's admin turns ``PermissionDenied`` into a redirect home."""
    if not can_manage_senders(request.user):
        return HttpResponseForbidden(_("You do not have permission to manage push senders."))


def _show_secret(request, feed, sender, secret, rotated=False):
    """Render the feed page with the new secret. This response is the only place it ever appears."""
    from georiva.ingestion.machine_senders import rclone_config
    from georiva.ingestion.upload_views import push_feed_page

    return push_feed_page(
        request,
        feed.pk,
        issued={
            "sender": sender,
            "secret": secret,
            "rotated": rotated,
            "rclone": rclone_config(feed, sender.label, sender.access_key, secret),
        },
    )


def push_sender_add(request, pk):
    from georiva.ingestion.machine_senders import add_sender
    from georiva.ingestion.models import PushFeed

    if (denied := _forbidden_unless_manager(request)) is not None:
        return denied
    feed = get_org_object_or_404(request, PushFeed.objects.select_related("catalog__organisation"), pk=pk)
    if request.method != "POST":
        return redirect("push_feed_page", pk=feed.pk)

    label = request.POST.get("label", "").strip()[:100]
    if not label:
        messages.error(request, _("Give the sender a label."))
        return redirect("push_feed_page", pk=feed.pk)

    sender, secret = add_sender(feed, label, user=request.user)
    return _show_secret(request, feed, sender, secret)


def push_sender_rotate(request, pk, sender_pk):
    from georiva.ingestion.machine_senders import rotate_sender

    if (denied := _forbidden_unless_manager(request)) is not None:
        return denied
    sender = _get_sender(request, pk, sender_pk)
    if request.method != "POST":
        return redirect("push_feed_page", pk=pk)

    secret = rotate_sender(sender, user=request.user)
    return _show_secret(request, sender.push_feed, sender, secret, rotated=True)


def push_sender_revoke(request, pk, sender_pk):
    from georiva.ingestion.machine_senders import revoke_sender

    if (denied := _forbidden_unless_manager(request)) is not None:
        return denied
    sender = _get_sender(request, pk, sender_pk)
    if request.method != "POST":
        return redirect("push_feed_page", pk=pk)

    label = sender.label
    revoke_sender(sender, user=request.user)
    messages.success(request, _("Sender '%s' revoked.") % label)
    return redirect("push_feed_page", pk=pk)


def _get_sender(request, pk, sender_pk):
    from georiva.ingestion.models import PushSender

    return get_org_object_or_404(
        request,
        PushSender.objects.select_related("push_feed__catalog__organisation"),
        pk=sender_pk,
        push_feed_id=pk,
    )


def delivery_retry(request, pk, delivery_pk):
    """Send a rejected delivery's file back to ``incoming``; it is checked again there."""
    from georiva.ingestion.models import Delivery
    from georiva.ingestion.quarantine import retry

    delivery = get_org_object_or_404(
        request,
        Delivery.objects.select_related("push_feed__catalog__organisation", "push_sender"),
        pk=delivery_pk,
        push_feed_id=pk,
    )
    if request.method != "POST":
        return redirect("push_feed_page", pk=pk)
    if delivery.status != Delivery.Status.REJECTED:
        messages.error(request, _("Only a rejected delivery can be retried."))
        return redirect("push_feed_page", pk=pk)

    try:
        retry(delivery, request.user)
    except Exception:
        logger.exception("Retry of delivery %s failed", delivery.pk)
        messages.error(request, _("The file could not be moved back. It may have expired from quarantine."))
    else:
        messages.success(request, _("'%s' sent back for ingestion.") % delivery.original_filename)
    return redirect("push_feed_page", pk=pk)
