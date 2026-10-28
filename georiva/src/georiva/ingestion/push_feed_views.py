import logging

from django.contrib import messages
from django.http import HttpResponseForbidden
from django.shortcuts import redirect, render
from django.urls import reverse, reverse_lazy
from django.utils.translation import gettext as _

from georiva.organisations.access import (
    get_org_object_or_404,
    scoped_queryset,
)

logger = logging.getLogger(__name__)


def push_feed_list(request):
    from georiva.ingestion.models import PushFeed

    feeds = list(
        PushFeed.with_last_delivery(scoped_queryset(request, PushFeed.objects.all()))
        .select_related("catalog")
        .order_by("catalog__name", "name")
    )
    for feed in feeds:
        feed.overdue = feed.overdue_text()

    return render(
        request,
        "georivaingestion/push_feed_list.html",
        {
            # Rendered by the slim header via wagtailadmin/generic/base.html.
            "breadcrumbs_items": [
                {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
                {"url": None, "label": _("Push feeds")},
            ],
            "header_title": _("Push feeds"),
            "header_icon": "upload",
            "add_url": reverse("upload_wizard_step1"),
            "feeds": feeds,
        },
    )


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
