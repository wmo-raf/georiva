from django.contrib import messages
from django.shortcuts import redirect, render
from django.urls import reverse, reverse_lazy
from django.utils.translation import gettext as _

from georiva.organisations.access import (
    get_org_object_or_404,
    scoped_queryset,
)


def push_feed_list(request):
    from georiva.ingestion.models import PushFeed

    feeds = scoped_queryset(request, PushFeed.objects.all()).select_related("catalog").order_by("catalog__name", "name")

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
