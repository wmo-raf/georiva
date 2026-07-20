from django.shortcuts import render
from django.urls import reverse


def ingestion_activity_feed(request):
    return render(request, "georivaingestion/activity_feed.html", {
        # Rendered by the slim header via wagtailadmin/generic/base.html.
        "breadcrumbs_items": [
            {"url": reverse("wagtailadmin_home"), "label": "Home"},
            {"url": None, "label": "Ingestion Activity"},
        ],
        "header_title": "Ingestion Activity",
        "header_icon": "history",
    })


def acquisition_feed(request):
    return render(request, "georivaingestion/acquisition_feed.html", {
        # Rendered by the slim header via wagtailadmin/generic/base.html.
        "breadcrumbs_items": [
            {"url": reverse("wagtailadmin_home"), "label": "Home"},
            {"url": None, "label": "Acquisition Feed"},
        ],
        "header_title": "Acquisition Feed",
        "header_icon": "download",
    })
