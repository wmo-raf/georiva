from django.shortcuts import render
from django.urls import reverse


def ingestion_activity_feed(request):
    return render(request, "georivaingestion/activity_feed.html", {
        "breadcrumbs_items": [
            {"url": reverse("wagtailadmin_home"), "label": "Home"},
            {"url": "", "label": "Ingestion Activity"},
        ],
    })


def acquisition_feed(request):
    return render(request, "georivaingestion/acquisition_feed.html", {
        "breadcrumbs_items": [
            {"url": reverse("wagtailadmin_home"), "label": "Home"},
            {"url": "", "label": "Acquisition Feed"},
        ],
    })
