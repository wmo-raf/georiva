"""A model run's page: its current state, and its history (ADR 0026, ADR 0030)."""

from django.shortcuts import render
from django.urls import reverse

from georiva.events.history import build_history
from georiva.ingestion.models import RunIngestion, RunIngestionClosed, RunIngestionReopened
from georiva.organisations.access import get_org_object_or_404


def run_ingestion_detail(request, run_ingestion_id):
    run = get_org_object_or_404(
        request,
        RunIngestion.objects.select_related("collection__catalog"),
        pk=run_ingestion_id,
    )
    history = build_history(
        request,
        [
            (RunIngestionClosed, {"run": run}),
            (RunIngestionReopened, {"run": run}),
        ],
    )
    return render(
        request,
        "georivaingestion/run_ingestion_detail.html",
        {
            "run": run,
            "history": history,
            "breadcrumbs_items": [
                {"url": reverse("wagtailadmin_home"), "label": "Home"},
                {"url": None, "label": f"Run — {run}"},
            ],
            "header_title": f"Run — {run}",
            "header_icon": "history",
        },
    )
