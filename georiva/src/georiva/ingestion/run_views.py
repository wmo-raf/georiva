"""A model run's page: its current state, and its history (ADR 0026, ADR 0030)."""

from django.shortcuts import render
from django.urls import reverse

from georiva.core.models import ProductPublished
from georiva.events.history import build_history
from georiva.ingestion.models import (
    FileIngestion,
    FileIngestionFinished,
    RunIngestion,
    RunIngestionClosed,
    RunIngestionReopened,
)
from georiva.organisations.access import get_org_object_or_404, get_via_scoped_parent_or_404, require_active_org


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


def file_ingestion_detail(request, file_ingestion_id):
    """A file's current state and every ending it has had (ADR 0030).

    ``FileIngestion`` is written before anything has decided which collection —
    and so which organisation — it belongs to, so it has no ORM path to one. Its
    owner is the leading segment of its key, which is what scopes this lookup.
    """
    organisation = require_active_org(request)
    log = get_via_scoped_parent_or_404(
        FileIngestion.objects.filter(file_path__startswith=f"{organisation.slug}/"),
        pk=file_ingestion_id,
    )
    history = build_history(
        request,
        [
            (FileIngestionFinished, {"file_ingestion": log}),
            (ProductPublished, {"source_file": f"{log.bucket}:{log.file_path}"}),
        ],
    )
    return render(
        request,
        "georivaingestion/file_ingestion_detail.html",
        {
            "log": log,
            "history": history,
            "breadcrumbs_items": [
                {"url": reverse("wagtailadmin_home"), "label": "Home"},
                {"url": None, "label": f"File — {log.file_path}"},
            ],
            "header_title": f"File — {log.file_path}",
            "header_icon": "doc-empty",
        },
    )
