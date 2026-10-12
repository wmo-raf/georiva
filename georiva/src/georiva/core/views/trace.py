"""From a Published Item to the chain that produced it (ADR 0030).

Nothing is added to the Item table and nothing is exposed on the public STAC API:
the link is resolved here, on demand, through the Item's own provenance.

* an **ingested** Item resolves through its source file to that file's
  ingestion events — and a file ingested more than once lists every ingestion,
  newest first, rather than guessing which one produced this Item;
* a **derived** Item resolves through the attempt that produced it.
"""

from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils.translation import gettext as _

from georiva.core.models import Item
from georiva.events.history import row_for
from georiva.events.timeline import timeline_url
from georiva.organisations.access import get_org_object_or_404
from georiva.organisations.ownership import scope_rows


def _candidates(request, item):
    """Events that could be the story of ``item``: derived attempts first, then ingestions, newest first."""
    from georiva.ingestion.models import FileIngestionFinished
    from georiva.processing.models import DerivationAttemptFinished

    derived = scope_rows(
        request,
        DerivationAttemptFinished.objects.filter(
            produced_item_id=item.pk, outcome=DerivationAttemptFinished.Outcome.COMPLETED
        ),
    ).order_by("-occurred_at")
    found = list(derived)

    if item.source_file and ":" in item.source_file:
        bucket, path = item.source_file.split(":", 1)
        ingested = scope_rows(
            request,
            FileIngestionFinished.objects.filter(
                bucket=bucket, file_path=path, outcome=FileIngestionFinished.Outcome.COMPLETED
            ),
        ).order_by("-occurred_at")
        found += list(ingested)
    return found


def item_trace(request, item_id):
    item = get_org_object_or_404(request, Item, pk=item_id)
    found = _candidates(request, item)

    if len(found) == 1:
        return redirect(timeline_url(found[0].correlation_id))
    # No match falls through to the page below: a clear "no history" state, not a 404.
    entries = [row_for(event) for event in found]
    return render(
        request,
        "core/item_trace.html",
        {
            "item": item,
            "entries": entries,
            "breadcrumbs_items": [
                {"url": reverse("wagtailadmin_home"), "label": _("Home")},
                {"url": reverse("item_preview", args=[item.pk]), "label": _("Item")},
                {"url": None, "label": _("Trace")},
            ],
            "header_title": _("Trace"),
            "header_icon": "history",
        },
    )
