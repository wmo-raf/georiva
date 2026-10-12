"""The inputs behind a Derivation attempt's product (ADR 0030).

A Timeline follows what *triggered* an attempt. Inputs that went into the product
without triggering it are not in the chain; they are reached through
``DerivationLink`` lineage, which is what this page lists.
"""

from django.http import Http404
from django.shortcuts import render
from django.urls import reverse

from georiva.organisations.ownership import scope_rows
from georiva.processing.models import DerivationAttemptFinished
from georiva.staging.models import DerivationLink


def attempt_inputs(request, event_id):
    attempt = scope_rows(request, DerivationAttemptFinished.objects.filter(pk=event_id)).first()
    if attempt is None:
        raise Http404("No such attempt.")

    links = []
    if attempt.produced_item_id is not None:
        links = list(
            DerivationLink.objects.filter(derived_item_id=attempt.produced_item_id).select_related(
                "source_staging_item__collection", "source_published_item__collection"
            )
        )
    inputs = []
    for link in links:
        if link.source_staging_item_id:
            item = link.source_staging_item
            inputs.append(
                {
                    "tier": "staging",
                    "collection": item.collection.slug,
                    "time": item.datetime or item.start_datetime,
                }
            )
        elif link.source_published_item_id:
            item = link.source_published_item
            inputs.append({"tier": "published", "collection": item.collection.slug, "time": item.time})

    return render(
        request,
        "georivaprocessing/attempt_inputs.html",
        {
            "attempt": attempt,
            "inputs": inputs,
            "breadcrumbs_items": [
                {"url": reverse("wagtailadmin_home"), "label": "Home"},
                {"url": reverse("events_timeline", args=[attempt.correlation_id]), "label": "Timeline"},
                {"url": None, "label": "Inputs"},
            ],
            "header_title": "Inputs",
            "header_icon": "doc-full",
        },
    )
