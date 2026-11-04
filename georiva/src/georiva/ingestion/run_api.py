"""A collection's model runs over HTTP: which are complete, and which is the latest (ADR 0026, ADR 0036).

``GET /api/runs/{catalog}/{collection}/`` answers the one question every forecast
client asks — "which run do I open?" — without that client having to infer a run's
boundary from ``forecast:reference_datetime`` on items, which cannot tell a finished
run from one still arriving.

**Complete means closed.** A run is complete when its ``RunIngestion`` row is
``closed`` and incomplete while ``open``. The ``closed_by`` field says on what
evidence: ``declared_set`` and ``upload_session`` are strong claims, ``quiet_period``
and ``manual`` are weaker. The route reports every closed run as complete and leaves
a client that wants only the strong claims to filter on ``closed_by``.

``?closed_by=declared_set,upload_session`` narrows ``latest_complete`` to runs closed on
that evidence (the values of ``RunIngestion.Closer``); an unknown value is a 400. It does
not change the ``runs`` listing or each run's ``complete`` flag.

**A reopened run is incomplete again.** A late file reopens a closed run and bumps
``revision`` (ADR 0026), so it drops out of ``latest_complete`` — the client falls
back to the run before it — until it closes again. ``version`` and ``revision`` let a
client that already loaded the run notice it was republished.

**Valid times are opt-in, per run.** ``?valid_times=1`` adds each listed run's
``valid_times`` — the item times carried by that reference time, oldest first —
and ``?reference_time=<iso>`` narrows the listing to one run, which is what a
forecast viewer actually needs once the user has picked a run (issue #600).
They are opt-in because twenty runs with every lead time each is exactly the
unbounded body this answers; ``reference_time`` narrows the ``runs`` listing
only, never ``latest_complete``, the same way ``closed_by`` touches only
``latest_complete``. An unparseable ``reference_time`` is a 400; one that
matches no run is a 200 with an empty listing.

**Visibility is the collection's.** The collection is resolved through
``Collection.objects.visible_to`` under the active organisation, so a collection the
caller may not see is a 404, never an empty list.
"""

from datetime import datetime

from django.utils.dateparse import parse_datetime
from rest_framework.renderers import JSONRenderer
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from georiva.core.models import Collection, Item
from georiva.ingestion.models import RunIngestion
from georiva.organisations.access import get_org_object_or_404

DEFAULT_LIMIT = 20
MAX_LIMIT = 200

TRUE_VALUES = {"1", "true", "yes", "on"}


def _limit(request: Request) -> int:
    try:
        value = int(request.query_params.get("limit", DEFAULT_LIMIT))
    except ValueError:
        return DEFAULT_LIMIT
    return max(1, min(value, MAX_LIMIT))


def _wants_valid_times(request: Request) -> bool:
    return (request.query_params.get("valid_times") or "").strip().lower() in TRUE_VALUES


def valid_times_for(collection: Collection, reference_times: list[datetime]) -> dict[datetime, list[str]]:
    """One bounded query for the listed runs' item times, oldest first per run.

    Keyed by reference time so the caller can hand each run its own list; a run
    whose items have not arrived yet simply has none.
    """
    if not reference_times:
        return {}
    times: dict[datetime, list[str]] = {ref: [] for ref in reference_times}
    rows = (
        Item.objects.filter(collection=collection, reference_time__in=reference_times)
        .order_by("reference_time", "time")
        .values_list("reference_time", "time")
    )
    for reference_time, time in rows:
        times.setdefault(reference_time, []).append(time.isoformat())
    return times


def serialize_run(run: RunIngestion, valid_times: dict[datetime, list[str]] | None = None) -> dict:
    data = {
        "reference_time": run.reference_time.isoformat(),
        "complete": run.is_complete,
        "status": run.status,
        "revision": run.revision,
        "version": run.version,
        "closed_by": run.closed_by or None,
        "closed_at": run.closed_at.isoformat() if run.closed_at else None,
        "file_count": run.file_count,
        "expected_file_count": run.expected_file_count,
        "last_file_at": run.last_file_at.isoformat() if run.last_file_at else None,
    }
    if valid_times is not None:
        data["valid_times"] = valid_times.get(run.reference_time, [])
    return data


class RunListView(APIView):
    """GET /api/runs/{catalog_slug}/{collection_slug}/ — newest first, plus ``latest_complete``."""

    renderer_classes = [JSONRenderer]

    def get(self, request: Request, catalog_slug: str, collection_slug: str) -> Response:
        collection = get_org_object_or_404(
            request,
            Collection.objects.visible_to(request),
            catalog__slug=catalog_slug,
            slug=collection_slug,
        )
        closers = request.query_params.get("closed_by")
        complete = RunIngestion.objects.filter(collection=collection, status=RunIngestion.COMPLETE_STATUS)
        if closers:
            wanted = {c.strip() for c in closers.split(",") if c.strip()}
            unknown = wanted - set(RunIngestion.Closer.values)
            if unknown:
                return Response(
                    {
                        "detail": f"Unknown closed_by: {', '.join(sorted(unknown))}.",
                        "allowed": RunIngestion.Closer.values,
                    },
                    status=400,
                )
            complete = complete.filter(closed_by__in=wanted)
        latest = complete.order_by("-reference_time").first()
        runs = RunIngestion.objects.filter(collection=collection)

        wanted_reference_time = request.query_params.get("reference_time")
        if wanted_reference_time:
            parsed = parse_datetime(wanted_reference_time)
            if parsed is None:
                return Response(
                    {"detail": f"Unparseable reference_time: {wanted_reference_time}. Expected ISO 8601."},
                    status=400,
                )
            runs = runs.filter(reference_time=parsed)

        listed = list(runs.order_by("-reference_time")[: _limit(request)])

        valid_times = None
        if _wants_valid_times(request):
            references = [run.reference_time for run in listed]
            if latest and latest.reference_time not in references:
                references.append(latest.reference_time)
            valid_times = valid_times_for(collection, references)

        return Response(
            {
                "catalog": catalog_slug,
                "collection": collection_slug,
                "latest_complete": serialize_run(latest, valid_times) if latest else None,
                "runs": [serialize_run(run, valid_times) for run in listed],
            }
        )
