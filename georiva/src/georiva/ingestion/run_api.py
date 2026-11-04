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

**A reopened run is incomplete again.** A late file reopens a closed run and bumps
``revision`` (ADR 0026), so it drops out of ``latest_complete`` — the client falls
back to the run before it — until it closes again. ``version`` and ``revision`` let a
client that already loaded the run notice it was republished.

**Visibility is the collection's.** The collection is resolved through
``Collection.objects.visible_to`` under the active organisation, so a collection the
caller may not see is a 404, never an empty list.
"""

from rest_framework.renderers import JSONRenderer
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from georiva.core.models import Collection
from georiva.ingestion.models import RunIngestion
from georiva.organisations.access import get_org_object_or_404

DEFAULT_LIMIT = 20
MAX_LIMIT = 200


def _limit(request: Request) -> int:
    try:
        value = int(request.query_params.get("limit", DEFAULT_LIMIT))
    except ValueError:
        return DEFAULT_LIMIT
    return max(1, min(value, MAX_LIMIT))


def serialize_run(run: RunIngestion) -> dict:
    return {
        "reference_time": run.reference_time.isoformat(),
        "complete": run.status == RunIngestion.Status.CLOSED,
        "status": run.status,
        "revision": run.revision,
        "version": run.version,
        "closed_by": run.closed_by or None,
        "closed_at": run.closed_at.isoformat() if run.closed_at else None,
        "file_count": run.file_count,
        "expected_file_count": run.expected_file_count,
        "last_file_at": run.last_file_at.isoformat() if run.last_file_at else None,
    }


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
        runs = RunIngestion.objects.filter(collection=collection)
        latest = runs.filter(status=RunIngestion.Status.CLOSED).order_by("-reference_time").first()
        return Response(
            {
                "catalog": catalog_slug,
                "collection": collection_slug,
                "latest_complete": serialize_run(latest) if latest else None,
                "runs": [serialize_run(r) for r in runs.order_by("-reference_time")[: _limit(request)]],
            }
        )
