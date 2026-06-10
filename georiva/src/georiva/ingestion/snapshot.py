from asgiref.sync import sync_to_async

TERMINAL_STATUSES = frozenset(["completed", "failed", "partial", "empty"])


def _build_arrival_dict(arrival) -> dict:
    file_ingestions = []
    for fi in arrival.file_ingestions.all():
        latest_job = fi.jobs.order_by("-created_at").first()
        file_ingestions.append({
            "id": fi.pk,
            "status": fi.status,
            "job_id": latest_job.pk if latest_job else None,
            "job_state": latest_job.state if latest_job else None,
        })
    collection = arrival.collection
    return {
        "id": arrival.pk,
        "status": arrival.status,
        "trigger": arrival.trigger,
        "file_path": arrival.file_path,
        "collection_name": collection.name if collection else None,
        "catalog_name": collection.catalog.name if collection else None,
        "started_at": arrival.started_at.isoformat(),
        "finished_at": arrival.finished_at.isoformat() if arrival.finished_at else None,
        "file_ingestions": file_ingestions,
    }


@sync_to_async
def _fetch_arrivals(terminal_limit: int) -> list[dict]:
    from georiva.ingestion.models import DataArrival

    base = (
        DataArrival.objects
        .select_related("collection__catalog")
        .prefetch_related("file_ingestions__jobs")
    )
    # All active arrivals (no cap — operator needs to see everything in flight).
    active = list(base.exclude(status__in=TERMINAL_STATUSES).order_by("-created_at"))
    # Only the most recent N terminal arrivals to keep the list manageable.
    terminal = list(base.filter(status__in=TERMINAL_STATUSES).order_by("-created_at")[:terminal_limit])

    combined = sorted(active + terminal, key=lambda a: a.created_at, reverse=True)
    return [_build_arrival_dict(a) for a in combined]


async def build_arrival_snapshot(terminal_limit: int = 10) -> list[dict]:
    """Return all active arrivals plus the last `terminal_limit` completed/failed ones."""
    return await _fetch_arrivals(terminal_limit)
