from asgiref.sync import sync_to_async


@sync_to_async
def _fetch_arrivals(limit: int) -> list[dict]:
    from georiva.ingestion.models import DataArrival

    arrivals = (
        DataArrival.objects
        .prefetch_related("file_ingestions")
        .order_by("-created_at")[:limit]
    )

    result = []
    for arrival in arrivals:
        file_ingestions = [
            {"id": fi.pk, "status": fi.status}
            for fi in arrival.file_ingestions.all()
        ]
        result.append({
            "id": arrival.pk,
            "status": arrival.status,
            "trigger": arrival.trigger,
            "started_at": arrival.started_at.isoformat(),
            "finished_at": arrival.finished_at.isoformat() if arrival.finished_at else None,
            "file_ingestions": file_ingestions,
        })
    return result


async def build_arrival_snapshot(limit: int = 50) -> list[dict]:
    """Return the last `limit` DataArrivals with their FileIngestion summaries."""
    return await _fetch_arrivals(limit)
