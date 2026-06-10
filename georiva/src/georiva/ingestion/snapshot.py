from asgiref.sync import sync_to_async


@sync_to_async
def _fetch_arrivals(limit: int) -> list[dict]:
    from georiva.ingestion.models import DataArrival

    arrivals = (
        DataArrival.objects
        .select_related("collection__catalog")
        .prefetch_related("file_ingestions__jobs")
        .order_by("-created_at")[:limit]
    )

    result = []
    for arrival in arrivals:
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
        result.append({
            "id": arrival.pk,
            "status": arrival.status,
            "trigger": arrival.trigger,
            "file_path": arrival.file_path,
            "collection_name": collection.name if collection else None,
            "catalog_name": collection.catalog.name if collection else None,
            "started_at": arrival.started_at.isoformat(),
            "finished_at": arrival.finished_at.isoformat() if arrival.finished_at else None,
            "file_ingestions": file_ingestions,
        })
    return result


async def build_arrival_snapshot(limit: int = 50) -> list[dict]:
    """Return the last `limit` DataArrivals with their FileIngestion and job summaries."""
    return await _fetch_arrivals(limit)
