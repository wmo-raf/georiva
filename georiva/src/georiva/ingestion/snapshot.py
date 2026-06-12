from asgiref.sync import sync_to_async
from django.db.models import Prefetch

TERMINAL_STATUSES = frozenset(["completed", "failed"])


def _build_file_ingestion_dict(fi) -> dict:
    all_jobs = fi.jobs.all()
    latest_job = all_jobs[0] if all_jobs else None
    return {
        "id": fi.pk,
        "status": fi.status,
        "bucket": fi.bucket,
        "file_path": fi.file_path,
        "created_at": fi.created_at.isoformat(),
        "completed_at": fi.completed_at.isoformat() if fi.completed_at else None,
        "job_id": latest_job.pk if latest_job else None,
        "job_state": latest_job.state if latest_job else None,
        "variables_discovered": fi.variables_discovered,
        "valid_time_start": fi.valid_time_start.isoformat() if fi.valid_time_start else None,
        "valid_time_end": fi.valid_time_end.isoformat() if fi.valid_time_end else None,
        "timestep_count": fi.timestep_count,
        "error": fi.error or "",
    }


@sync_to_async
def _fetch_file_ingestions(terminal_limit: int) -> list[dict]:
    from georiva.ingestion.models import FileIngestion, FileIngestionJob

    base = FileIngestion.objects.prefetch_related(
        Prefetch(
            "jobs",
            queryset=FileIngestionJob.objects.order_by("-created_at"),
        )
    )
    active = list(
        base.exclude(status__in=TERMINAL_STATUSES).order_by("-created_at")
    )
    terminal = list(
        base.filter(status__in=TERMINAL_STATUSES).order_by("-created_at")[:terminal_limit]
    )

    combined = sorted(active + terminal, key=lambda fi: fi.created_at, reverse=True)
    return [_build_file_ingestion_dict(fi) for fi in combined]


async def build_ingestion_snapshot(terminal_limit: int = 10) -> list[dict]:
    """Return all active FileIngestions plus the last `terminal_limit` completed/failed ones."""
    return await _fetch_file_ingestions(terminal_limit)
