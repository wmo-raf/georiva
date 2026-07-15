from asgiref.sync import sync_to_async

TERMINAL_FETCH_STATUSES = frozenset(["completed", "failed", "cancelled"])
TERMINAL_UPLOAD_STATUSES = frozenset(["completed", "failed", "cancelled"])


def _build_fetch_run_dict(run) -> dict:
    files = [
        {
            "id": ff.pk,
            "file_path": ff.file_path,
            "status": ff.status,
            "bytes_transferred": ff.bytes_transferred,
            "error": ff.error or "",
        }
        for ff in run.fetched_files.all()
    ]
    feed = run.data_feed
    return {
        "type": "fetch_run",
        "id": run.pk,
        "status": run.status,
        "data_feed_id": feed.pk if feed else None,
        "data_feed_name": feed.name if feed else None,
        "started_at": run.started_at.isoformat(),
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "files_fetched": run.files_fetched,
        "files_skipped": run.files_skipped,
        "files_failed": run.files_failed,
        "bytes_transferred": run.bytes_transferred,
        "files": files,
    }


def _build_upload_session_dict(session) -> dict:
    files = [
        {
            "id": uf.pk,
            "original_filename": uf.original_filename,
            "status": uf.status,
            "file_path": uf.file_path,
            "error": uf.error or "",
        }
        for uf in session.uploaded_files.all()
    ]
    catalog = session.catalog
    return {
        "type": "upload_session",
        "id": session.pk,
        "status": session.status,
        "catalog_name": catalog.name if catalog else None,
        "user_display": session.user.get_full_name() or session.user.username if session.user_id else None,
        "started_at": session.started_at.isoformat(),
        "completed_at": session.completed_at.isoformat() if session.completed_at else None,
        "files": files,
    }


@sync_to_async
def _fetch_acquisition_items(terminal_limit: int) -> list[dict]:
    from django.db.models import Prefetch
    from georiva.sources.models import FetchRun, FetchedFile
    from georiva.ingestion.models import UploadSession, UploadedFile

    # FetchRuns
    active_runs = list(
        FetchRun.objects
        .select_related("data_feed")
        .prefetch_related(Prefetch("fetched_files", queryset=FetchedFile.objects.order_by("id")))
        .exclude(status__in=TERMINAL_FETCH_STATUSES)
        .order_by("-started_at")
    )
    terminal_runs = list(
        FetchRun.objects
        .select_related("data_feed")
        .prefetch_related(Prefetch("fetched_files", queryset=FetchedFile.objects.order_by("id")))
        .filter(status__in=TERMINAL_FETCH_STATUSES)
        .order_by("-started_at")[:terminal_limit]
    )

    # UploadSessions
    active_sessions = list(
        UploadSession.objects
        .select_related("catalog", "user")
        .prefetch_related(Prefetch("uploaded_files", queryset=UploadedFile.objects.order_by("id")))
        .exclude(status__in=TERMINAL_UPLOAD_STATUSES)
        .order_by("-started_at")
    )
    terminal_sessions = list(
        UploadSession.objects
        .select_related("catalog", "user")
        .prefetch_related(Prefetch("uploaded_files", queryset=UploadedFile.objects.order_by("id")))
        .filter(status__in=TERMINAL_UPLOAD_STATUSES)
        .order_by("-started_at")[:terminal_limit]
    )

    items = (
        [_build_fetch_run_dict(r) for r in active_runs + terminal_runs] +
        [_build_upload_session_dict(s) for s in active_sessions + terminal_sessions]
    )
    items.sort(key=lambda x: x["started_at"], reverse=True)
    return items


async def build_acquisition_snapshot(terminal_limit: int = 10) -> list[dict]:
    """Return all active FetchRuns and UploadSessions plus last `terminal_limit` of each."""
    return await _fetch_acquisition_items(terminal_limit)
