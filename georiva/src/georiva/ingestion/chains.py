"""Which chain does an arriving file belong to? (ADR 0030)

The fetch → ingestion hop crosses object storage, not the task queue, so no task
header survives it. It is bridged by lookup instead: the Bucket-notification
consumer, and the unprocessed-file sweep, find the most recent fetched-file (or
uploaded-file) record for the key and adopt that fetch's Correlation and
organisation. No match means a drop-zone arrival — a fresh chain, honestly
recorded as "arrived in the bucket".

Known limitation: "most recent record for this key" is a heuristic. It is wrong
only when two fetches of one key are in flight at once, which the file-ingestion
lock largely prevents.
"""

from georiva.events.context import EventContext, new_context
from georiva.ingestion.models import organisation_id_for_key


def context_for_revival(bucket: str, file_path: str, sweep: str) -> EventContext:
    """The context a sweep reviving ``file_path`` should act under.

    It reuses the Correlation and organisation of the file's most recent event, as
    the sweep and with no Caused-by, so a recovery does not split one story in two.
    A file with no prior event is handled as a fresh arrival would be.
    """
    from georiva.ingestion.models import FileIngestionFinished

    actor = f"sweep:{sweep}"
    last = FileIngestionFinished.objects.filter(bucket=bucket, file_path=file_path).order_by("-occurred_at").first()
    if last is not None:
        return EventContext(correlation_id=last.correlation_id, actor=actor, organisation_id=last.organisation_id)
    return context_for_arrival(file_path, actor=actor)


def context_for_arrival(file_path: str, *, actor: str) -> EventContext:
    """The context an arrival of ``file_path`` should be handled under."""
    adopted = _adopt_latest(file_path, actor)
    if adopted is not None:
        return adopted
    return new_context(actor=actor, organisation_id=organisation_id_for_key(file_path))


def _adopt_latest(file_path: str, actor: str) -> EventContext | None:
    """The chain of whichever stored the key last: a fetch or a person's upload."""
    from georiva.ingestion.models import UploadedFile

    upload = (
        UploadedFile.objects.filter(file_path=file_path, correlation_id__isnull=False)
        .select_related("session__catalog", "session__user")
        .order_by("-id")
        .first()
    )
    fetch = _adopt_fetch(file_path, actor)
    if upload is None:
        return fetch
    if fetch is not None and not _upload_is_newer(upload, file_path):
        return fetch
    session = upload.session
    return new_context(
        actor=f"user:{session.user_id}" if session.user_id else actor,
        organisation_id=session.catalog.organisation_id,
        correlation_id=upload.correlation_id,
    )


def _reached_rows(file_path: str):
    """The FetchedFiles for this key that a run actually reached. A run plans every
    row up front (#474), so a Waiting row, or one failed because its run stopped
    before it, says nothing about who stored the key."""
    from georiva.sources.models import FetchedFile

    return (
        FetchedFile.objects.filter(file_path=file_path)
        .exclude(status=FetchedFile.Status.PENDING)
        .exclude(status=FetchedFile.Status.FAILED, started_at__isnull=True)
    )


def _upload_is_newer(upload, file_path: str) -> bool:
    """Whether the upload stored this key after the latest fetch did."""
    fetched = _reached_rows(file_path).order_by("-id").first()
    fetched_at = fetched.completed_at or fetched.started_at if fetched else None
    return fetched_at is None or (upload.started_at is not None and upload.started_at >= fetched_at)


def _adopt_fetch(file_path: str, actor: str) -> EventContext | None:
    from georiva.sources.models import FetchRunFinished

    fetched = (
        _reached_rows(file_path)
        .filter(fetch_run__correlation_id__isnull=False)
        .select_related("fetch_run__data_feed__catalog")
        .order_by("-id")
        .first()
    )
    if fetched is None:
        return None
    run = fetched.fetch_run
    # The fetch may still be running when its file lands, in which case there is no
    # event to point at yet — the chain is still the same chain.
    finished = FetchRunFinished.objects.filter(fetch_run=run).order_by("-occurred_at").first()
    return new_context(
        actor=actor,
        organisation_id=run.data_feed.catalog.organisation_id,
        correlation_id=run.correlation_id,
        caused_by=finished.pk if finished else None,
    )
