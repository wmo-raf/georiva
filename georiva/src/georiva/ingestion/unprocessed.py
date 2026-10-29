"""
Unprocessed-file scan (PRD #217, issue #223).

The bucket-scan phase of the periodic Sweep, extracted so the feed-scoped
"Check unprocessed files" action can run it read-only under a catalog
prefix. Classifies every conforming file in the incoming/sources buckets:

- ``untracked`` — no FileIngestion record at all (the webhook missed it)
- ``pending``   — registered but never processed (a lost dispatch)
- ``reingest``  — completed-but-dead data or an operator-forced re-ingest
- ``rewritten`` — an ``incoming`` key written again after it was ingested,
  whose notification was lost (#549)

In-flight (processing) and failed records are not the scan's business —
failures are the Sweep's bounded-retry phase.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from georiva.core.storage import BucketType, storage
from georiva.core.storage.filename import validate_path


@dataclass
class UnprocessedFile:
    bucket: str
    file_path: str
    reason: str  # 'untracked' | 'pending' | 'reingest' | 'rewritten'
    reference_time: datetime | None = None


def admit_untracked(unprocessed_file: UnprocessedFile) -> bool:
    """Whether an untracked or rewritten file may be ingested (ADR 0034).

    A file found in ``incoming`` is attributed to its push feed, or quarantined as a
    notification's would be. A failure here skips that one file, so a bad move cannot
    stop the rest of the scan; the file stays in ``incoming`` for the next one.
    """
    import logging

    from georiva.core.storage import BucketType
    from georiva.ingestion.consumer import admit_found_in_incoming

    if unprocessed_file.bucket != BucketType.INCOMING:
        return True
    try:
        return admit_found_in_incoming(unprocessed_file.file_path)
    except Exception:
        logging.getLogger(__name__).exception("Could not admit %s found in incoming", unprocessed_file.file_path)
        return False


def ingest_unprocessed(found: list[UnprocessedFile]) -> int:
    """Queue Ingestion for a find_unprocessed() result: register untracked
    files, reset reingest candidates and rewrites, and dispatch one processing task per
    file (pending files just get their lost dispatch re-sent)."""
    from georiva.core.storage import BucketType
    from georiva.ingestion import tasks
    from georiva.ingestion.consumer import admit_found_in_incoming
    from georiva.ingestion.models import FileIngestion

    queued = 0
    for unprocessed_file in found:
        if unprocessed_file.reason in ("untracked", "rewritten"):
            if unprocessed_file.bucket == BucketType.INCOMING and not admit_found_in_incoming(
                unprocessed_file.file_path
            ):
                continue
        if unprocessed_file.reason == "untracked":
            FileIngestion.register(
                bucket=unprocessed_file.bucket,
                file_path=unprocessed_file.file_path,
                reference_time=unprocessed_file.reference_time,
            )
        elif unprocessed_file.reason in ("reingest", "rewritten"):
            FileIngestion.reset_for_reingest(unprocessed_file.bucket, unprocessed_file.file_path)
        tasks.process_incoming_file.delay(
            file_path=unprocessed_file.file_path,
            origin_bucket=unprocessed_file.bucket,
            reference_time=(unprocessed_file.reference_time.isoformat() if unprocessed_file.reference_time else None),
        )
        queued += 1
    return queued


def reingest_records(records) -> int:
    """Queue reingestion for FileIngestion records: reset each (state,
    retries, results) and dispatch one processing task per file — the same
    plumbing the MinIO consumer and upload views use (PRD #217, #224)."""
    from georiva.ingestion import tasks
    from georiva.ingestion.models import FileIngestion

    count = 0
    for record in records:
        FileIngestion.reset_for_reingest(record.bucket, record.file_path)
        tasks.process_incoming_file.delay(
            file_path=record.file_path,
            origin_bucket=record.bucket,
            reference_time=(record.reference_time.isoformat() if record.reference_time else None),
        )
        count += 1
    return count


def _rewritten_since_ingested(bucket, record) -> bool:
    """True when ``incoming`` holds a write of a completed key that its ingestion never read (#549).

    A completed ingestion deletes the write it read, so another one stored there is a
    sender's later write whose notification was lost. A partial ingest keeps the write it
    read on purpose, and an ingestion that recorded no write cannot tell the two apart:
    neither is one. ``sources`` is not a sender's bucket and keeps its own rules.
    """
    from georiva.ingestion.models import FileIngestion

    if record.bucket != BucketType.INCOMING or record.status != FileIngestion.Status.COMPLETED:
        return False
    if not record.read_version:
        return False
    stored = bucket.version_of(record.file_path)
    return stored is not None and stored != record.read_version


def find_unprocessed(prefix: str | None = None) -> list[UnprocessedFile]:
    """Scan the incoming and sources buckets (optionally under a path
    prefix) and classify files needing Ingestion. Read-only: registers
    nothing, dispatches nothing."""
    from georiva.ingestion.models import FileIngestion

    found = []
    for bucket_type in [BucketType.INCOMING, BucketType.SOURCES]:
        bucket = storage.bucket(bucket_type)
        for f in bucket.list_files(recursive=True):
            path = f["path"]
            filename = Path(path).name

            if filename.startswith(".") or filename == ".keep":
                continue
            if prefix and not path.startswith(prefix):
                continue
            try:
                meta = validate_path(path)
            except ValueError:
                continue

            record = FileIngestion.objects.filter(bucket=bucket_type, file_path=path).first()

            if record is None:
                reason = "untracked"
            elif record.force_reingest or (
                record.status == FileIngestion.Status.COMPLETED and not record.has_live_data
            ):
                # force_reingest wins regardless of status — matches the
                # pre-extraction sweep.
                reason = "reingest"
            elif record.status == FileIngestion.Status.PENDING:
                reason = "pending"
            elif _rewritten_since_ingested(bucket, record):
                reason = "rewritten"
            else:
                continue

            found.append(
                UnprocessedFile(
                    bucket=bucket_type,
                    file_path=path,
                    reason=reason,
                    reference_time=meta.get("reference_time"),
                )
            )
    return found
