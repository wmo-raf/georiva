"""
Unprocessed-file scan (PRD #217, issue #223).

The bucket-scan phase of the periodic Sweep, extracted so the feed-scoped
"Check unprocessed files" action can run it read-only under a catalog
prefix. Classifies every conforming file in the incoming/sources buckets:

- ``untracked`` — no FileIngestion record at all (the webhook missed it)
- ``pending``   — registered but never processed (a lost dispatch)
- ``reingest``  — completed-but-dead data or an operator-forced re-ingest

In-flight (processing) and failed records are not the scan's business —
failures are the Sweep's bounded-retry phase.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from georiva.core.filename import validate_path
from georiva.core.storage import BucketType, storage


@dataclass
class UnprocessedFile:
    bucket: str
    file_path: str
    reason: str  # 'untracked' | 'pending' | 'reingest'
    reference_time: Optional[datetime] = None


def ingest_unprocessed(found: list[UnprocessedFile]) -> int:
    """Queue Ingestion for a find_unprocessed() result: register untracked
    files, reset reingest candidates, and dispatch one processing task per
    file (pending files just get their lost dispatch re-sent)."""
    from georiva.ingestion import tasks
    from georiva.ingestion.models import FileIngestion

    for unprocessed_file in found:
        if unprocessed_file.reason == "untracked":
            FileIngestion.register(
                bucket=unprocessed_file.bucket,
                file_path=unprocessed_file.file_path,
                reference_time=unprocessed_file.reference_time,
            )
        elif unprocessed_file.reason == "reingest":
            FileIngestion.reset_for_reingest(
                unprocessed_file.bucket, unprocessed_file.file_path
            )
        tasks.process_incoming_file.delay(
            file_path=unprocessed_file.file_path,
            origin_bucket=unprocessed_file.bucket,
            reference_time=(
                unprocessed_file.reference_time.isoformat()
                if unprocessed_file.reference_time else None
            ),
        )
    return len(found)


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
            reference_time=(
                record.reference_time.isoformat()
                if record.reference_time else None
            ),
        )
        count += 1
    return count


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

            record = FileIngestion.objects.filter(
                bucket=bucket_type, file_path=path
            ).first()

            if record is None:
                reason = "untracked"
            elif record.force_reingest or (
                record.status == FileIngestion.Status.COMPLETED
                and not record.has_live_data
            ):
                # force_reingest wins regardless of status — matches the
                # pre-extraction sweep.
                reason = "reingest"
            elif record.status == FileIngestion.Status.PENDING:
                reason = "pending"
            else:
                continue

            found.append(UnprocessedFile(
                bucket=bucket_type,
                file_path=path,
                reason=reason,
                reference_time=meta.get("reference_time"),
            ))
    return found
