import logging
from datetime import timedelta

import pytz
from django.conf import settings
from django.utils import timezone as dj_timezone

from georiva.events.context import new_chain

logger = logging.getLogger(__name__)

#: How long a collection must go without a new file before the quiet-period
#: closer will call a run finished. Only ever the last resort — the other two
#: closers fire on evidence, not on a clock.
DEFAULT_SETTLE_MINUTES = 20


def settle_minutes() -> int:
    return int(getattr(settings, "GEORIVA_RUN_SETTLE_MINUTES", DEFAULT_SETTLE_MINUTES))


def _reftime_stamp(reference_time) -> str:
    """The ``GR--{stamp}--`` fragment naming this reference time in a key."""
    from georiva.core.storage.filename import GEORIVA_REFTIME_FORMAT

    return reference_time.astimezone(pytz.utc).strftime(GEORIVA_REFTIME_FORMAT)


def _collection_prefix(collection) -> str:
    return f"{collection.catalog.storage_prefix}/{collection.slug}/"


def declared_expected_paths(collection, reference_time) -> set[str]:
    from georiva.sources.models import FetchedFile

    prefix = _collection_prefix(collection)
    fragment = f"GR--{_reftime_stamp(reference_time)}--"
    return set(
        FetchedFile.objects.filter(
            file_path__startswith=prefix,
            file_path__contains=fragment,
            status__in=[FetchedFile.Status.STORED, FetchedFile.Status.SKIPPED],
        ).values_list("file_path", flat=True)
    )


def arrived_paths(collection, reference_time) -> set[str]:
    """Every key that has actually completed into this collection for this run."""
    from georiva.ingestion.models import FileIngestion

    return set(
        FileIngestion.objects.filter(
            collections=collection,
            reference_time=reference_time,
            status=FileIngestion.Status.COMPLETED,
        )
        .distinct()
        .values_list("file_path", flat=True)
    )


def close_if_declared_set_complete(run) -> bool:
    """Close ``run`` iff every declared key has arrived. No declaration, no close."""
    from georiva.ingestion.models import RunIngestion

    if not run.is_open:
        return False

    expected = declared_expected_paths(run.collection, run.reference_time)
    if not expected:
        return False

    if run.expected_file_count != len(expected):
        run.expected_file_count = len(expected)
        run.save(update_fields=["expected_file_count", "updated_at"])

    if not expected.issubset(arrived_paths(run.collection, run.reference_time)):
        return False

    return run.close(RunIngestion.Closer.DECLARED_SET)


def close_if_files_per_run_reached(run) -> bool:
    """Close ``run`` iff its catalog's push feed has had ``files_per_run`` machine
    deliveries for this reference time finish ingesting.

    A machine delivery is one attributed to a ``PushSender``. The feed declares how
    many files make up a run, so this is the push-side counterpart of the DataFeed's
    declared set. A person's upload never counts: a batch proves the batch, not the
    run, and keeps closing as ``upload_session``. Short of the count, the quiet
    period still closes the run.
    """
    from georiva.ingestion.models import Delivery, PushFeed, RunIngestion

    if not run.is_open:
        return False

    feed = PushFeed.objects.filter(catalog_id=run.collection.catalog_id).first()
    if feed is None:
        return False

    arrived = arrived_paths(run.collection, run.reference_time)
    delivered = (
        Delivery.objects.filter(
            push_feed=feed,
            file_path__in=arrived,
            push_sender__isnull=False,
            status=Delivery.Status.ACCEPTED,
        )
        .values("file_path")
        .distinct()
        .count()
    )
    if delivered < feed.files_per_run:
        return False

    if run.expected_file_count != feed.files_per_run:
        run.expected_file_count = feed.files_per_run
        run.save(update_fields=["expected_file_count", "updated_at"])

    return run.close(RunIngestion.Closer.DECLARED_SET)


def close_if_upload_batches_complete(run) -> bool:
    """Close ``run`` iff every upload batch that fed it has finished landing: the
    session is closed to new files, and every file it stored has finished ingesting.
    """
    from georiva.ingestion.models import Delivery, FileIngestion, RunIngestion, UploadSession

    if not run.is_open:
        return False

    arrived = arrived_paths(run.collection, run.reference_time)
    if not arrived:
        return False

    session_ids = set(
        Delivery.objects.filter(file_path__in=arrived, session__isnull=False).values_list("session_id", flat=True)
    )
    if not session_ids:
        return False

    still_active = UploadSession.objects.filter(pk__in=session_ids, status=UploadSession.Status.ACTIVE).exists()
    if still_active:
        return False

    stored = set(
        Delivery.objects.filter(
            session_id__in=session_ids,
            status=Delivery.Status.ACCEPTED,
        ).values_list("file_path", flat=True)
    )
    ingested = set(
        FileIngestion.objects.filter(
            file_path__in=stored,
            status=FileIngestion.Status.COMPLETED,
        ).values_list("file_path", flat=True)
    )
    if not stored.issubset(ingested):
        return False

    return run.close(RunIngestion.Closer.UPLOAD_SESSION)


def has_files_in_flight(collection) -> bool:
    """Whether anything is still being ingested that could land in ``collection``."""
    from georiva.ingestion.models import FileIngestion

    return FileIngestion.objects.filter(
        file_path__startswith=f"{collection.catalog.storage_prefix}/",
        status__in=[FileIngestion.Status.PENDING, FileIngestion.Status.PROCESSING],
    ).exists()


def close_quiet_runs(minutes: int | None = None) -> int:
    from georiva.ingestion.models import RunIngestion

    minutes = settle_minutes() if minutes is None else minutes
    cutoff = dj_timezone.now() - timedelta(minutes=minutes)

    candidates = RunIngestion.objects.filter(
        status=RunIngestion.Status.OPEN,
        last_file_at__lt=cutoff,
    ).select_related("collection__catalog__organisation")

    closed = 0
    for run in candidates:
        if has_files_in_flight(run.collection):
            continue
        # One chain per run: this task spans organisations, a chain does not.
        with new_chain(organisation_id=run.collection.catalog.organisation_id):
            if run.close(RunIngestion.Closer.QUIET_PERIOD):
                closed += 1
    return closed
