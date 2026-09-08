"""
When a model run has finished arriving (ADR 0026).

A ``RunIngestion`` opens on its first completed file. Closing it is the hard
half: nothing in the pipeline announces "that was the last file". Each arrival
route is asked for the strongest evidence it actually has, and they differ in
strength:

- **DataFeed** — the Loader enumerates every ``FileRequest`` up front and
  persists one ``FetchedFile`` per request, so the run's expected set is
  declared before any of it arrives. This closer can be exact, and cannot close
  early.
- **Manual upload** — an ``UploadSession`` completing proves *that batch*
  finished, not that the run did. An operator uploading 20 of 68 steps today and
  48 tomorrow completes twice and means "run complete" neither time.
- **MinIO drop / sweep** — no declaration and no batch. All that is left is
  quiet: nothing in flight for the catalog, and nothing new for a settle window.

Only the first is a proof. That is why closing is optimistic everywhere and any
later arrival reopens the row at a higher revision — the weakness is absorbed by
the reopen, not papered over here.
"""

import logging
from datetime import timedelta

import pytz
from django.conf import settings
from django.utils import timezone as dj_timezone

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


# ---------------------------------------------------------------------------
# Route 1 — the declared expected set (DataFeed)
# ---------------------------------------------------------------------------


def declared_expected_paths(collection, reference_time) -> set[str]:
    """Every key the Loader said this run would produce for this collection.

    ``FetchRun`` carries no collection FK — it is created per
    ``(feed execution, collection)`` but records only the feed — so the
    collection is recovered from the key instead. That is sound rather than
    incidental: ``Loader._get_storage_path`` builds every key as
    ``{org}/{catalog}/{collection}/{filename}`` from the collection's own
    catalog chain, and stamps the reference time into the filename via the
    ``GR--`` convention. Both segments are therefore the Loader's own output,
    not anything a source plugin or remote server chose.

    FAILED files are excluded: they will never arrive, so waiting for them would
    hold the run open forever. STORED and SKIPPED both mean the bytes are in the
    bucket — skipped only means they were already there.
    """
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
    """Close ``run`` iff every declared key has arrived. No declaration, no close.

    Compares key *sets*, not counts: a re-ingest of one file and a missing other
    file produce the same count, and only the set notices.
    """
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


# ---------------------------------------------------------------------------
# Route 2 — an upload session completing
# ---------------------------------------------------------------------------


def close_if_upload_batches_complete(run) -> bool:
    """Close ``run`` iff every upload batch that fed it has finished landing.

    Hooked on a file *arriving*, not on ``UploadSession._check_auto_complete``.
    A session completes when its files reach a terminal *upload* state — the
    bytes are in MinIO — and ingestion of those bytes happens afterwards,
    asynchronously. Closing at session completion would therefore close a run
    before a single file of it had been ingested. The two conditions that
    actually matter are: the session is closed to new files, and every file it
    stored has finished ingesting.

    ``UploadedFile`` carries no ``reference_time`` of its own — unlike
    ``FetchedFile``, whose ``request_payload`` persists one — so the link runs
    through the ``FileIngestion`` rows the uploaded keys produced, which is also
    the only place the collection was resolved.

    Optimistic by construction: a session declares its own batch size, never the
    run's. Twenty of sixty-eight steps today and forty-eight tomorrow completes
    twice and means "run complete" neither time — so this closes, and tomorrow's
    arrival reopens at a higher revision.
    """
    from georiva.ingestion.models import FileIngestion, RunIngestion, UploadedFile, UploadSession

    if not run.is_open:
        return False

    arrived = arrived_paths(run.collection, run.reference_time)
    if not arrived:
        return False

    session_ids = set(UploadedFile.objects.filter(file_path__in=arrived).values_list("session_id", flat=True))
    if not session_ids:
        return False

    still_active = UploadSession.objects.filter(pk__in=session_ids, status=UploadSession.Status.ACTIVE).exists()
    if still_active:
        return False

    stored = set(
        UploadedFile.objects.filter(
            session_id__in=session_ids,
            status=UploadedFile.Status.STORED,
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


# ---------------------------------------------------------------------------
# Route 3 — quiet period (MinIO drop, sweep, and anything else)
# ---------------------------------------------------------------------------


def has_files_in_flight(collection) -> bool:
    """Whether anything is still being ingested that could land in ``collection``.

    Deliberately catalog-wide rather than collection-wide. A ``FileIngestion``
    gets its collections only once resolution succeeds, so a PENDING row has
    none yet — and a key with no collection segment fans out to *every* active
    collection of its catalog. Scoping to the catalog prefix is the only test
    that sees those; it errs toward holding a run open, which is the safe side.
    """
    from georiva.ingestion.models import FileIngestion

    return FileIngestion.objects.filter(
        file_path__startswith=f"{collection.catalog.storage_prefix}/",
        status__in=[FileIngestion.Status.PENDING, FileIngestion.Status.PROCESSING],
    ).exists()


def close_quiet_runs(minutes: int | None = None) -> int:
    """Close open runs that have gone quiet. Returns how many closed.

    The universal safety net: it runs for every route, so a DataFeed run whose
    declared set can never complete — a file the source stopped publishing — is
    eventually closed too, just on weaker evidence and with ``closed_by``
    recording which.
    """
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
        if run.close(RunIngestion.Closer.QUIET_PERIOD):
            closed += 1
    return closed
