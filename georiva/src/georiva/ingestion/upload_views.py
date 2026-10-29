"""
Push feed Upload Page views.

The end-to-end path a person takes to submit files into a PushFeed and track
their ingestion progress:

1. GET  push_feed_page          — render the upload form
2. POST push_feed_extract_times — pre-fill the time field from a filename
3. POST push_feed_prepare       — validate, record an UploadSession + Deliveries, and hand the
                                      browser a presigned PUT URL per file
   (browser PUTs each file straight to MinIO ``incoming``; the bytes never touch this process)
   POST push_feed_complete      — per file, once the PUT is done: confirm it landed, start ingestion
   POST push_feed_submit        — the same for a backend with nothing to sign for: the files come
                                      in the request, are written to ``incoming`` here, and ingested
4. Client polls GET /api/arrivals/{id}/status/ until a terminal status.
"""

import logging
from datetime import datetime

import pytz
from django.http import JsonResponse
from django.shortcuts import render
from django.urls import reverse, reverse_lazy
from django.utils.translation import gettext as _

from georiva.core.storage.filename import build_filename, parse_filename
from georiva.ingestion import sender_version
from georiva.organisations.access import get_org_object_or_404

logger = logging.getLogger(__name__)

RECENT_DELIVERIES = 25

#: S3 takes a single PUT up to this size; anything larger needs a multipart upload.
MAX_SINGLE_PUT_BYTES = 5 * 1024**3

#: How long a presigned URL may wait before the browser starts its PUT. The upload itself
#: may run longer: the URL is only checked as the request begins.
PRESIGN_TTL = 3600


def _parse_datetime_local(value: str):
    """Parse an HTML datetime-local value ('2025-01-15T06:00') to aware UTC."""
    value = (value or "").strip()
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt)
    return dt


def _resolve_times(config, filename: str, operator_time):
    """
    Combine extract_times() output with the operator-supplied time.

    Extraction from the filename wins; the operator field fills whichever
    slot the config's semantics assign it (reference time for forecasts,
    valid time otherwise).
    """
    from georiva.ingestion.time_extraction import extract_times

    extracted = extract_times(filename, config.valid_time_format)
    reference_time = extracted.get("reference_time")
    valid_time = extracted.get("valid_time")

    if config.is_forecast:
        reference_time = reference_time or operator_time
    else:
        valid_time = valid_time or operator_time

    return reference_time, valid_time


def _build_incoming_path(config, variable, filename: str, reference_time, valid_time) -> str:
    """
    Construct the MinIO incoming-bucket path.

    GeoTIFF:      {org}/{catalog}/{collection}/{variable}/{YYYY}/{MM}/{DD}/{filename}
    GRIB/NetCDF:  {org}/{catalog}/{filename}  (GR-- prefixed when a reference time
                  exists, so the whole file is processed against every collection)

    The org segment comes from ``config.catalog.organisation``, never from the
    uploading client — an operator cannot file data into another institution.
    """
    catalog_prefix = config.catalog.storage_prefix
    original_name = parse_filename(filename)["original_name"]
    final_name = build_filename(original_name, reference_time)

    if config.catalog.file_format == "geotiff":
        var_slug = variable.slug
        coll_slug = variable.collection.slug
        return f"{catalog_prefix}/{coll_slug}/{var_slug}/{valid_time:%Y}/{valid_time:%m}/{valid_time:%d}/{final_name}"
    return f"{catalog_prefix}/{final_name}"


def _batch_error(config, filenames, variable_ids, times) -> str | None:
    """Why a batch of files cannot be sent, or None when every file passes.

    Checked before anything is recorded, so a refused batch leaves nothing behind.
    """
    is_geotiff = config.catalog.file_format == "geotiff"

    # For GeoTIFF, validate that each file has a corresponding variable.
    if is_geotiff:
        feed_variable_ids = {str(pk) for pk in config.variables().values_list("pk", flat=True)}
        for i in range(len(filenames)):
            vid = variable_ids[i] if i < len(variable_ids) else None
            if not vid or vid not in feed_variable_ids:
                return str(_("Please choose a variable for each file."))

    # Pre-validate time requirements before creating the session.
    for i, name in enumerate(filenames):
        operator_time = _parse_datetime_local(times[i] if i < len(times) else "")
        reference_time, valid_time = _resolve_times(config, name, operator_time)
        if config.is_forecast and reference_time is None:
            return str(_("Model run time is required for forecast uploads."))
        if is_geotiff and valid_time is None:
            return str(
                _(
                    "Could not determine the observation date. Use a filename matching "
                    "the '%s' format, or fill in the date field."
                )
                % config.valid_time_format
            )
    return None


def _senders_with_versions(feed):
    """The feed's machine senders, each with the georiva-push version it last wrote with.

    The version is its latest own write's, so a sender that moved to another client shows
    none; a person's Retry of its delivery is not its write and is passed over.
    """
    from django.db.models import OuterRef, Subquery

    from georiva.ingestion.models import Delivery

    latest = Delivery.objects.filter(push_sender=OuterRef("pk"), sent_by__isnull=True).order_by("-received_at", "-id")
    senders = list(feed.senders.annotate(last_version=Subquery(latest.values("sender_version")[:1])))
    for sender in senders:
        sender.last_version = sender.last_version or ""
        sender.version_outdated = sender_version.is_outdated(sender.last_version)
    return senders


def push_feed_page(request, pk, issued=None):
    from georiva.core.storage import BucketType
    from georiva.ingestion.machine_senders import endpoint_is_plain_http
    from georiva.ingestion.models import FileIngestion, PushFeed
    from georiva.ingestion.push_feed_views import can_manage_senders
    from georiva.ingestion.upload_wizard_views import (
        _CATALOG_FORMAT_ACCEPT,
        _CATALOG_FORMAT_LABEL,
    )

    config = get_org_object_or_404(
        request, PushFeed.with_last_delivery(PushFeed.objects.select_related("catalog__organisation")), pk=pk
    )
    variables = config.variables().select_related("collection").order_by("collection__name", "name")
    file_format = config.catalog.file_format
    is_geotiff = file_format == "geotiff"

    # For GRIB/NetCDF: deduplicate collections so the template can show
    # "these collections will be processed" without a variable picker.
    affected_collections = []
    if not is_geotiff:
        seen = set()
        for v in variables:
            if v.collection_id not in seen:
                seen.add(v.collection_id)
                affected_collections.append(v.collection)

    deliveries = list(config.deliveries.select_related("sent_by", "push_sender")[:RECENT_DELIVERIES])
    ingestions = {
        fi.file_path: fi
        for fi in FileIngestion.objects.filter(
            bucket=BucketType.INCOMING, file_path__in=[d.file_path for d in deliveries if d.file_path]
        )
    }
    for delivery in deliveries:
        delivery.file_ingestion = ingestions.get(delivery.file_path)

    return render(
        request,
        "georivaingestion/push_feed_page.html",
        {
            # Rendered by the slim header via wagtailadmin/generic/base.html.
            "breadcrumbs_items": [
                {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
                {"url": reverse("push_feed_list"), "label": _("Push feeds")},
                {"url": None, "label": config.name},
            ],
            "header_title": _("Upload files — %s") % config.name,
            "header_icon": "upload",
            # 'feed', not 'config': wagtailadmin/admin_base.html assigns
            # {% wagtail_config as config %}, which shadows a 'config' context var
            # by the time extra_js renders.
            "feed": config,
            "overdue": config.overdue_text(),
            "senders": _senders_with_versions(config),
            "served_version": sender_version.served(),
            "can_manage_senders": can_manage_senders(request.user),
            "plain_http": endpoint_is_plain_http(),
            "issued": issued,
            "deliveries": deliveries,
            "variables": variables,
            "is_geotiff": is_geotiff,
            "affected_collections": affected_collections,
            "accept_extensions": _CATALOG_FORMAT_ACCEPT.get(file_format, ""),
            "format_label": _CATALOG_FORMAT_LABEL.get(file_format, file_format),
            "time_label": _("Model run time") if config.is_forecast else _("Observation date"),
            "time_required": config.is_forecast or is_geotiff,
        },
    )


def push_feed_extract_times(request, pk):
    """Pre-fill attempt: extract times from a filename before upload."""
    from georiva.ingestion.models import PushFeed

    if request.method != "POST":
        return JsonResponse({"error": "Method not allowed"}, status=405)

    config = get_org_object_or_404(request, PushFeed, pk=pk)
    filename = request.POST.get("filename", "").strip()
    if not filename:
        return JsonResponse({"error": str(_("No filename provided."))}, status=400)

    reference_time, valid_time = _resolve_times(config, filename, operator_time=None)
    prefill = reference_time if config.is_forecast else valid_time

    def _iso(dt):
        return dt.isoformat() if dt else None

    return JsonResponse(
        {
            "reference_time": _iso(reference_time),
            "valid_time": _iso(valid_time),
            # naive local value for the datetime-local input
            "prefill": prefill.strftime("%Y-%m-%dT%H:%M") if prefill else None,
        }
    )


def push_feed_submit(request, pk):
    from django.contrib.contenttypes.models import ContentType

    from georiva.core.storage import BucketType, storage
    from georiva.events.middleware import inbound_correlation_id
    from georiva.ingestion.models import (
        Delivery,
        FileIngestion,
        FileIngestionJob,
        PushFeed,
        UploadSession,
    )
    from georiva.ingestion.tasks import process_incoming_file

    if request.method != "POST":
        return JsonResponse({"error": "Method not allowed"}, status=405)

    config = get_org_object_or_404(request, PushFeed.objects.select_related("catalog__organisation"), pk=pk)

    uploaded_files = request.FILES.getlist("files")
    variable_ids = request.POST.getlist("variable_ids")
    times = request.POST.getlist("times")

    if not uploaded_files:
        return JsonResponse({"error": str(_("Please choose a file to upload."))}, status=400)

    error = _batch_error(config, [uploaded.name for uploaded in uploaded_files], variable_ids, times)
    if error:
        return JsonResponse({"error": error}, status=400)

    session = UploadSession.objects.create(
        push_feed=config,
        user=request.user if request.user.is_authenticated else None,
    )

    ct = ContentType.objects.get_for_model(FileIngestionJob, for_concrete_model=False)
    result_files = []

    for i, uploaded in enumerate(uploaded_files):
        vid = variable_ids[i] if i < len(variable_ids) else None
        variable = config.variables().select_related("collection").filter(pk=vid).first() if vid else None
        operator_time = _parse_datetime_local(times[i] if i < len(times) else "")
        reference_time, valid_time = _resolve_times(config, uploaded.name, operator_time)

        file_path = _build_incoming_path(config, variable, uploaded.name, reference_time, valid_time)

        # The delivery is recorded *before* the object is stored. Storing raises a Bucket
        # notification, and the consumer finds this delivery's chain by looking up its
        # key; a path written only afterwards loses that race and orphans the chain.
        delivery = Delivery.receive(
            push_feed=config,
            filename=uploaded.name,
            file_path=file_path,
            session=session,
            sent_by=request.user if request.user.is_authenticated else None,
            correlation_id=inbound_correlation_id(request),
        )

        try:
            saved_path = storage.incoming.save(file_path, uploaded)
        except Exception as exc:
            # Nothing arrived: a delivery is a file that reached the feed.
            logger.error("Push feed upload MinIO write failed for %s: %s", file_path, exc)
            delivery.delete()
            result_files.append({"id": None, "status": "failed", "error": str(exc), "job_id": None})
            continue

        delivery.mark_accepted(file_path=saved_path, bytes=uploaded.size or 0, check_session=False)

        # Register before enqueueing — idempotent with the bucket event that will also fire.
        _fi, created = FileIngestion.register(
            bucket=BucketType.INCOMING,
            file_path=saved_path,
            reference_time=reference_time,
        )
        if not created:
            FileIngestion.reset_for_reingest(BucketType.INCOMING, saved_path)

        job = FileIngestionJob.objects.create(
            user=None,
            content_type=ct,
            file_path=saved_path,
            bucket=BucketType.INCOMING,
        )

        process_incoming_file.delay(
            file_path=saved_path,
            origin_bucket=BucketType.INCOMING,
            reference_time=reference_time.isoformat() if reference_time else None,
            job_id=job.pk,
        )

        result_files.append({"id": delivery.pk, "status": "accepted", "job_id": job.pk})

    # Checked once the batch is whole: completing on the first accept would close
    # the session before the later files had a delivery at all.
    if session.deliveries.exists():
        session._check_auto_complete()
    else:
        session.mark_failed()

    return JsonResponse({"upload_session_id": session.pk, "files": result_files})


def push_feed_prepare(request, pk):
    """Record a batch of deliveries and hand the browser a presigned PUT URL for each file.

    The browser sends the bytes straight to ``incoming``, so a large file never passes through
    this process. The key, its organisation segment and the times are still decided here: the
    browser only receives a URL good for the one key it was given.

    Each delivery is recorded before its URL is used, as a submitted one is before its write,
    so the Bucket notification finds it and admits the file as this person's. They are marked
    ``awaits_completion``: the notification admits the file but leaves starting its ingestion
    to ``push_feed_complete``, which has the job the page follows.
    """
    from django.db import transaction

    from georiva.core.storage import storage
    from georiva.events.middleware import inbound_correlation_id
    from georiva.ingestion.models import Delivery, PushFeed, UploadSession

    if request.method != "POST":
        return JsonResponse({"error": "Method not allowed"}, status=405)

    config = get_org_object_or_404(request, PushFeed.objects.select_related("catalog__organisation"), pk=pk)

    # Nothing to sign for on a local backend: the page posts the files to push_feed_submit.
    if not storage.incoming.is_s3:
        return JsonResponse({"mode": "direct"})

    filenames = request.POST.getlist("filenames")
    variable_ids = request.POST.getlist("variable_ids")
    times = request.POST.getlist("times")

    if not filenames:
        return JsonResponse({"error": str(_("Please choose a file to upload."))}, status=400)

    try:
        sizes = [int(size) for size in request.POST.getlist("sizes")]
    except ValueError:
        return JsonResponse({"error": str(_("Could not read the size of a file."))}, status=400)
    if len(sizes) != len(filenames):
        return JsonResponse({"error": str(_("Could not read the size of a file."))}, status=400)
    for name, size in zip(filenames, sizes, strict=True):
        if size > MAX_SINGLE_PUT_BYTES:
            return JsonResponse(
                {"error": str(_("%s is larger than the 5 GB a single upload can carry.")) % name},
                status=400,
            )

    error = _batch_error(config, filenames, variable_ids, times)
    if error:
        return JsonResponse({"error": error}, status=400)

    user = request.user if request.user.is_authenticated else None
    files = []
    with transaction.atomic():
        session = UploadSession.objects.create(push_feed=config, user=user)

        for i, name in enumerate(filenames):
            vid = variable_ids[i] if i < len(variable_ids) else None
            variable = config.variables().select_related("collection").filter(pk=vid).first() if vid else None
            operator_time = _parse_datetime_local(times[i] if i < len(times) else "")
            reference_time, valid_time = _resolve_times(config, name, operator_time)

            file_path = _build_incoming_path(config, variable, name, reference_time, valid_time)
            delivery = Delivery.receive(
                push_feed=config,
                filename=name,
                file_path=file_path,
                session=session,
                sent_by=user,
                correlation_id=inbound_correlation_id(request),
                awaits_completion=True,
            )
            files.append(
                {
                    "delivery_id": delivery.pk,
                    "upload_url": storage.incoming.get_upload_url(file_path, PRESIGN_TTL),
                }
            )

    return JsonResponse({"mode": "presigned", "upload_session_id": session.pk, "files": files})


def push_feed_complete(request, pk):
    """The browser's PUT of one file is done: confirm it landed, then start its ingestion.

    Does for a presigned upload what the end of ``push_feed_submit`` does for a posted one:
    register the file, create the job the page follows, and enqueue ingestion with it. Safe to
    call again; a delivery already completed answers with the job it has.
    """
    from django.contrib.contenttypes.models import ContentType

    from georiva.core.storage import BucketType, storage
    from georiva.core.storage.filename import validate_path
    from georiva.ingestion.models import Delivery, FileIngestion, FileIngestionJob, PushFeed
    from georiva.ingestion.tasks import process_incoming_file

    if request.method != "POST":
        return JsonResponse({"error": "Method not allowed"}, status=405)

    config = get_org_object_or_404(request, PushFeed, pk=pk)

    try:
        delivery_id = int(request.POST.get("delivery_id", ""))
    except ValueError:
        return JsonResponse({"error": str(_("No delivery given."))}, status=400)

    delivery = config.deliveries.select_related("session").filter(pk=delivery_id, sent_by=request.user).first()
    if delivery is None:
        return JsonResponse({"error": "Not found"}, status=404)

    key = delivery.file_path

    if delivery.status == Delivery.Status.REJECTED:
        return JsonResponse(
            {"id": delivery.pk, "status": "rejected", "error": delivery.reason or str(_("The file was refused."))},
            status=409,
        )

    if not delivery.awaits_completion:
        job = FileIngestionJob.objects.filter(bucket=BucketType.INCOMING, file_path=key).order_by("-pk").first()
        return JsonResponse({"id": delivery.pk, "status": "accepted", "job_id": job.pk if job else None})

    if not storage.incoming.exists(key):
        return JsonResponse(
            {"id": delivery.pk, "status": "missing", "error": str(_("The file has not reached storage."))},
            status=409,
        )

    # The notification may not have been seen yet; it recognises a delivery already accepted.
    if delivery.status == Delivery.Status.RECEIVED:
        delivery.mark_accepted(file_path=key, bytes=storage.incoming.size(key), check_session=False)

    reference_time = validate_path(key).get("reference_time")

    # Register before enqueueing — idempotent with the bucket event that registers it too.
    _fi, created = FileIngestion.register(bucket=BucketType.INCOMING, file_path=key, reference_time=reference_time)
    if not created:
        FileIngestion.reset_for_reingest(BucketType.INCOMING, key)

    ct = ContentType.objects.get_for_model(FileIngestionJob, for_concrete_model=False)
    job = FileIngestionJob.objects.create(user=None, content_type=ct, file_path=key, bucket=BucketType.INCOMING)

    delivery.awaits_completion = False
    delivery.save(update_fields=["awaits_completion"])

    process_incoming_file.delay(
        file_path=key,
        origin_bucket=BucketType.INCOMING,
        reference_time=reference_time.isoformat() if reference_time else None,
        job_id=job.pk,
    )

    if delivery.session_id:
        delivery.session._check_auto_complete()

    return JsonResponse({"id": delivery.pk, "status": "accepted", "job_id": job.pk})
