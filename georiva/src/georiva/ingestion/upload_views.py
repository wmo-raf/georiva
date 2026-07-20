"""
Manual Upload Page views.

The end-to-end path an operator takes to submit a single file against a
ManualUploadConfig and track its ingestion progress:

1. GET  manual_upload_page          — render the upload form
2. POST manual_upload_extract_times — pre-fill the time field from a filename
3. POST manual_upload_submit        — validate, write to MinIO incoming,
                                      create UploadSession, enqueue ingestion
4. Client polls GET /api/arrivals/{id}/status/ until a terminal status.
"""

import logging
from datetime import datetime

import pytz
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, render
from django.urls import reverse, reverse_lazy
from django.utils.text import slugify
from django.utils.translation import gettext as _

from georiva.core.filename import build_filename, parse_filename

logger = logging.getLogger(__name__)


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

    GeoTIFF:      {catalog}/{collection}/{variable}/{YYYY}/{MM}/{DD}/{filename}
    GRIB/NetCDF:  {catalog}/{filename}  (GR-- prefixed when a reference time exists,
                  so the whole file is processed against every collection)
    """
    catalog_slug = config.catalog.slug
    original_name = parse_filename(filename)["original_name"]
    final_name = build_filename(original_name, reference_time)

    if config.catalog.file_format == "geotiff":
        var_slug = slugify(variable.variable_name)
        coll_slug = variable.collection.slug
        return (
            f"{catalog_slug}/{coll_slug}/{var_slug}/"
            f"{valid_time:%Y}/{valid_time:%m}/{valid_time:%d}/{final_name}"
        )
    return f"{catalog_slug}/{final_name}"


def manual_upload_page(request, pk):
    from georiva.ingestion.models import ManualUploadConfig
    from georiva.ingestion.upload_wizard_views import (
        _CATALOG_FORMAT_ACCEPT,
        _CATALOG_FORMAT_LABEL,
    )

    config = get_object_or_404(
        ManualUploadConfig.objects.select_related("catalog"), pk=pk
    )
    variables = config.variables.select_related("collection").order_by(
        "long_name", "variable_name"
    )
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

    return render(request, "georivaingestion/manual_upload_page.html", {
        # Rendered by the slim header via wagtailadmin/generic/base.html.
        "breadcrumbs_items": [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse("manual_upload_config_list"), "label": _("Manual Uploads")},
            {"url": None, "label": config.name},
        ],
        "header_title": _("Upload files — %s") % config.name,
        "header_icon": "upload",
        # 'upload_config', not 'config': wagtailadmin/admin_base.html assigns
        # {% wagtail_config as config %}, which shadows a 'config' context var
        # by the time extra_js renders.
        "upload_config": config,
        "variables": variables,
        "is_geotiff": is_geotiff,
        "affected_collections": affected_collections,
        "accept_extensions": _CATALOG_FORMAT_ACCEPT.get(file_format, ""),
        "format_label": _CATALOG_FORMAT_LABEL.get(file_format, file_format),
        "time_label": _("Model run time") if config.is_forecast else _("Observation date"),
        "time_required": config.is_forecast or is_geotiff,
    })


def manual_upload_extract_times(request, pk):
    """Pre-fill attempt: extract times from a filename before upload."""
    from georiva.ingestion.models import ManualUploadConfig

    if request.method != "POST":
        return JsonResponse({"error": "Method not allowed"}, status=405)

    config = get_object_or_404(ManualUploadConfig, pk=pk)
    filename = request.POST.get("filename", "").strip()
    if not filename:
        return JsonResponse({"error": str(_("No filename provided."))}, status=400)

    reference_time, valid_time = _resolve_times(config, filename, operator_time=None)
    prefill = reference_time if config.is_forecast else valid_time

    def _iso(dt):
        return dt.isoformat() if dt else None

    return JsonResponse({
        "reference_time": _iso(reference_time),
        "valid_time": _iso(valid_time),
        # naive local value for the datetime-local input
        "prefill": prefill.strftime("%Y-%m-%dT%H:%M") if prefill else None,
    })


def manual_upload_submit(request, pk):
    from django.contrib.contenttypes.models import ContentType

    from georiva.core.storage import BucketType, storage
    from georiva.ingestion.models import (
        FileIngestion, FileIngestionJob, ManualUploadConfig,
        UploadSession, UploadedFile,
    )
    from georiva.ingestion.tasks import process_incoming_file

    if request.method != "POST":
        return JsonResponse({"error": "Method not allowed"}, status=405)

    config = get_object_or_404(
        ManualUploadConfig.objects.select_related("catalog"), pk=pk
    )

    uploaded_files = request.FILES.getlist("files")
    is_geotiff = config.catalog.file_format == "geotiff"
    variable_ids = request.POST.getlist("variable_ids")
    times = request.POST.getlist("times")

    if not uploaded_files:
        return JsonResponse({"error": str(_("Please choose a file to upload."))}, status=400)

    # For GeoTIFF, validate that each file has a corresponding variable.
    if is_geotiff:
        for i, uploaded in enumerate(uploaded_files):
            vid = variable_ids[i] if i < len(variable_ids) else None
            if not vid or not config.variables.filter(pk=vid).exists():
                return JsonResponse(
                    {"error": str(_("Please choose a variable for each file."))}, status=400
                )

    # Pre-validate time requirements before creating the session.
    for i, uploaded in enumerate(uploaded_files):
        operator_time = _parse_datetime_local(times[i] if i < len(times) else "")
        reference_time, valid_time = _resolve_times(config, uploaded.name, operator_time)
        if config.is_forecast and reference_time is None:
            return JsonResponse(
                {"error": str(_("Model run time is required for forecast uploads."))},
                status=400,
            )
        if is_geotiff and valid_time is None:
            return JsonResponse(
                {"error": str(_(
                    "Could not determine the observation date. Use a filename matching "
                    "the '%s' format, or fill in the date field."
                ) % config.valid_time_format)},
                status=400,
            )

    session = UploadSession.objects.create(
        catalog=config.catalog,
        user=request.user if request.user.is_authenticated else None,
    )

    ct = ContentType.objects.get_for_model(FileIngestionJob, for_concrete_model=False)
    result_files = []

    for i, uploaded in enumerate(uploaded_files):
        vid = variable_ids[i] if i < len(variable_ids) else None
        variable = (
            config.variables.select_related("collection").filter(pk=vid).first()
            if vid else None
        )
        operator_time = _parse_datetime_local(times[i] if i < len(times) else "")
        reference_time, valid_time = _resolve_times(config, uploaded.name, operator_time)

        file_path = _build_incoming_path(
            config, variable, uploaded.name, reference_time, valid_time
        )

        uf = UploadedFile.objects.create(session=session, original_filename=uploaded.name)
        uf.mark_uploading()

        try:
            saved_path = storage.incoming.save(file_path, uploaded)
        except Exception as exc:
            logger.error("Manual upload MinIO write failed for %s: %s", file_path, exc)
            uf.mark_failed(error=str(exc)[:2000])
            result_files.append({"id": uf.pk, "status": "failed", "error": str(exc), "job_id": None})
            continue

        uf.mark_stored(file_path=saved_path, bytes=uploaded.size or 0)

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

        result_files.append({"id": uf.pk, "status": "stored", "job_id": job.pk})

    return JsonResponse({"upload_session_id": session.pk, "files": result_files})
