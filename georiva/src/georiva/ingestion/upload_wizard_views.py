import logging
import tempfile
import os
from pathlib import Path

from django.contrib import messages
from django.shortcuts import redirect, render
from django.utils.text import slugify
from django.utils.translation import gettext as _

from georiva.formats.registry import format_registry

logger = logging.getLogger(__name__)

_WIZARD_SESSION_KEY = "georiva_upload_wizard"

STEP_LABELS = [
    _("Catalog"),
    _("Config Name"),
    _("Sample File"),
    _("Format & Timing"),
    _("Variables"),
    _("Review"),
]


def _session(request):
    return request.session.get(_WIZARD_SESSION_KEY, {})


def _save_session(request, data):
    request.session[_WIZARD_SESSION_KEY] = data


# =============================================================================
# Step 1 — Catalog
# =============================================================================

def upload_wizard_step1(request):
    from georiva.core.models import Catalog

    all_catalogs = Catalog.objects.order_by("name")
    file_format_choices = Catalog.FileFormat.choices

    if request.method == "POST":
        catalog_mode = request.POST.get("catalog_mode", "create")
        catalog_id = request.POST.get("catalog_id") or None
        new_catalog_name = request.POST.get("new_catalog_name", "").strip()
        new_catalog_slug = request.POST.get("new_catalog_slug", "").strip() or slugify(new_catalog_name)
        new_catalog_format = request.POST.get("new_catalog_format", "")
        new_catalog_description = request.POST.get("new_catalog_description", "").strip()

        errors = []
        if catalog_mode == "select" and not catalog_id:
            errors.append(_("Please choose a catalog."))
        if catalog_mode == "create":
            if not new_catalog_name:
                errors.append(_("Please enter a name for the new Catalog."))
            if not new_catalog_format:
                errors.append(_("Please choose a file format."))
            if new_catalog_slug and Catalog.objects.filter(slug=new_catalog_slug).exists():
                errors.append(_("A Catalog with slug '%s' already exists.") % new_catalog_slug)

        if errors:
            for e in errors:
                messages.error(request, e)
        else:
            session = _session(request)
            session.update({
                "catalog_mode": catalog_mode,
                "catalog_id": int(catalog_id) if catalog_mode == "select" and catalog_id else None,
                "new_catalog_name": new_catalog_name if catalog_mode == "create" else None,
                "new_catalog_slug": new_catalog_slug if catalog_mode == "create" else None,
                "new_catalog_format": new_catalog_format if catalog_mode == "create" else None,
                "new_catalog_description": new_catalog_description if catalog_mode == "create" else None,
            })
            _save_session(request, session)
            return redirect("upload_wizard_step2")

    session = _session(request)
    return render(request, "georivaingestion/wizard_step1_catalog.html", {
        "all_catalogs": all_catalogs,
        "file_format_choices": file_format_choices,
        "session": session,
        "step": 1,
        "step_labels": STEP_LABELS,
    })


# =============================================================================
# Step 2 — Config name
# =============================================================================

def upload_wizard_step2(request):
    session = _session(request)
    if not session.get("catalog_mode"):
        return redirect("upload_wizard_step1")

    if request.method == "POST":
        config_name = request.POST.get("config_name", "").strip()
        if not config_name:
            messages.error(request, _("Please enter a name for this upload configuration."))
        else:
            session["config_name"] = config_name
            _save_session(request, session)
            return redirect("upload_wizard_step3")

    default_config_name = session.get("config_name", "")
    if not default_config_name:
        if session.get("catalog_mode") == "create" and session.get("new_catalog_name"):
            default_config_name = f"{session['new_catalog_name']} Config"
        elif session.get("catalog_id"):
            from georiva.core.models import Catalog as _Catalog
            try:
                default_config_name = f"{_Catalog.objects.get(pk=session['catalog_id']).name} Config"
            except _Catalog.DoesNotExist:
                pass

    return render(request, "georivaingestion/wizard_step2_name.html", {
        "session": session,
        "default_config_name": default_config_name,
        "step": 2,
        "step_labels": STEP_LABELS,
    })


# =============================================================================
# Step 3 — Sample file → list_variables() → discard
# =============================================================================

def upload_wizard_step3(request):
    session = _session(request)
    if not session.get("config_name"):
        return redirect("upload_wizard_step2")

    if request.method == "POST":
        uploaded = request.FILES.get("sample_file")
        if not uploaded:
            messages.error(request, _("Please upload a sample file."))
            return render(request, "georivaingestion/wizard_step3_sample.html", {
                "session": session, "step": 3, "step_labels": STEP_LABELS,
            })

        ext = Path(uploaded.name).suffix.lower()
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
                for chunk in uploaded.chunks():
                    tmp.write(chunk)
                tmp_path = tmp.name

            plugin = format_registry.get_for_file(tmp_path)
            if plugin is None:
                messages.error(request, _("Unsupported file format: %s") % uploaded.name)
                return render(request, "georivaingestion/wizard_step3_sample.html", {
                    "session": session, "step": 3, "step_labels": STEP_LABELS,
                })

            raw_variables = plugin.list_variables(tmp_path)
        except Exception as exc:
            messages.error(request, _("Could not read variables from file: %s") % exc)
            return render(request, "georivaingestion/wizard_step3_sample.html", {
                "session": session, "step": 3, "step_labels": STEP_LABELS,
            })
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)

        variables = [
            {
                "name": v.get("name", ""),
                "long_name": v.get("long_name", ""),
                "units": v.get("units", ""),
            }
            for v in raw_variables
        ]
        if not variables:
            messages.error(request, _("No variables found in the uploaded file."))
            return render(request, "georivaingestion/wizard_step3_sample.html", {
                "session": session, "step": 3, "step_labels": STEP_LABELS,
            })

        session["variables"] = variables
        session["sample_filename"] = uploaded.name
        _save_session(request, session)
        return redirect("upload_wizard_step4")

    return render(request, "georivaingestion/wizard_step3_sample.html", {
        "session": session,
        "step": 3,
        "step_labels": STEP_LABELS,
    })


# =============================================================================
# Step 4 — Filename format & forecast timing
# =============================================================================

def upload_wizard_step4(request):
    from georiva.ingestion.models import ManualUploadConfig

    session = _session(request)
    if not session.get("variables"):
        return redirect("upload_wizard_step3")

    format_choices = ManualUploadConfig.ValidTimeFormat.choices

    FORMAT_EXAMPLES = {
        "YYYYMMDD":   "20250115.grib2",
        "DDMMYYYY":   "15012025.grib2",
        "YYYYMMDDHH": "2025011506.grib2",
        "YYYYMMDDHHMM": "202501150630.grib2",
        "DDMMYY":     "150125.grib2",
        "YYMMDD":     "250115.grib2",
    }

    if request.method == "POST":
        valid_time_format = request.POST.get("valid_time_format", "")
        is_forecast = request.POST.get("is_forecast") == "1"
        if not valid_time_format:
            messages.error(request, _("Please choose a filename format."))
        else:
            session["valid_time_format"] = valid_time_format
            session["is_forecast"] = is_forecast
            _save_session(request, session)
            return redirect("upload_wizard_step5")

    return render(request, "georivaingestion/wizard_step4_format.html", {
        "session": session,
        "format_choices": format_choices,
        "format_examples": FORMAT_EXAMPLES,
        "sample_filename": session.get("sample_filename", ""),
        "step": 4,
        "step_labels": STEP_LABELS,
    })


# =============================================================================
# Step 5 — Assign variables to Collections
# =============================================================================

def upload_wizard_step5(request):
    from georiva.core.models import Collection

    session = _session(request)
    if not session.get("valid_time_format"):
        return redirect("upload_wizard_step4")

    variables = session["variables"]
    collections = Collection.objects.select_related("catalog").order_by("catalog__name", "name")

    if request.method == "POST":
        assignments = []
        errors = []

        for var in variables:
            col_id = request.POST.get(f"collection_{var['name']}")
            if not col_id:
                errors.append(_("Please assign variable '%s' to a collection.") % var["name"])
            else:
                assignments.append({"variable_name": var["name"], "collection_id": int(col_id)})

        if errors:
            for e in errors:
                messages.error(request, e)
        else:
            session["assignments"] = assignments
            _save_session(request, session)
            return redirect("upload_wizard_step6")

    return render(request, "georivaingestion/wizard_step5_variables.html", {
        "session": session,
        "variables": variables,
        "collections": collections,
        "step": 5,
        "step_labels": STEP_LABELS,
    })


# =============================================================================
# Step 6 — Review
# =============================================================================

def upload_wizard_step6(request):
    from georiva.core.models import Catalog, Collection

    session = _session(request)
    if not session.get("assignments"):
        return redirect("upload_wizard_step5")

    # Build display summary
    catalog_display = None
    if session.get("catalog_mode") == "create":
        catalog_display = session.get("new_catalog_name")
    elif session.get("catalog_id"):
        try:
            catalog_display = Catalog.objects.get(pk=session["catalog_id"]).name
        except Catalog.DoesNotExist:
            pass

    assignments_display = []
    for a in session.get("assignments", []):
        col_name = ""
        try:
            col_name = Collection.objects.get(pk=a["collection_id"]).name
        except Collection.DoesNotExist:
            pass
        assignments_display.append({"variable_name": a["variable_name"], "collection_name": col_name})

    return render(request, "georivaingestion/wizard_step6_review.html", {
        "session": session,
        "catalog_display": catalog_display,
        "assignments_display": assignments_display,
        "step": 6,
        "step_labels": STEP_LABELS,
    })


# =============================================================================
# Provision — create DB records
# =============================================================================

def upload_wizard_provision(request):
    from georiva.core.models import Catalog, Collection
    from georiva.ingestion.models import ManualUploadConfig, ManualUploadConfigVariable

    if request.method != "POST":
        return redirect("upload_wizard_step6")

    session = _session(request)
    if not session.get("valid_time_format"):
        messages.warning(request, _("Please complete all steps first."))
        return redirect("upload_wizard_step1")

    # Resolve or create Catalog
    if session.get("catalog_mode") == "create":
        catalog, _created = Catalog.objects.get_or_create(
            slug=session["new_catalog_slug"],
            defaults={
                "name": session["new_catalog_name"],
                "file_format": session["new_catalog_format"],
                "description": session.get("new_catalog_description") or "",
            },
        )
    else:
        try:
            catalog = Catalog.objects.get(pk=session["catalog_id"])
        except Catalog.DoesNotExist:
            messages.error(request, _("Selected catalog no longer exists."))
            return redirect("upload_wizard_step1")

    try:
        config = ManualUploadConfig.objects.create(
            catalog=catalog,
            name=session["config_name"],
            is_forecast=session.get("is_forecast", False),
            valid_time_format=session["valid_time_format"],
        )

        for assignment in session.get("assignments", []):
            collection = Collection.objects.get(pk=assignment["collection_id"])
            var_meta = next(
                (v for v in session["variables"] if v["name"] == assignment["variable_name"]),
                {},
            )
            ManualUploadConfigVariable.objects.create(
                config=config,
                collection=collection,
                variable_name=assignment["variable_name"],
                long_name=var_meta.get("long_name", ""),
                units=var_meta.get("units", ""),
            )

        request.session.pop(_WIZARD_SESSION_KEY, None)
        messages.success(
            request,
            _("Upload configuration '%s' created with %d variable(s).") % (
                config.name, config.variables.count()
            ),
        )
        return redirect("wagtailadmin_home")

    except Exception as exc:
        messages.error(request, _("Provisioning failed: %s") % exc)
        return redirect("upload_wizard_step6")
