import json
import logging
import math
import os
import tempfile
from pathlib import Path

from django.contrib import messages
from django.db import IntegrityError, transaction
from django.http import JsonResponse
from django.shortcuts import redirect, render
from django.utils.text import slugify
from django.utils.translation import gettext as _

from georiva.formats.registry import format_registry

logger = logging.getLogger(__name__)

_WIZARD_SESSION_KEY = "georiva_upload_wizard"

STEP_LABELS = [
    _("Catalog"),
    _("Config Name"),
    _("File & Variables"),
    _("Collection Setup"),
    _("Review"),
]

_FORMAT_EXAMPLES = {
    "YYYYMMDD":     "20250115.grib2",
    "DDMMYYYY":     "15012025.grib2",
    "YYYYMMDDHH":   "2025011506.grib2",
    "YYYYMMDDHHMM": "202501150630.grib2",
    "DDMMYY":       "150125.grib2",
    "YYMMDD":       "250115.grib2",
}

_CATALOG_FORMAT_ACCEPT = {
    "grib2":   ".grib2,.grib,.grb2,.grb",
    "netcdf":  ".nc,.nc4,.netcdf",
    "geotiff": ".tif,.tiff",
    "zarr":    ".zarr",
}

_CATALOG_FORMAT_LABEL = {
    "grib2":   "GRIB / GRIB2",
    "netcdf":  "NetCDF",
    "geotiff": "GeoTIFF",
    "zarr":    "Zarr",
}


def _catalog_format_from_session(session):
    """Return the file_format string for the catalog chosen in step 1."""
    if session.get("catalog_mode") == "create":
        return session.get("new_catalog_format") or ""
    if session.get("catalog_id"):
        from georiva.core.models import Catalog as _C
        try:
            return _C.objects.get(pk=session["catalog_id"]).file_format
        except _C.DoesNotExist:
            pass
    return ""


def _catalog_name_from_session(session):
    """Return the catalog display name from the session."""
    if session.get("catalog_mode") == "create":
        return session.get("new_catalog_name") or ""
    if session.get("catalog_id"):
        from georiva.core.models import Catalog as _C
        try:
            return _C.objects.get(pk=session["catalog_id"]).name
        except _C.DoesNotExist:
            pass
    return ""


def _show_filename_format(catalog_format: str) -> bool:
    """Return True only if the format plugin requires time extracted from the filename."""
    plugin = format_registry.get(catalog_format)
    if plugin is None:
        return True  # unknown format — show the field to be safe
    return plugin.time_from_filename


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
        from georiva.ingestion.models import ManualUploadConfig

        config_name = request.POST.get("config_name", "").strip()
        duplicate = (
            config_name
            and session.get("catalog_mode") == "select"
            and session.get("catalog_id")
            and ManualUploadConfig.objects.filter(
                catalog_id=session["catalog_id"], name=config_name
            ).exists()
        )
        if not config_name:
            messages.error(request, _("Please enter a name for this upload configuration."))
        elif duplicate:
            messages.error(
                request,
                _("A configuration named '%s' already exists for this catalog.") % config_name,
            )
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
# AJAX — sample file upload → variables JSON
# =============================================================================

def _nice_bounds(vmin: float, vmax: float) -> tuple[float, float]:
    """Round a data range outward to bounds suitable for palette/encoding ranges."""
    if vmin > vmax:
        vmin, vmax = vmax, vmin
    span = vmax - vmin
    if span == 0:
        span = abs(vmax) or 1.0
    step = 10 ** math.floor(math.log10(span))
    nmin = math.floor(vmin / step) * step
    nmax = math.ceil(vmax / step) * step
    if nmin == nmax:
        nmax = nmin + step
    return round(nmin, 6), round(nmax, 6)


def _scan_value_range(plugin, file_path: str, raw_var: dict) -> dict:
    """Compute a variable's data min/max from the sample file. Never raises."""
    kwargs = {}
    if raw_var.get("key") is not None:
        kwargs["key"] = raw_var["key"]
    try:
        with plugin.open_variable(file_path, raw_var.get("name", ""), **kwargs) as info:
            vmin = float(info.data.min())
            vmax = float(info.data.max())
        if math.isnan(vmin) or math.isnan(vmax):
            return {"value_min": None, "value_max": None}
        vmin, vmax = _nice_bounds(vmin, vmax)
        return {"value_min": vmin, "value_max": vmax}
    except Exception as exc:
        logger.warning("Value range scan failed for %s: %s", raw_var.get("name"), exc)
        return {"value_min": None, "value_max": None}


def _resolve_unit(units_str: str) -> dict:
    """
    Match a scanned units string against existing Unit records.

    Returns unit_id when an existing Unit matches (exact symbol or pint
    equivalence, e.g. 'kelvin' matches 'K'), or can_create=True when the
    string is pint-valid but no matching Unit exists yet.
    """
    from georiva.core.models import Unit
    from georiva.core.unit_utils import ureg

    units_str = (units_str or "").strip()
    if not units_str:
        return {"unit_id": None, "can_create": False}

    exact = Unit.objects.filter(symbol__iexact=units_str).first()
    if exact:
        return {"unit_id": exact.pk, "can_create": False}

    try:
        target = ureg(units_str)
    except Exception:
        return {"unit_id": None, "can_create": False}

    for unit in Unit.objects.all():
        try:
            if ureg(unit.symbol).units == target.units:
                return {"unit_id": unit.pk, "can_create": False}
        except Exception:
            continue

    return {"unit_id": None, "can_create": True}


def upload_wizard_upload_sample(request):
    if request.method != "POST":
        return JsonResponse({"error": "Method not allowed"}, status=405)

    uploaded = request.FILES.get("sample_file")
    if not uploaded:
        return JsonResponse({"error": str(_("No file uploaded."))})

    ext = Path(uploaded.name).suffix.lower()
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
            for chunk in uploaded.chunks():
                tmp.write(chunk)
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp_path = tmp.name

        plugin = format_registry.get_for_file(tmp_path)
        if plugin is None:
            return JsonResponse({"error": str(_("Unsupported file format: %s") % uploaded.name)})

        raw_variables = plugin.list_variables(tmp_path)
        if not raw_variables:
            return JsonResponse({"error": str(_("No variables found in the uploaded file."))})

        variables = []
        for v in raw_variables:
            entry = {
                "name": v.get("name", ""),
                "long_name": v.get("long_name", ""),
                "units": v.get("units", ""),
            }
            entry.update(_scan_value_range(plugin, tmp_path, v))
            entry.update(_resolve_unit(entry["units"]))
            variables.append(entry)

        plugin.clear_cache()
        return JsonResponse({"variables": variables, "sample_filename": uploaded.name})

    except Exception as exc:
        logger.error("upload_wizard_upload_sample error: %s", exc)
        return JsonResponse({"error": str(exc)})
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


# =============================================================================
# Step 3 — File, Format & variable selection
# =============================================================================

def upload_wizard_step3(request):
    from georiva.ingestion.models import ManualUploadConfig

    session = _session(request)
    if not session.get("config_name"):
        return redirect("upload_wizard_step2")

    format_choices = ManualUploadConfig.ValidTimeFormat.choices
    catalog_format = _catalog_format_from_session(session)
    accept_extensions = _CATALOG_FORMAT_ACCEPT.get(catalog_format, ",".join(_CATALOG_FORMAT_ACCEPT.values()))
    format_label = _CATALOG_FORMAT_LABEL.get(catalog_format, "NetCDF, GRIB2, or GeoTIFF")
    show_fmt = _show_filename_format(catalog_format)

    if request.method == "POST":
        sample_filename = request.POST.get("sample_filename", "").strip()
        variables_json_str = request.POST.get("variables_json", "").strip()
        selected_json_str = request.POST.get("selected_variables_json", "").strip()
        valid_time_format = request.POST.get("valid_time_format", "")
        is_forecast = request.POST.get("is_forecast") == "1"

        if not show_fmt:
            valid_time_format = "CONTENT"

        errors = []
        if not sample_filename or not variables_json_str:
            errors.append(_("Please upload a sample file first."))
        if show_fmt and not valid_time_format:
            errors.append(_("Please choose a filename format."))

        variables = []
        if variables_json_str:
            try:
                variables = json.loads(variables_json_str)
            except (json.JSONDecodeError, ValueError):
                errors.append(_("Invalid variable data — please re-upload the file."))

        selected_names = []
        if selected_json_str:
            try:
                selected_names = json.loads(selected_json_str)
            except (json.JSONDecodeError, ValueError):
                errors.append(_("Invalid selection data — please try again."))

        if not errors and not selected_names:
            errors.append(_("Please select at least one variable."))

        if errors:
            for e in errors:
                messages.error(request, e)
            return render(request, "georivaingestion/wizard_step3_combined.html", {
                "session": session,
                "format_choices": format_choices,
                "format_examples": _FORMAT_EXAMPLES,
                "accept_extensions": accept_extensions,
                "format_label": format_label,
                "show_filename_format": show_fmt,
                "prefill_filename": sample_filename,
                "prefill_variables_json": variables_json_str or "[]",
                "prefill_selected_json": selected_json_str or "[]",
                "prefill_format": valid_time_format,
                "prefill_is_forecast": is_forecast,
                "step": 3,
                "step_labels": STEP_LABELS,
            })

        session.update({
            "variables": variables,
            "selected_variable_names": selected_names,
            "sample_filename": sample_filename,
            "valid_time_format": valid_time_format,
            "is_forecast": is_forecast,
        })
        _save_session(request, session)
        return redirect("upload_wizard_step4")

    # GET — prefill from session if navigating back
    prefill_filename = session.get("sample_filename", "")
    prefill_variables_json = json.dumps(session.get("variables", []))
    prefill_selected_json = json.dumps(session.get("selected_variable_names", []))
    prefill_format = session.get("valid_time_format", "")
    prefill_is_forecast = session.get("is_forecast", False)

    return render(request, "georivaingestion/wizard_step3_combined.html", {
        "session": session,
        "format_choices": format_choices,
        "format_examples": _FORMAT_EXAMPLES,
        "accept_extensions": accept_extensions,
        "format_label": format_label,
        "show_filename_format": show_fmt,
        "prefill_filename": prefill_filename,
        "prefill_variables_json": prefill_variables_json,
        "prefill_selected_json": prefill_selected_json,
        "prefill_format": prefill_format,
        "prefill_is_forecast": prefill_is_forecast,
        "step": 3,
        "step_labels": STEP_LABELS,
    })


# =============================================================================
# Step 4 — Collection setup
# =============================================================================

def upload_wizard_step4(request):
    from georiva.core.models import Unit

    session = _session(request)
    if not session.get("selected_variable_names"):
        return redirect("upload_wizard_step3")

    all_variables = session.get("variables", [])
    selected_names = session.get("selected_variable_names", [])
    selected_variables = [v for v in all_variables if v["name"] in selected_names]

    catalog_name = _catalog_name_from_session(session)
    collection_base_name = f"{catalog_name} Collection" if catalog_name else "Collection"
    units = list(Unit.objects.order_by("name").values("id", "name", "symbol"))

    def _render(prefill_collections_json="[]", prefill_assignments_json="[]"):
        return render(request, "georivaingestion/wizard_step4_collections.html", {
            "session": session,
            "selected_variables_json": json.dumps(selected_variables),
            "collection_base_name": collection_base_name,
            "units_json": json.dumps(units),
            "prefill_collections_json": prefill_collections_json,
            "prefill_assignments_json": prefill_assignments_json,
            "step": 4,
            "step_labels": STEP_LABELS,
        })

    if request.method == "POST":
        collections_json_str = request.POST.get("collections_json", "").strip()
        assignments_json_str = request.POST.get("assignments_json", "").strip()

        errors = []
        collections = []
        assignments = []

        try:
            collections = json.loads(collections_json_str) if collections_json_str else []
        except (json.JSONDecodeError, ValueError):
            errors.append(_("Invalid collection data — please try again."))

        try:
            assignments = json.loads(assignments_json_str) if assignments_json_str else []
        except (json.JSONDecodeError, ValueError):
            errors.append(_("Invalid assignment data — please try again."))

        if not collections:
            errors.append(_("Please define at least one collection."))

        for i, c in enumerate(collections):
            if not c.get("name", "").strip():
                errors.append(_("Collection %d has no name.") % (i + 1))
            if not c.get("slug", "").strip():
                c["slug"] = slugify(c.get("name", ""))

        assigned_idxs = {a.get("collection_idx") for a in assignments}
        for i in range(len(collections)):
            if i not in assigned_idxs:
                errors.append(
                    _("Collection '%s' has no variables assigned to it.") % collections[i].get("name", i + 1)
                )

        from georiva.core.unit_utils import ureg

        for a in assignments:
            var_label = a.get("variable_name", "?")
            vmin, vmax = a.get("value_min"), a.get("value_max")
            if not isinstance(vmin, (int, float)) or not isinstance(vmax, (int, float)) \
                    or isinstance(vmin, bool) or isinstance(vmax, bool):
                errors.append(_("Variable '%s' needs numeric min and max values.") % var_label)
            elif vmin >= vmax:
                errors.append(_("Variable '%s': min value must be less than max.") % var_label)

            unit_id = a.get("unit_id")
            unit_create = (a.get("unit_create") or "").strip()
            if not unit_id and not unit_create:
                errors.append(_("Variable '%s' needs a unit.") % var_label)
            elif unit_create:
                try:
                    ureg(unit_create)
                except Exception:
                    errors.append(
                        _("Variable '%s': unit '%s' is not a valid unit symbol.") % (var_label, unit_create)
                    )

        if errors:
            for e in errors:
                messages.error(request, e)
            return _render(collections_json_str or "[]", assignments_json_str or "[]")

        session.update({"collections": collections, "assignments": assignments})
        _save_session(request, session)
        return redirect("upload_wizard_step5")

    return _render(
        json.dumps(session.get("collections", [])),
        json.dumps(session.get("assignments", [])),
    )


# =============================================================================
# Step 5 — Review
# =============================================================================

def upload_wizard_step5(request):
    from georiva.core.models import Catalog

    session = _session(request)
    if not session.get("assignments"):
        return redirect("upload_wizard_step4")

    catalog_display = None
    if session.get("catalog_mode") == "create":
        catalog_display = session.get("new_catalog_name")
    elif session.get("catalog_id"):
        try:
            catalog_display = Catalog.objects.get(pk=session["catalog_id"]).name
        except Catalog.DoesNotExist:
            pass

    collections_display = []
    for idx, coll in enumerate(session.get("collections", [])):
        variables = [a for a in session.get("assignments", []) if a.get("collection_idx") == idx]
        collections_display.append({
            "name": coll["name"],
            "slug": coll["slug"],
            "variables": variables,
        })

    return render(request, "georivaingestion/wizard_step5_review.html", {
        "session": session,
        "catalog_display": catalog_display,
        "collections_display": collections_display,
        "step": 5,
        "step_labels": STEP_LABELS,
    })


# =============================================================================
# Provision — create DB records
# =============================================================================

def _unit_for_assignment(assignment: dict):
    """Resolve the Unit for an assignment: existing pk or get_or_create from symbol."""
    from georiva.core.models import Unit

    if assignment.get("unit_id"):
        return Unit.objects.get(pk=assignment["unit_id"])
    symbol = (assignment.get("unit_create") or "").strip()
    if not symbol:
        raise ValueError(
            _("Variable '%s' has no unit — please revisit Collection Setup.")
            % assignment.get("variable_name", "?")
        )
    unit, _created = Unit.objects.get_or_create(symbol=symbol, defaults={"name": symbol})
    return unit


def upload_wizard_provision(request):
    from georiva.core.models import Catalog, Collection, Variable
    from georiva.ingestion.models import ManualUploadConfig, ManualUploadConfigVariable

    if request.method != "POST":
        return redirect("upload_wizard_step5")

    session = _session(request)
    if not session.get("valid_time_format"):
        messages.warning(request, _("Please complete all steps first."))
        return redirect("upload_wizard_step1")

    try:
        with transaction.atomic():
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

            created_collections = {}
            for idx, coll_data in enumerate(session.get("collections", [])):
                collection, _created = Collection.objects.get_or_create(
                    catalog=catalog,
                    slug=coll_data["slug"],
                    defaults={"name": coll_data["name"]},
                )
                created_collections[idx] = collection

            config = ManualUploadConfig.objects.create(
                catalog=catalog,
                name=session["config_name"],
                is_forecast=session.get("is_forecast", False),
                valid_time_format=session["valid_time_format"],
            )

            for assignment in session.get("assignments", []):
                collection = created_collections[assignment["collection_idx"]]
                variable_name = assignment["variable_name"]
                unit = _unit_for_assignment(assignment)

                ManualUploadConfigVariable.objects.create(
                    config=config,
                    collection=collection,
                    variable_name=variable_name,
                    long_name=assignment.get("long_name", ""),
                    units=assignment.get("units", "")[:50],
                )

                # get_or_create: re-provisioning into an existing collection must
                # not clobber hand-tuned Variables (palette, transform, ranges).
                Variable.objects.get_or_create(
                    collection=collection,
                    slug=slugify(variable_name),
                    defaults={
                        "name": assignment.get("long_name") or variable_name,
                        "transform_type": Variable.TransformType.PASSTHROUGH,
                        "unit": unit,
                        "value_min": assignment["value_min"],
                        "value_max": assignment["value_max"],
                        "sources": [("primary", {"source_name": variable_name})],
                    },
                )

    except IntegrityError:
        messages.error(
            request,
            _("A configuration named '%s' already exists for this catalog.")
            % session.get("config_name", ""),
        )
        return redirect("upload_wizard_step5")
    except Exception as exc:
        messages.error(request, _("Provisioning failed: %s") % exc)
        return redirect("upload_wizard_step5")

    request.session.pop(_WIZARD_SESSION_KEY, None)
    messages.success(
        request,
        _("Upload configuration '%s' created with %d variable(s).") % (
            config.name, config.variables.count()
        ),
    )
    return redirect("manual_upload_config_list")
