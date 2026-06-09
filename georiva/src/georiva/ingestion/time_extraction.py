from pathlib import Path

import pytz

from georiva.core.filename import parse_filename
from georiva.formats.registry import format_registry

_FORMAT_PATTERNS = {
    "YYYYMMDD":   "%Y%m%d",
    "DDMMYYYY":   "%d%m%Y",
    "YYYYMMDDHH": "%Y%m%d%H",
    "YYYYMMDDHHMM": "%Y%m%d%H%M",
    "DDMMYY":     "%d%m%y",
    "YYMMDD":     "%y%m%d",
}


def extract_times(filename: str, format_choice: str, file_obj=None) -> dict:
    """
    Determine reference_time and valid_time from a filename and optional file content.

    Extraction order:
      1. GR--{reftime}-- prefix in filename → reference_time
      2. Stem of original filename parsed with format_choice → valid_time
      3. File content via format plugin (GRIB/NetCDF only, requires file_obj)

    Never raises. Returns a partial or empty dict when fields cannot be resolved.
    """
    result = {}

    parsed = parse_filename(Path(filename).name)
    if parsed["reference_time"] is not None:
        result["reference_time"] = parsed["reference_time"]

    original_name = parsed["original_name"]
    stem = Path(original_name).stem
    valid_time = _parse_stem(stem, format_choice)
    if valid_time is not None:
        result["valid_time"] = valid_time

    if file_obj is not None and len(result) < 2:
        _fill_from_content(filename, file_obj, result)

    return result


def _parse_stem(stem: str, format_choice: str):
    pattern = _FORMAT_PATTERNS.get(format_choice)
    if not pattern:
        return None
    from datetime import datetime
    try:
        dt = datetime.strptime(stem, pattern)
        return pytz.utc.localize(dt)
    except ValueError:
        return None


def _fill_from_content(filename: str, file_obj, result: dict):
    import tempfile

    ext = Path(filename).suffix.lower()
    content_formats = {".grib", ".grib2", ".grb", ".grb2", ".nc", ".nc4", ".netcdf"}
    if ext not in content_formats:
        return

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
            tmp.write(file_obj.read())
            tmp_path = tmp.name

        plugin = format_registry.get_for_file(tmp_path)
        if plugin is None:
            return

        variables = plugin.list_variables(tmp_path)
        if not variables:
            return

        first_var = variables[0]
        var_name = first_var.get("name") or first_var.get("key")
        if not var_name:
            return

        timestamps = plugin.get_timestamps(tmp_path, var_name)
        if timestamps and "valid_time" not in result:
            result["valid_time"] = timestamps[0]

    except Exception:
        pass
    finally:
        if tmp_path:
            try:
                import os
                os.unlink(tmp_path)
            except Exception:
                pass
