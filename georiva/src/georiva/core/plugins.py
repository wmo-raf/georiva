"""Helpers for introspecting installed GeoRiva plugins.

A "plugin" is one of the packages discovered from ``GEORIVA_PLUGIN_DIRS`` and
added to ``INSTALLED_APPS`` at startup (see ``config/settings/base.py``); their
import-package names are exposed as ``settings.GEORIVA_PLUGIN_NAMES``.

Metadata is read from the installed distribution via ``importlib.metadata``.
"""
import importlib.metadata

from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.validators import URLValidator

_url_validator = URLValidator()


def _valid_url(value):
    if not value:
        return None
    try:
        _url_validator(value)
    except ValidationError:
        return None
    return value


def _distribution_name(module_name):
    """Map an import-package name (e.g. ``georiva_source_cds``) to its distribution
    name (e.g. ``georiva-source-cds``). ``packages_distributions`` handles the case
    where they differ; fall back to the module name, which importlib also
    normalizes."""
    mapping = importlib.metadata.packages_distributions()
    dists = mapping.get(module_name)
    if dists:
        return dists[0]
    return module_name


def _home_page(meta):
    """Prefer the Home-page metadata field; fall back to a Project-URL entry
    (newer setuptools puts the URL there instead)."""
    home_page = _valid_url(meta.get("Home-page"))
    if home_page:
        return home_page
    for entry in meta.get_all("Project-URL") or []:
        # entries look like "Homepage, https://example.com"
        _, _, url = entry.partition(",")
        url = _valid_url(url.strip())
        if url:
            return url
    return None


def get_plugin_metadata(module_name):
    """Return a metadata dict for a single installed plugin package.

    On failure (package not installed / no metadata) returns a dict with the
    module name and ``available=False`` so callers can still render a row."""
    try:
        dist = importlib.metadata.distribution(_distribution_name(module_name))
    except importlib.metadata.PackageNotFoundError:
        return {"name": module_name, "module": module_name, "available": False}
    
    meta = dist.metadata
    return {
        "module": module_name,
        "name": meta.get("Name") or module_name,
        "version": meta.get("Version"),
        "summary": meta.get("Summary"),
        "author": meta.get("Author") or meta.get("Author-email"),
        "license": meta.get("License"),
        "home_page": _home_page(meta),
        "available": True,
    }


def get_installed_plugins():
    """Metadata for every discovered plugin, sorted by display name."""
    plugins = [get_plugin_metadata(name) for name in settings.GEORIVA_PLUGIN_NAMES]
    return sorted(plugins, key=lambda p: (p.get("name") or "").lower())
