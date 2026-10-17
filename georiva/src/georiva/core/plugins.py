"""Helpers for introspecting installed GeoRiva plugins.

A "plugin" is one of the packages discovered from ``GEORIVA_PLUGIN_DIRS`` and
added to ``INSTALLED_APPS`` at startup (see ``config/settings/base.py``); their
import-package names are exposed as ``settings.GEORIVA_PLUGIN_NAMES``.

Metadata is read from the installed distribution via ``importlib.metadata``.

Two things live here: what a plugin *is* (the metadata above) and the one place
a plugin may reach into the instance's public surface — :func:`api_urlpatterns`.
"""

import importlib
import importlib.metadata
import importlib.util
import logging

from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.validators import URLValidator
from django.urls import include, path

logger = logging.getLogger(__name__)

_url_validator = URLValidator()

#: The module a plugin publishes to contribute routes under ``/api/``.
API_URLS_MODULE = "api_urls"


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


def api_urlpatterns():
    """The URL patterns discovered plugins contribute under ``/api/``.

    A plugin that wants a route publishes an ``api_urls`` module with a
    ``urlpatterns`` list, and it is included. The convention is the whole of the
    registration: there is no registry to append to and no settings entry to
    add, so a plugin cannot be installed and routed separately, and there is no
    second list that can disagree with what is installed.

    **Core's patterns are matched first.** This list is appended to core's, never
    merged into it, so a plugin declaring ``stac/`` does not shadow
    ``/api/stac/`` — it declares a route that can never be reached, which is a
    plugin's own bug rather than an instance outage. Between plugins the order is
    discovery order and the first match wins; nothing here arbitrates that,
    because the alternative is a namespace prefix nobody asked for on every route
    a plugin publishes.

    **A plugin whose ``api_urls`` will not import is logged and skipped.** One
    broken plugin must not take down the instance's URL conf: an unhandled
    exception here is raised while Django is building the resolver, so it reaches
    every request on the instance rather than the routes of the plugin that
    caused it — the STAC API included. Skipping costs that plugin its routes,
    which is the blast radius the fault deserves.
    """
    patterns = []

    for module_name in getattr(settings, "GEORIVA_PLUGIN_NAMES", []) or []:
        module = _import_api_urls(module_name)
        if module is None:
            continue
        if not getattr(module, "urlpatterns", None):
            logger.warning(
                "plugin %r has an %s module with no urlpatterns; it contributes no routes",
                module_name,
                API_URLS_MODULE,
            )
            continue
        patterns.append(path("", include(module)))

    return patterns


def _import_api_urls(module_name):
    """The plugin's ``api_urls`` module, or None if it has none or it is broken.

    The two are told apart deliberately, and it is the whole reason this is not
    a bare ``try: import_module``. Most plugins publish no routes at all, so
    "no such module" is the ordinary case and must stay silent — but an
    ``api_urls`` that imports a dependency the operator did not install raises
    ``ModuleNotFoundError`` too, and catching both together would file a genuine
    fault under the silent case. Asking the finder first separates "the file is
    not there" from "the file is there and does not work", and only the second
    is worth waking anybody for.
    """
    dotted = f"{module_name}.{API_URLS_MODULE}"

    try:
        if importlib.util.find_spec(dotted) is None:
            return None
    except Exception:
        logger.exception("could not look for %s; plugin %r contributes no routes", dotted, module_name)
        return None

    try:
        return importlib.import_module(dotted)
    except Exception:
        logger.exception("%s will not import; plugin %r contributes no routes", dotted, module_name)
        return None
