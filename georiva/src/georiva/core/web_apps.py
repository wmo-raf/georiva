"""Page routes a plugin contributes — where an app built on GeoRiva is served
from (ADR 0039).

ADR 0036 puts a whole application in a plugin's repository: a ``plugin/`` half
that runs inside GeoRiva and a ``web/`` half that is a built bundle. ADR 0028
gave the first half a public route, under ``/api/``. This gives the second half
an *address*: a path a plugin declares, an operator may rename, that serves the
bundle's entry document and falls back to it for unknown sub-paths.

Without it a bundle shipped in a plugin's ``static/`` folder is reachable at
exactly one URL — ``/static/<app>/index.html`` — which names the build output
rather than the application, cannot be moved, inherits the caching the static
tier chose for fingerprinted assets, and rewrites nothing, so client-side
routing has nowhere to land.

Three rules shape what is here:

**A page route lives under a reserved prefix.** ``/api/`` is a tenant's whole
public data service and ADR 0028 kept plugins from taking a path core already
answers by appending their patterns to core's. A page shares its space with
something less orderly still: Wagtail's page tree, whose slugs are an editor's
to choose at any time. So the mounts are confined to one prefix (``/app/`` by
default) and matched after core's own routes — an operator who empties the
prefix is choosing to share a space with their own page slugs, which is a choice
only they can make.

**The label is the operator's.** The plugin declares a slug, and that is a
default. The address an organisation gives its users is theirs to pick, through
``GEORIVA_WEB_APP_MOUNTS``; nothing about it should be an artefact of how the
bundle got into the image.

**Only the entry document is served here.** Assets stay on the static tier,
where fingerprinted files belong. This serves one HTML document, uncached, and
answers an asset-shaped sub-path with a 404 rather than with HTML a script tag
would try to parse.
"""

import logging
import re
from dataclasses import dataclass
from pathlib import Path

from django.conf import settings
from django.contrib.staticfiles import finders
from django.http import Http404, HttpResponse
from django.urls import path
from django.views import View

from georiva.core.plugins import import_plugin_module

logger = logging.getLogger(__name__)

#: The module a plugin publishes to contribute a page route.
WEB_APP_MODULE = "web_app"

#: The attribute that module holds its :class:`WebApp` in.
WEB_APP_ATTRIBUTE = "app"

#: Where page routes live unless the operator says otherwise.
DEFAULT_PATH_PREFIX = "app/"

#: One segment of a URL, and nothing that could be read as more than one: no
#: slash, no dot, no empty string, nothing to climb with.
_SLUG = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


@dataclass(frozen=True)
class WebApp:
    """What a plugin declares in its ``web_app`` module.

    ``slug`` is the mount label it would like, which the operator may override.
    ``index`` is the bundle's entry document as a *staticfiles* path — the same
    path ``{% static %}`` would take — because the bundle is collected with
    every other static file and the finders are the only thing that knows where
    it landed. ``name`` is for people, in a log line or an admin listing.
    """

    slug: str
    index: str
    name: str = ""


@dataclass(frozen=True)
class WebAppMount:
    """One plugin's app, at the address this instance serves it from."""

    plugin: str
    slug: str
    index: str
    name: str
    path: str


def first_segments(patterns):
    """The first path segment of each of ``patterns``, which is what a mount at
    the site root would have to stay out of.

    Empty routes are dropped: a pattern mounted at ``""`` claims no segment of
    its own, and treating it as one would reserve everything.
    """
    segments = set()
    for pattern in patterns:
        route = str(pattern.pattern).lstrip("^/")
        head = route.split("/", 1)[0]
        if head:
            segments.add(head)
    return segments


def web_app_mounts(reserved=()):
    """Every discovered plugin's app, in discovery order, at its resolved address.

    ``reserved`` is the set of first path segments core answers on, and a mount
    may not take one. It matters only when the operator has emptied the prefix,
    because the default prefix is a segment of its own — and it matters a great
    deal then: a mount's patterns are appended after core's, but Django keeps
    looking past an ``include`` whose inner patterns did not match, so a plugin
    labelled ``api`` at the root would not take ``/api/stac/`` and *would* answer
    every unmatched path under ``/api/`` with its own HTML. Core's 404s are part
    of core's surface.

    A plugin with no ``web_app`` module contributes nothing and says nothing: it
    is the ordinary case. Anything else that goes wrong costs that one plugin its
    page and is logged, because this runs while Django assembles the URL conf for
    the whole instance — an exception raised here reaches every request, STAC and
    the admin included, not just the plugin that caused it.

    A label can be claimed once. Unlike ``/api/``, where arbitrating a collision
    would have meant writing a package name into every public route (ADR 0028),
    the label is ours to police — and an operator's explicit choice claims before
    any plugin's default, so a rename cannot be refused by a default nobody
    chose. Within each of those two rounds, discovery order wins and the loser is
    told it has no page.
    """
    prefix = _path_prefix()
    reserved = set(reserved)
    chosen_labels = _chosen_labels()
    plugins = list(getattr(settings, "GEORIVA_PLUGIN_NAMES", []) or [])

    # The head of every mount is the prefix's first segment, unless there is no
    # prefix, in which case each label is its own head.
    prefix_head = prefix.split("/", 1)[0] if prefix else None
    if prefix_head is not None and prefix_head in reserved:
        logger.error(
            "GEORIVA_WEB_APP_PATH_PREFIX is %r, which core already answers on; no plugin page is served",
            prefix,
        )
        return []

    unknown = set(chosen_labels) - set(plugins)
    if unknown:
        logger.warning(
            "GEORIVA_WEB_APP_MOUNTS names %s, which are not discovered plugins; "
            "those labels are ignored and the plugins keep their own",
            ", ".join(sorted(unknown)),
        )

    candidates = []

    for plugin in plugins:
        module = import_plugin_module(plugin, WEB_APP_MODULE, logger=logger, contributes="page")
        if module is None:
            continue

        app = getattr(module, WEB_APP_ATTRIBUTE, None)
        if not isinstance(app, WebApp):
            logger.warning(
                "plugin %r has a %s module with no %s of type WebApp; it contributes no page",
                plugin,
                WEB_APP_MODULE,
                WEB_APP_ATTRIBUTE,
            )
            continue

        label = _label_for(plugin, app, chosen_labels)
        if not _SLUG.fullmatch(label or ""):
            logger.error(
                "plugin %r asks to be mounted at %r, which is not one path segment; it contributes no page",
                plugin,
                label,
            )
            continue

        if not _is_static_path(app.index):
            logger.error(
                "plugin %r declares index %r, which is not a path inside the static tree; it contributes no page",
                plugin,
                app.index,
            )
            continue

        if prefix_head is None and label in reserved:
            logger.error(
                "plugin %r asks to be mounted at %r at the site root, which core already answers on; "
                "it contributes no page",
                plugin,
                label,
            )
            continue

        candidates.append((plugin, app, label))

    kept = _claim_labels(candidates, chosen_labels)
    mounts = []

    for plugin, app, label in candidates:
        if kept.get(label) != plugin:
            continue
        mount = WebAppMount(
            plugin=plugin,
            slug=label,
            index=app.index,
            name=app.name or plugin,
            path=f"/{prefix}{label}/",
        )
        logger.info("plugin %r serves %r at %s", plugin, mount.name, mount.path)
        mounts.append(mount)

    return mounts


def _claim_labels(candidates, chosen_labels):
    """Which plugin holds each label: ``{label: plugin}``.

    Two rounds, so that an operator's rename beats a plugin's default rather than
    being refused by it. The loser of either round is told, at ERROR, because a
    page that is installed and not served is exactly the confusion this
    mechanism exists to remove.
    """
    held = {}

    for operators_choice in (True, False):
        for plugin, _app, label in candidates:
            if (plugin in chosen_labels) is not operators_choice:
                continue
            if label in held:
                logger.error(
                    "plugin %r asks for the mount %r, which plugin %r holds; it contributes no page",
                    plugin,
                    label,
                    held[label],
                )
                continue
            held[label] = plugin

    return held


def web_app_urlpatterns(reserved=()):
    """The page routes discovered plugins contribute.

    Two patterns per mount: the mount itself, named ``web_app_<plugin>`` so a
    plugin can reverse its own address without writing down a label the operator
    chose, and a catch-all beneath it. The catch-all is what makes a shareable
    URL possible — the server knows nothing about the app's own routes, so every
    path under the mount is the same document.

    ``reserved`` is passed through to :func:`web_app_mounts`; the caller holds
    core's patterns, so the caller derives it.
    """
    patterns = []

    for mount in web_app_mounts(reserved):
        route = mount.path.lstrip("/")
        view = WebAppIndexView.as_view(index=mount.index, plugin=mount.plugin)
        patterns.append(path(route, view, name=f"web_app_{mount.plugin}"))
        patterns.append(path(f"{route}<path:subpath>", view))

    return patterns


class WebAppIndexView(View):
    """Serves one bundle's entry document, for the mount and everything under it."""

    http_method_names = ["get", "head", "options"]

    #: The staticfiles path of the document, and the plugin to name in a log line.
    index = None
    plugin = None

    def get(self, request, subpath=""):
        if _asset_shaped(subpath):
            # Assets are the static tier's job. A dotted last segment under the
            # mount is a mistaken asset reference, and answering it with HTML
            # turns a missing file into a parse error somewhere less obvious.
            raise Http404(f"{subpath!r} is not a page of this app")

        document = self._document()
        if document is None:
            logger.error(
                "plugin %r is mounted but %r is not in the static tree; its page is not served",
                self.plugin,
                self.index,
            )
            raise Http404(f"{self.index!r} was not found")

        response = HttpResponse(document, content_type="text/html; charset=utf-8")
        # A document, not a fingerprinted asset. An operator who replaces the
        # bundle must not have to wait out a cache they never chose.
        response["Cache-Control"] = "no-cache"
        return response

    def _document(self):
        """The entry document's bytes, or None if it is not there.

        Resolved per request rather than once at startup: the bundle may be
        replaced under a running instance, and a plugin whose bundle was never
        built should cost its own page rather than the URL conf. The finders are
        asked first, so this works the same in dev and on a server; ``STATIC_ROOT``
        is the fallback for a deployment where the collected tree is all there is.
        """
        found = finders.find(self.index)
        if found is None and settings.STATIC_ROOT:
            collected = Path(settings.STATIC_ROOT) / self.index
            found = str(collected) if collected.is_file() else None
        if found is None:
            return None
        return Path(found).read_bytes()


def _path_prefix():
    """The reserved prefix, normalised to ``""`` or ``"something/"``."""
    prefix = getattr(settings, "GEORIVA_WEB_APP_PATH_PREFIX", DEFAULT_PATH_PREFIX)
    prefix = (prefix or "").strip().strip("/")
    return f"{prefix}/" if prefix else ""


def _chosen_labels():
    """The operator's labels, by plugin: ``GEORIVA_WEB_APP_MOUNTS``."""
    return getattr(settings, "GEORIVA_WEB_APP_MOUNTS", None) or {}


def _label_for(plugin, app, chosen_labels):
    """The operator's label for this plugin's app, falling back to the plugin's own."""
    label = chosen_labels[plugin] if plugin in chosen_labels else app.slug
    return str(label or "").strip().strip("/")


def _is_static_path(index):
    """Whether ``index`` is a plain relative path the finders could answer for."""
    if not index or index.startswith("/") or "\\" in index:
        return False
    return all(segment not in ("", ".", "..") for segment in index.split("/"))


def _asset_shaped(subpath):
    """Whether a sub-path looks like a file rather than one of the app's routes."""
    last = subpath.rstrip("/").rsplit("/", 1)[-1]
    return "." in last
