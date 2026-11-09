"""The apps a portal links to — its navbar's Apps menu and its home page's Apps
section, which are one list.

Two things decide it. The instance decides what exists: every app a plugin
contributes and the URL conf serves (ADR 0039), at the address the operator gave
it. Each site decides what it shows: a ``PortalAppEntry`` row may hide an app,
restate its title, description or icon, or move it up. Everything else is
shown as the plugin describes it — a newly installed app needs nothing from
anybody to be found.
"""

from dataclasses import dataclass

from georiva.core.web_apps import DEFAULT_ICON, served_web_apps


@dataclass(frozen=True)
class PortalApp:
    """One link, ready to draw."""

    plugin: str
    title: str
    description: str
    icon: str
    url: str


def portal_apps(request):
    """The apps this request's site lists, in order.

    Apps that have a row come first, in row order; the rest follow in the order
    the URL conf mounted them. A row naming a plugin that is not served is
    skipped, and so is a second row for the same app.
    """
    from .models import GeoRivaSettings

    mounts = {mount.plugin: mount for mount in served_web_apps()}
    if not mounts:
        return []

    entries = GeoRivaSettings.for_request(request).app_entries.all()

    apps = []
    decided = set()

    for entry in entries:
        if entry.plugin in decided:
            continue
        decided.add(entry.plugin)
        mount = mounts.get(entry.plugin)
        if mount is None or entry.hidden:
            continue
        apps.append(_portal_app(mount, entry))

    for plugin, mount in mounts.items():
        if plugin not in decided:
            apps.append(_portal_app(mount))

    return apps


def _portal_app(mount, entry=None):
    def restated(field):
        value = getattr(entry, field, "") if entry is not None else ""
        return value.strip() if value else ""

    icon = restated("icon").removeprefix("bi-")

    return PortalApp(
        plugin=mount.plugin,
        title=restated("title") or str(mount.name),
        description=restated("description") or str(mount.description or ""),
        icon=icon or mount.icon or DEFAULT_ICON,
        url=mount.path,
    )
