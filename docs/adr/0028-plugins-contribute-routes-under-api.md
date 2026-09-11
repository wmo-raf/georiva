# Plugins contribute routes under `/api/`

## Status

accepted

## Context

A plugin can already add models, a Wagtail admin surface, Celery tasks, a format
handler, a source and a recipe. It cannot add a **public route**. Everything
served under `/api/` is written into `georiva/api/urls.py`, which is core's file
in core's repository, so a plugin that publishes something over HTTP has no way
to say so.

That gap has been filled once already, and wrongly. A point-forecast endpoint was
added to core as `georiva.api.ForecastView`, with its settings in core's
settings, its cache zone in core's nginx and a bind mount in core's compose —
one plugin's feature spread across five files core had no reason to know about.
The rule that was broken is that core holds generic mechanisms and a plugin holds
everything shaped like its own domain. Core does not need to know what a point
forecast is; it needs to know that a plugin may have routes.

Three things make this more delicate than "import a list".

**`/api/` is a tenant's whole public service.** It is the surface an
organisation's consumers are pointed at, and `/api/stac/` in particular is a
standards-conformant API somebody else's client is already parsing. A mechanism
that lets a plugin take a path core already answers is a mechanism that lets
installing a plugin silently move the STAC API.

**The URL conf is built once, for the whole instance.** Django assembles the
resolver on first use and caches it. An exception raised while assembling it is
not confined to the routes that raised it — it reaches every request, so one
plugin with a missing dependency would return 500 for `/api/stac/`, the admin and
the tile config alike.

**"No routes" and "broken routes" raise the same exception.** A plugin that
publishes no `api_urls` raises `ModuleNotFoundError` on import. So does an
`api_urls` that imports a dependency the operator did not install. The ordinary
case and the fault are indistinguishable to a bare `try: import_module`, and the
ordinary case is much the more common — so a single handler would file every
genuine fault under "nothing to see".

## Decision

**A discovered plugin exposing an `api_urls` module has its `urlpatterns`
included under `/api/`.** The convention is the whole of the registration:
`georiva.core.plugins.api_urlpatterns()` walks `settings.GEORIVA_PLUGIN_NAMES`,
and `georiva/api/urls.py` appends what it returns.

There is no registry and no settings entry. A second list of what is routed is a
second thing that can disagree with what is installed, and the failure mode of
that disagreement — a plugin installed and not routed, or routed and not
installed — is exactly the confusion this is meant to remove.

**Core's patterns are matched first.** The plugin patterns are appended, never
merged. The resolver stops at the first match, so a plugin declaring `stac/`
declares a route that can never be reached. That is a plugin's own bug and it
costs that plugin one route; the alternative costs the instance its STAC API.

Between plugins the order is discovery order and the first match wins. Nothing
arbitrates that, because the only mechanism that would — a mandatory namespace
prefix derived from the plugin's package name — puts the package name in every
URL a plugin publishes, and a public API route is a contract with somebody
else's client, not a place to encode our packaging.

**A plugin whose `api_urls` will not import is logged and skipped.** Its routes
are lost; nothing else is. The blast radius matches the fault.

**"Absent" is told from "broken" by asking the finder first.**
`importlib.util.find_spec` answers whether the module exists; `import_module`
answers whether it works. A plugin with no `api_urls` returns quietly, and only a
module that is there and fails is logged. This is the only part of the mechanism
that is not obvious from reading it, and it is the part that decides whether an
operator ever hears about a broken plugin.

A plugin may set `app_name` in its `api_urls`; `include` is handed the module, so
the namespace it declares is the namespace it gets.

## Alternatives considered

**An entry point (`georiva.api_urls` in `pyproject.toml`).** The standard Python
answer, and it would work. Rejected because GeoRiva's dev plugins are bind
mounted and made importable by path (`_discover_plugin_apps`), not always
installed as distributions — an entry point would be visible for a pip-installed
plugin and invisible for the same plugin in dev, which is the worst possible
place for a mechanism to differ.

**A settings list (`GEORIVA_PLUGIN_API_URLS`).** Explicit, and inspectable
without importing anything. Rejected for the reason above: it is a second
statement of what is installed, maintained by hand, and wrong the first time
somebody adds a plugin and not the list.

**Mounting each plugin under its own prefix.** Removes plugin-to-plugin
collisions by construction and would also remove the core-shadowing question.
Rejected because it puts `georiva_publisher_forti/` in front of a route whose
whole point is to be `/api/forecast/` — a public contract named after our
packaging.

**Letting the exception propagate.** Simplest, and the usual Django posture:
a broken URL conf is a deployment error and should be loud. Rejected because the
unit of brokenness here is not the deployment but one plugin, and the instance
serves other tenants' data through the same resolver.

## Consequences

- A plugin can publish a public route without core knowing anything about it.
  This is what makes it possible for the forti serving plane — the view, the
  route, its settings and its throttle — to live entirely in its plugin.
- Two plugins declaring the same path resolve by discovery order, silently. There
  is no check for it, and it will be found by somebody's route not answering.
  A collision-detection pass at startup is the additive way back if it is ever
  wanted; it is not written now because there is one plugin with routes.
- A plugin's routes can disappear because of an import error in that plugin and
  the instance will keep serving. The log line is the only signal, so it names
  the plugin and says its routes are not served.
- `georiva/api/urls.py` now has an ordering that is load-bearing rather than
  incidental. It is asserted by a test that resolves `/stac/` against core's real
  pattern table with a plugin declaring `stac/` appended.
