# Plugins contribute page routes under a reserved prefix

## Status

accepted

## Context

ADR 0036 puts a whole application in a plugin's repository: a `plugin/` half that
runs inside GeoRiva and a `web/` half that is a built bundle talking to GeoRiva
over HTTP only. ADR 0028 gave the first half a public route, under `/api/`. The
second half had no address, and ADR 0036 said so: "Plugin installation (ADR 0035)
carries Python packages and has no way to bring a frontend container, so `web/`
needs its own deployment step for now."

`georiva-forecast-viewer` sidesteps the container by committing its built bundle
into the plugin's `static/` folder. `collectstatic` collects it, one
`plugins.toml` install brings both halves, and the app is reachable at exactly
one address:

```
/static/forecast_viewer/index.html
```

That works, and it is not an address anybody should be given.

- It names the build output, not the application. The URL an operator hands their
  users is an artefact of how the bundle got into the image.
- There is nowhere else to put it. An operator cannot serve the viewer at
  `/viewer/`, at `/app/forecast/`, or anywhere their users would look.
- It closes off shareable URLs. The moment an app wants "send someone this model
  at this run" it needs a path that unknown sub-paths fall back to, and `/static/`
  rewrites nothing.
- `/static/` is for assets. An entry point served from it inherits the caching
  and headers that tier chose for fingerprinted files, which is the opposite of
  what an HTML document wants.

So a plugin needs a **page route**: a user-facing path outside `/api/`, declared
by the plugin, that serves the bundle's `index.html`.

Two things make this a different question from ADR 0028's, rather than the same
one with a different prefix.

**A page shares its space with the page tree.** `/api/` is core's, written down
in one file, and ADR 0028 kept a plugin off a path core already answers by
appending plugin patterns after core's. A page route's neighbours are Wagtail
pages, whose slugs are an editor's to choose at any time, in a tenant's own
database. Appending is no protection there: Wagtail's resolver answers for
*every* path, so order alone decides, and whichever way it is decided, one side
is a surprise — either installing a plugin shadows a page an editor made, or
making a page silently unpublishes an app.

**The address is the operator's business, not the plugin's.** An API route is a
contract with somebody's client and belongs to the plugin that publishes it;
that is why ADR 0028 refused to put a package name in front of it. A page is what
an organisation tells its users to visit. Two instances may reasonably serve the
same app at `/viewer/` and at `/app/forecasts/`, and neither is the plugin's
call.

## Decision

**A discovered plugin exposing a `web_app` module has its bundle served as a
page, under a prefix reserved for plugin pages.** `georiva.core.web_apps`
discovers them, `config/urls.py` mounts what it returns between core's own routes
and the page tree's catch-all.

The declaration is data, not routes:

```python
# georiva_forecast_viewer/web_app.py
from georiva.core.web_apps import WebApp

app = WebApp(slug="forecast-viewer", index="forecast_viewer/index.html", name="Forecast Viewer")
```

`index` is a *staticfiles* path, resolved through the finders — the bundle is
collected with every other static file and the finders are the only thing that
knows where it landed. A plugin therefore keeps shipping its bundle exactly as it
does today; what changes is that the bundle is no longer its own address.

**It is data and not a `urlpatterns` list** because the serving behaviour is the
mechanism, not the plugin's: the entry document, the fallback, the headers and
the sub-path rule should be the same for every app, and a plugin that wrote its
own view would get them subtly differently and would also be free to mount itself
wherever it liked.

**The prefix is reserved and the label is the operator's.**
`GEORIVA_WEB_APP_PATH_PREFIX` defaults to `app/`; nothing under it is a Wagtail
page, so a mount can collide with neither core's routes nor an editor's slug.
Within it, the plugin's `slug` is a default and `GEORIVA_WEB_APP_MOUNTS` is the
operator's rename — `georiva_forecast_viewer=viewer` serves it at `/app/viewer/`.
This is the one place a settings entry is right where ADR 0028 refused one: it
does not restate what is installed, it states a choice only the operator can
make, and its absence means the plugin's default rather than "not served".

**An operator may empty the prefix**, mounting apps at the site root — `/viewer/`
— and then owns the collision with their own page slugs. It is refused to a
plugin and allowed to a person: an operator can see their page tree, and a
plugin's author cannot.

**No mount may take a first path segment core answers on.** Appending the mounts
after core's patterns is not enough on its own, because Django keeps looking past
an `include` whose inner patterns did not match. A plugin labelled `api` at the
root would leave `/api/stac/` alone — and would answer every *unmatched* path
under `/api/` with its own HTML, turning core's 404s into one plugin's front
page. So `config/urls.py` hands the page mechanism the first segment of each of
core's own patterns (`reserved_segments`) and a mount that asks for one is
refused. This is what confines the root-mount choice to a collision with the page
tree, which is the only collision an operator can see and judge. The same check
refuses a prefix pointed at core's space, and then no page is served at all.

**The label an operator wrote down claims before any plugin's default.** A label
can be held once, so collisions are resolved in two rounds: operators' choices
first, defaults second, discovery order within each. Otherwise a rename could be
refused by a default nobody chose, which would make "the label is the operator's"
false exactly when it matters. A label naming a plugin that is not installed is
reported, because the alternative is an app sitting silently at its default
address while the operator looks for it at the one they chose.

**Core's routes are matched first, the page tree last.** The mounts sit between.
The first half is ADR 0028's rule, now enforced by the prefix as well as by
ordering. The second half is what makes a root mount reachable at all, and it is
the reason the default prefix exists: with it, nothing of the page tree is taken.

**The mount serves one document and falls back to it.** Every path under the
mount returns the bundle's `index.html`, which is the whole of what client-side
routing needs. Two refinements:

- **an asset-shaped sub-path is a 404.** If the last segment has a dot in it, the
  request is a mistaken asset reference — assets live on the static tier — and
  answering it with HTML turns a missing file into a parse error somewhere less
  obvious. An app route whose last segment contains a dot is not addressable;
  that is the price, and it is cheap.
- **the document is sent `Cache-Control: no-cache`.** It is a document, not a
  fingerprinted asset. An operator who replaces a bundle must not have to wait
  out a cache they never chose.

**The document is resolved per request**, not once at startup, so a bundle can be
replaced under a running instance and a plugin whose bundle was never built costs
its own page rather than the URL conf.

**Faults cost one plugin its page.** The URL conf is built once for the whole
instance, so an exception while assembling it reaches every request — the STAC
API and the admin included. A `web_app` that will not import, declares no
`WebApp`, asks for a slug that is not one path segment, names an index outside
the static tree, or asks for a mount another plugin already holds, is logged and
skipped. "Absent" is told from "broken" by asking the finder first, exactly as in
ADR 0028, and that import rule now lives in one place
(`plugins.import_plugin_module`) used by both conventions.

**A mount label can be claimed only once.** Unlike `/api/`, where arbitrating a
collision would have meant writing a package name into every public route, the
label is ours: the first plugin in discovery order keeps it and the second is
told, at ERROR, that it has no page.

**The Plugins admin page says where each app is served.** The label is set in the
environment, so without this the only way to read it back is the settings file or
a log line — and an operator asking "where is the viewer" is asking from the
admin.

Nothing changes at the proxy. `/app/` reaches Django through the same
`location /` every other page route uses.

## Alternatives considered

**A `page_urls` module, mirroring `api_urls`.** Symmetric with ADR 0028 and the
obvious first idea. Rejected because the serving is the hard part: every app
needs the same entry document, fallback, headers and sub-path rule, and a
`urlpatterns` list leaves each plugin to get those right — and free to mount
itself at any path, which is what the reserved prefix exists to prevent.

**Mount the bundle as a static route, not through a view.** Faster, and it is how
assets are served. Rejected because the fallback is the point: a static handler
answers 404 for an app's own routes, and teaching it otherwise is writing this
view with fewer places to put the reasoning.

**A Wagtail page type that renders the bundle.** Puts the address where an editor
already chooses addresses, which is genuinely attractive, and it would make the
collision question disappear. Rejected for now because it couples an app's
availability to a tenant's page tree and to the admin's permissions, requires a
page to be created per organisation before the app exists anywhere, and makes an
app's URL unreversible from the plugin. Worth reopening if operators ask for apps
in their navigation rather than at a fixed path.

**One mount per organisation, in the database.** The tenancy-shaped answer: each
organisation picks its own label for an app it serves. Rejected because the URL
conf is built once per process and a per-tenant label would have to be resolved
per request against the host; the instance-wide label costs nothing today, since
every organisation reaches the same path on its own host and sees its own data
there.

**A frontend container per app.** ADR 0036's unsolved half, and the answer for an
app with server-side rendering or its own build at deploy time. Rejected as the
mechanism here because it is a deployment change for every operator of every app,
and the bundle-in-the-plugin install that ADR 0035 already carries is enough for
a static bundle. This decision does not preclude it; it removes the address
problem from it.

**Leaving it at `/static/<app>/index.html`.** Costs nothing to keep. Rejected
because it is the one address an app can never have, for the four reasons in the
context.

## Consequences

- A plugin that ships a bundle gets a page by adding one module, and its address
  is the operator's. `georiva-forecast-viewer` is the first, and the
  `/static/forecast_viewer/index.html` path keeps working — a bundle is still
  collected static files — so nothing has to move at once.
- A bundle built with a `base` of `/static/<app>/` keeps working unchanged: the
  page is served from the mount and its assets are still requested from
  `/static/`. An app that wants its assets under the mount instead would need
  this mechanism to serve them, which it deliberately does not.
- `/app/` is reserved instance-wide. A page tree with an `app` slug at its root
  is shadowed; the slug is a plain one, so this is worth saying in the
  installation docs rather than detecting.
- The ordering in `config/urls.py` is now load-bearing in both directions, so the
  composition is a function (`build_urlpatterns`) over two named lists — a test
  assembles the real thing with a plugin discovered rather than restating it.
  `reserved_segments()` lives there too, and the Plugins admin page asks the same
  question, so a mount the URL conf refused is not shown as an address.
- An app route whose last path segment contains a dot cannot be addressed.
- The Plugins admin page shows each plugin's address, because the address is
  chosen in the environment and an operator should not have to read settings or a
  log line back to find where their app ended up. There is also an INFO line per
  mount when the URL conf is built, which an instance logging at WARNING will not
  show.
- The entry document is read from disk on every request to the mount, with no
  cache. It is a few kilobytes and it is what makes replacing a bundle take
  effect; a per-process cache is the additive way back if an app ever gets
  traffic that makes it matter.

## Amendment: the portal links to the apps it serves

An address nobody can find is only half an address. The public portal now lists
every served app, in an **Apps** dropdown in the navbar and an **Apps** section on
the home page just after Explore. Both show the same list.

- **The declaration also says how the app is presented.** `WebApp` gains an
  optional `icon` (a Bootstrap Icons name, defaulting to `grid-3x3-gap`) and
  `description` (which may be a lazy translation). Both are optional, so a plugin
  declared before this keeps its page and gets a generic card.
- **The links are read back from the URL conf** (`served_web_apps()`), not
  worked out again. A link points at wherever the resolver actually answers,
  including any operator rename or prefix, and rendering a page does not repeat
  `web_app_mounts()`'s log lines.
- **Shown by default, curated per site.** Each organisation's `GeoRivaSettings`
  holds `PortalAppEntry` rows, which record exceptions only: hide an app, restate
  its title, description or icon, or move it up. An app with no row is shown as
  its plugin describes it, after the apps that have a row. Installing a plugin
  needs nothing from any site's editors.
- **Rows are keyed by plugin name, not by mount label.** The label is the
  operator's to rename, and a rename must not detach an editor's choices. A row
  whose plugin is no longer served is skipped on the portal and shown in the
  admin as "(not installed)". Two rows for one app are refused.
- Whether a site lists an app is a presentation choice only. Every site's host
  serves every mount, and the app resolves its organisation from the host like
  any other request.
