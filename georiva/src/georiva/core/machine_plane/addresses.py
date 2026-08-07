"""The addresses of the services that answer without a Host.

Titiler and Martin read object storage and the database directly. Neither sees a
usable Host header — the browser dials them through nginx on whatever hostname
the portal happens to run on, and Titiler dials Django back on an internal
container name that belongs to no organisation at all. So neither can resolve a
tenant the way ``OrganisationMiddleware`` does, and neither should try:
tenancy logic copied into a second and third service is tenancy logic that
drifts out of step with the first.

The organisation therefore travels *in the address*. Titiler's routes open with
an ``{org}`` segment, Martin's function takes an ``org`` parameter, and both stay
purely conventional — they read the org they were handed and join or concatenate
with it, deciding nothing. Deciding is Django's job, and this module is where it
happens: every machine-plane URL on the instance is built here, from an object
whose organisation was already established by the request that fetched it. Host
and path then agree because the same request produced both.

The grammar mirrors the storage grammar deliberately (``{org}/{catalog}/…``, see
``core.storage.path_resolution``): a Titiler tile URL and the COG key behind it differ
only by prefix, so an operator reading one can find the other.

Since #274 the grammar is read as well as written: the nginx gateway hands
Django a tile URL it did not build and asks whose collection it addresses.
:func:`scope_of` is that reader, and it lives here rather than beside the gate
for one reason — it is the exact inverse of the builders above, and an inverse
that drifts from its function is a gate that authorises one collection while the
tile server reads another.
"""
import hashlib
import json
from collections import namedtuple
from urllib.parse import parse_qs, urlencode

TITILER_PREFIX = "/titiler"
MARTIN_PREFIX = "/martin"

#: Martin's function source, mounted under its own tile path.
MARTIN_BOUNDARY_STATS_SOURCE = "boundary_stats"

#: The organisation, catalog and collection a machine-plane URL addresses —
#: everything needed to decide whether it may be served, and nothing more. The
#: variable is deliberately absent: ``visibility`` is a property of the
#: collection, so naming the variable here would imply a check that is not made.
MachineScope = namedtuple("MachineScope", "org catalog collection")


def _titiler_scope(segments):
    """The scope of ``/titiler/{org}/{catalog}/{collection}/{variable}/…``.

    Every Titiler route on the instance sits under the four-segment prefix
    (``TILE_ROUTE_PREFIX`` in ``titiler-app/app/main.py``), so a URL that does
    not reach a variable addresses no data and gets no scope — which the gate
    reads as a denial. The service's own ``/docs`` and ``/openapi.json`` fall
    under that too, and being unreachable through the proxy is the correct
    answer for them.
    """
    if len(segments) < 5:
        return None
    return MachineScope(*segments[1:4])


def _martin_scope(query):
    """The scope of Martin's zonal-stats tile, carried in its query parameters.

    ``georiva_boundary_stats`` already requires the triple and answers an empty
    tile for one that names nothing (ADR 0013). This reads the same three, with
    one addition: a parameter given twice yields no scope at all. Martin's own
    choice between duplicates is not something this side can see, so a request
    that leaves it a choice is refused rather than authorised against a guess.
    """
    params = parse_qs(query)
    values = [params.get(name, []) for name in ("org", "catalog", "collection")]
    if not all(len(v) == 1 and v[0] for v in values):
        return None
    return MachineScope(*(v[0] for v in values))


def scope_of(uri):
    """The collection a machine-plane ``uri`` addresses, or ``None``.

    Takes the original request line as nginx received it — path and query, still
    percent-encoded — and returns the triple that decides whether it may be
    served. Anything this does not recognise returns ``None``, and the caller
    treats that as "no". Encoded slugs are left encoded on purpose: they then
    match no row and are denied, which is the right answer for an address whose
    spelling is trying to be two things at once.

    Recognising is all this does. Whether the collection exists, and who may see
    it, is :mod:`georiva.core.machine_plane.auth_view`'s question.
    """
    path, _, query = (uri or "").partition("?")
    segments = [segment for segment in path.split("/") if segment]
    if not segments:
        return None
    if segments[0] == TITILER_PREFIX.strip("/"):
        return _titiler_scope(segments)
    if segments[:2] == [MARTIN_PREFIX.strip("/"), MARTIN_BOUNDARY_STATS_SOURCE]:
        return _martin_scope(query)
    return None


def org_slug_of(collection) -> str:
    """The slug of the organisation owning ``collection``'s catalog.

    Takes a ``Collection`` and nothing else. An earlier version accepted
    anything with a ``.collection`` attribute, which quietly did the wrong thing
    for an ``Asset`` (whose ``.collection`` route runs through ``.item``) — and
    the wrong thing here is a URL under another institution's prefix. Callers
    holding an item or a variable pass ``obj.collection``; it is one attribute,
    and it says which chain is being walked.

    Deliberately not defensive beyond that: a missing link is a broken row, and
    a blank org segment would address nothing or somebody else.
    """
    return collection.catalog.organisation.slug


def titiler_variable_root(org_slug, catalog_slug, collection_slug, variable_slug) -> str:
    """The Titiler route prefix identifying one variable of one organisation."""
    return f"{TITILER_PREFIX}/{org_slug}/{catalog_slug}/{collection_slug}/{variable_slug}"


def titiler_preview_url(item, variable, style=None) -> str:
    """A rendered preview of ``item``'s ``variable`` band.

    The organisation comes from the item rather than from the caller, so a page
    cannot hand Titiler a catalog of one tenant under the org segment of
    another: the two segments are read from the same row.

    ``style`` (a ``VariableStyle`` row) pins a non-default style as a query
    param — a rendering parameter, never a path segment (ADR 0023). Omission
    deliberately names nothing: a URL that spelled out the default's slug
    would survive a default flip still pointing at the old default.

    ``variable`` is the ``Variable`` row where the caller holds one, and the
    URL then carries the per-style version token that makes a colorized
    response honestly cacheable — an edited style or range is a different
    URL. A bare slug is accepted for callers holding nothing more (the page
    templatetag); those thumbnails are never cached, so an untokened URL
    stays honest there.
    """
    collection = item.collection
    params = {"time": item.time_iso}
    if item.reference_time:
        params["reftime"] = item.reference_time_iso
    if isinstance(variable, str):
        variable_slug, variable = variable, None
    else:
        variable_slug = variable.slug
    if style is not None:
        params["style"] = style.slug
    if variable is not None:
        params["v"] = style_version_token(
            variable, style if style is not None else variable.default_style,
        )
    root = titiler_variable_root(
        org_slug_of(collection), collection.catalog.slug, collection.slug, variable_slug,
    )

    return f"{root}/preview.webp?{urlencode(params)}"


def render_config_token(variable) -> str:
    """A short hash of everything baked into an encoded texture's pixels.

    Carried as ``v=`` on :func:`titiler_encoded_preview_url` so a change to the
    variable's render range is a *different URL* — the endpoint answers with an
    immutable cache header, and this token is what makes that safe (ADR 0021).
    A hash rather than a stored counter: identical config always yields the
    same URL, and there is no version state to migrate or drift.
    """
    basis = f"{variable.value_min}:{variable.value_max}:{variable.scale_type or 'linear'}"
    return hashlib.sha1(basis.encode()).hexdigest()[:8]


def style_version_token(variable, style=None) -> str:
    """A short hash of everything baked into a *colorized* output's pixels.

    Extends :func:`render_config_token` with the style's stops snapshot
    (ADR 0023): a colorized preview or tile renders range, scale *and*
    palette, so any of the three changing must change the URL for its
    responses to be honestly cacheable. ``style`` is the one being rendered —
    the default for a styleless URL — or ``None`` for a still-grayscale
    variable, whose empty basis segment keeps the token stable until a first
    style appears. Encoded textures deliberately keep the styleless token:
    encoding bakes no palette in, and sharing this hash would churn every
    cached texture on a palette edit.
    """
    stops = json.dumps(style.stops, sort_keys=True) if style is not None else ""
    basis = (
        f"{variable.value_min}:{variable.value_max}"
        f":{variable.scale_type or 'linear'}:{stops}"
    )
    return hashlib.sha1(basis.encode()).hexdigest()[:8]


def titiler_encoded_preview_url(item, variable) -> str:
    """The value-encoded texture of ``item``'s ``variable`` band (ADR 0021).

    The whole extent as one image, pixels rescaled to the variable's current
    ``value_min``/``value_max`` — the machine texture WeatherLayers unscales
    client-side. Derived at request time from the COG, so nothing behind this
    URL can go stale; the ``v`` token turns render-config edits into new URLs.
    The org comes from the item's own row, as in :func:`titiler_preview_url`.
    """
    collection = item.collection
    params = {"time": item.time_iso}
    if item.reference_time:
        params["reftime"] = item.reference_time_iso
    params["v"] = render_config_token(variable)
    root = titiler_variable_root(
        org_slug_of(collection), collection.catalog.slug, collection.slug, variable.slug,
    )

    return f"{root}/encoded-preview.png?{urlencode(params)}"


def martin_boundary_stats_url(collection, base) -> str:
    """Martin's zonal-stats tile URL for ``collection``, less the varying params.

    Carries the ``org``/``catalog``/``collection`` triple that pins the tile to
    one organisation's rows; the caller drawing the map appends the parameters
    that change as it is driven (``variable``, ``time``, ``admin_level``,
    ``reference_time``). Those name nothing outside the triple, so a client
    editing them can only ever move within the collection it was given.

    ``base`` is required rather than defaulted to :data:`MARTIN_PREFIX`: the map
    fetches these cross-origin in some deployments, so every real caller builds
    an absolute URL from the request, and a default would only ever be a wrong
    answer that looked like a working one.
    """
    params = urlencode({
        "org": org_slug_of(collection),
        "catalog": collection.catalog.slug,
        "collection": collection.slug,
    })
    return f"{base.rstrip('/')}/{MARTIN_BOUNDARY_STATS_SOURCE}/{{z}}/{{x}}/{{y}}?{params}"
