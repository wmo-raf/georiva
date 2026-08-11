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

#: The path segment closing the org-scoped KVP WMTS endpoint,
#: ``/titiler/{org}/wmts`` — three segments exactly, so it can never be read
#: as the ``{org}/{catalog}/{collection}/{variable}`` tile grammar.
WMTS_KVP_SEGMENT = "wmts"

#: The organisation, catalog and collection a machine-plane URL addresses —
#: everything needed to decide whether it may be served, and nothing more. The
#: variable is deliberately absent: ``visibility`` is a property of the
#: collection, so naming the variable here would imply a check that is not made.
#: A WMTS GetCapabilities scopes to the organisation alone — catalog and
#: collection are ``None`` — because discovery addresses no collection: the
#: document itself filters per layer inside Django (#354).
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


def _wmts_scope(org, query):
    """The scope of a KVP request on ``/titiler/{org}/wmts``.

    KVP parameter *names* are case-insensitive (OGC 06-121r3, and legacy
    clients exercise that), so ``layer`` and ``LAYER`` are one parameter — and
    two spellings of it are a repetition, refused for the same reason Martin's
    duplicates are: which one the shim would read is not visible from here.

    ``GetTile`` and ``GetFeatureInfo`` name their data in ``LAYER`` as
    ``catalog:collection:variable`` — the organisation stays in the path, never
    in the identifier (#354). ``GetCapabilities`` names no collection at all
    and scopes to the organisation alone; anything else, or anything
    half-spelt, scopes to nothing.

    Unlike path slugs, ``LAYER`` percent-decodes here (``parse_qs``), exactly
    as Martin's query triple does: the shim on the other side of the proxy
    reads the same decoded value, so the collection this authorises is still
    the collection that will be read.
    """
    params = {}
    for name, values in parse_qs(query, keep_blank_values=True).items():
        params.setdefault(name.lower(), []).extend(values)

    requests = params.get("request", [])
    if len(requests) != 1:
        return None
    operation = requests[0].lower()
    if operation == "getcapabilities":
        return MachineScope(org, None, None)
    if operation not in ("gettile", "getfeatureinfo"):
        return None

    layers = params.get("layer", [])
    if len(layers) != 1:
        return None
    parts = layers[0].split(":")
    if len(parts) != 3 or not all(parts):
        return None
    return MachineScope(org, parts[0], parts[1])


def scope_of(uri):
    """The collection a machine-plane ``uri`` addresses, or ``None``.

    Takes the original request line as nginx received it — path and query, still
    percent-encoded — and returns the triple that decides whether it may be
    served. Anything this does not recognise returns ``None``, and the caller
    treats that as "no". Encoded *path* slugs are left encoded on purpose: they
    then match no row and are denied, which is the right answer for an address
    whose spelling is trying to be two things at once. Query values — Martin's
    triple, the WMTS ``LAYER`` — decode instead, because the service behind the
    proxy decodes them identically before reading.

    Recognising is all this does. Whether the collection exists, and who may see
    it, is :mod:`georiva.core.machine_plane.auth_view`'s question.
    """
    path, _, query = (uri or "").partition("?")
    segments = [segment for segment in path.split("/") if segment]
    if not segments:
        return None
    if segments[0] == TITILER_PREFIX.strip("/"):
        if len(segments) == 3 and segments[2] == WMTS_KVP_SEGMENT:
            return _wmts_scope(segments[1], query)
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
        # A pinned style knows its own variable, so even a slug-only caller
        # gets the token then — a styled-but-untokened URL would undo the
        # honesty the token exists for.
        variable_slug, variable = variable, style.variable if style else None
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
    return hashlib.sha1(_render_range_basis(variable).encode()).hexdigest()[:8]


def _render_range_basis(variable) -> str:
    """The hash basis both tokens share: everything a rescale bakes in.

    One spelling for the two token schemes, so a change to what "the render
    range" means cannot move one token and not the other."""
    return f"{variable.value_min}:{variable.value_max}:{variable.scale_type or 'linear'}"


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
    basis = f"{_render_range_basis(variable)}:{stops}"
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


def wmts_kvp_endpoint(organisation) -> str:
    """The org-scoped KVP WMTS endpoint under the Titiler proxy (#354).

    The one paste-able URL a legacy client needs: GetCapabilities, GetTile and
    GetFeatureInfo all answer here, addressed by KVP parameters rather than
    path segments. Org-level rather than variable-level on purpose — the whole
    point of WMTS is that one URL per organisation discovers everything — so
    the argument is the ``Organisation`` row itself, not something to walk an
    org out of.
    """
    return f"{TITILER_PREFIX}/{organisation.slug}/{WMTS_KVP_SEGMENT}"


def wmts_layer_identifier(variable) -> str:
    """The WMTS layer identifier a capabilities document advertises (#354).

    ``catalog:collection:variable`` — the organisation stays in the URL path,
    never in the identifier, because the document is already per-organisation
    and a second spelling of the tenant could disagree with the first. Not a
    URL, but machine-plane grammar all the same: :func:`_wmts_scope` splits a
    KVP ``LAYER`` parameter back into this triple, and a writer living
    anywhere else is an inverse waiting to drift.
    """
    collection = variable.collection
    return f"{collection.catalog.slug}:{collection.slug}:{variable.slug}"


#: The WMTS dimension identifiers the capabilities document may advertise
#: (#358). Named here, beside the template builder that turns them into
#: placeholders, so the document's writer imports them rather than spelling
#: them again.
WMTS_TIME_DIMENSION = "Time"
WMTS_REFTIME_DIMENSION = "Reftime"

#: The tile-route query parameter each advertised WMTS dimension drives —
#: identifier as the capabilities document spells it, parameter as the tile
#: route reads it (and as the KVP shim forwards it). One mapping, because the
#: document's ``{Time}``/``{Reftime}`` placeholders and the route's
#: ``time``/``reftime`` are two spellings of the same axis and may not drift.
WMTS_DIMENSION_PARAMS = {
    WMTS_TIME_DIMENSION: "time",
    WMTS_REFTIME_DIMENSION: "reftime",
}


def wmts_rest_tile_template(variable, dimensions=()) -> str:
    """The REST ``ResourceURL`` template a capabilities Layer advertises (#354).

    The existing per-variable tile route, spelt as a WMTS template: the
    ``{TileMatrix}``/``{TileCol}``/``{TileRow}`` placeholders are the client's
    to fill, everything around them is the grammar the rest of this module
    writes — org read from the variable's own row, ``WebMercatorQuad`` because
    it is the only TileMatrixSet the document advertises.

    ``dimensions`` names the layer's advertised dimension identifiers (#358):
    each becomes a ``{Identifier}`` placeholder on the query parameter the tile
    route reads, so a client's time slider fills ``{Time}`` and a
    dimension-ignorant client substitutes the document's defaults — either way
    landing on the same route the gate authorises. A layer with no items
    advertises no dimensions and gets a bare template: there is no honest time
    to name, and a layer without data cannot serve tiles under any spelling.
    """
    collection = variable.collection
    root = titiler_variable_root(
        org_slug_of(collection), collection.catalog.slug, collection.slug, variable.slug,
    )
    template = f"{root}/tiles/WebMercatorQuad/{{TileMatrix}}/{{TileCol}}/{{TileRow}}.png"
    query = "&".join(
        f"{WMTS_DIMENSION_PARAMS[identifier]}={{{identifier}}}"
        for identifier in dimensions
    )
    return f"{template}?{query}" if query else template


def wmts_capabilities_url(organisation) -> str:
    """The REST capabilities document on the metadata plane (#354).

    What Titiler itself fetches when proxying a KVP GetCapabilities, and what
    API tooling reads without speaking KVP. Org-in-path on a Django plane for
    the same reason ``tile_config`` is: Titiler dials it on an internal
    hostname that belongs to no organisation, so the path is the only copy of
    the tenant (ADR 0013).
    """
    return f"/api/{WMTS_KVP_SEGMENT}/{organisation.slug}/WMTSCapabilities.xml"
