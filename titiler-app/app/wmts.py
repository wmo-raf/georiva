"""The KVP WMTS shim on the org-scoped endpoint (#357).

``/{org_slug}/wmts`` answers ``REQUEST=GetTile`` by translating KVP parameters
into the existing semantic tile route and dispatching the request back through
this same application — no rendering code of its own, and no catalog or tenancy
knowledge (ADR 0013/0015): the org segment is carried from path to path
unchanged, and the ``LAYER`` triple is split, never looked up. The split
mirrors ``_wmts_scope`` in Django's machine-plane addresses module, which
authorised this request against the same decoded value before nginx let it
through — the two readers must agree, or the gate authorises one collection
while this shim reads another.

``REQUEST=GetCapabilities`` is answered from the same endpoint by fetching
Django's document and handing it on unread (#362) — which layers exist and who
may see them is exactly the knowledge ADR 0013 keeps out of this service — so
one pasted URL discovers layers and then serves their tiles.

``REQUEST=GetFeatureInfo`` answers the identify click (#363): the tile
coordinate and the ``I``/``J`` pixel within it become a lon/lat, and the COG
behind the same semantic address is point-read for the raw physical value.

Errors answer OGC ExceptionReport XML (OWS 1.1), never framework JSON: the
clients on this endpoint are legacy KVP speakers that can parse nothing else.
"""
import logging
import math
import re
from typing import Optional
from xml.sax.saxutils import escape, quoteattr

import httpx
import morecantile
from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from rio_tiler.errors import PointOutsideBounds
from starlette.concurrency import run_in_threadpool

from app import dependencies

logger = logging.getLogger(__name__)

router = APIRouter()

OWS_NS = "http://www.opengis.net/ows/1.1"

#: The only grid and format the capabilities document advertises (#354);
#: anything else in a request is a client error, not a missing feature.
TILE_MATRIX_SET = "WebMercatorQuad"
TILE_FORMAT = "image/png"
MAX_ZOOM = 24

#: The one InfoFormat GetFeatureInfo answers, and the only one the
#: capabilities document offers a client (#363).
INFO_FORMAT = "application/json"

#: The grid itself, from the same library that cuts the tiles this endpoint
#: serves — so the pixel an identify click names is read off the very geometry
#: the pixel was drawn on. Reading it from a second implementation of the
#: Mercator formulae would be an identify that lands beside the pixel a client
#: sees rather than on it.
TILE_GRID = morecantile.tms.get(TILE_MATRIX_SET)

#: What the capabilities document advertises as the default style's
#: identifier, and what dimension-ignorant clients send on their own: both
#: resolve to the styleless request, whose Redis key is the alias Django keeps
#: mirroring the actual default (ADR 0023). Only an *unknown* style is a hard
#: 404 — these two spellings are the default, not a fallback to it.
DEFAULT_STYLE_ALIASES = ("", "default")

#: Every path segment this shim splices into another URL must be slug-shaped:
#: the org it was dialled on, and the three parts of ``LAYER``. Deliberately
#: looser than Django's own ``slug`` converter, which is ASCII — this service
#: is not the one deciding what exists, and a segment that gets past here
#: still has to match a row on the other side. What it must not get past is
#: anything that could re-shape a path, which would let the gate authorise
#: one address while this reads another.
SLUG_RE = re.compile(r"[\w-]+")

#: Django's REST capabilities address — the spelling
#: ``core.machine_plane.addresses.wmts_capabilities_url`` writes. The second
#: and last piece of Django URL grammar this service holds; the tile-config
#: callback in ``app.dependencies`` is the first, and both carry the
#: organisation in the path because that is where Django put it. Neither can
#: be pinned to its Django author by a test — the two services share a repo
#: and nothing else — so both are spellings to change in pairs.
CAPABILITIES_PATH = "/api/wmts/{org_slug}/WMTSCapabilities.xml"

#: The credential parameter a keyed document's own URLs carry
#: (``accounts.authentication.QUERY_PARAM``). Forwarded verbatim and never
#: read: whether it names anybody is Django's question.
API_KEY_PARAM = "api_key"


class WMTSException(Exception):
    """An error owed to the client as an OWS 1.1 ExceptionReport."""

    def __init__(self, status_code: int, code: str, locator: Optional[str], text: str):
        super().__init__(text)
        self.status_code = status_code
        self.code = code
        self.locator = locator
        self.text = text


def exception_report(exc: WMTSException) -> Response:
    """Render ``exc`` as the ExceptionReport XML legacy clients parse."""
    locator = f" locator={quoteattr(exc.locator)}" if exc.locator else ""
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<ExceptionReport xmlns="{OWS_NS}" version="1.1.0">'
        f'<Exception exceptionCode={quoteattr(exc.code)}{locator}>'
        f"<ExceptionText>{escape(exc.text)}</ExceptionText>"
        "</Exception></ExceptionReport>"
    )
    return Response(xml, status_code=exc.status_code, media_type="application/xml")


def _collect_params(request: Request) -> dict[str, str]:
    """KVP parameters with case-insensitive names, refusing repetitions.

    Parameter names are case-insensitive on this binding (OGC 06-121r3, and
    legacy clients exercise that), so ``layer`` and ``LAYER`` are one
    parameter — and two spellings of it are a repetition. Repetitions are
    refused for the same reason the auth gate refuses them: which copy wins
    would be this side's silent choice, invisible to the side that authorised.
    """
    params: dict[str, list[str]] = {}
    for name, value in request.query_params.multi_items():
        params.setdefault(name.lower(), []).append(value)
    repeated = sorted(name for name, values in params.items() if len(values) > 1)
    if repeated:
        raise WMTSException(
            400, "InvalidParameterValue", repeated[0].upper(),
            f"Parameter given more than once: {', '.join(n.upper() for n in repeated)}",
        )
    return {name: values[0] for name, values in params.items()}


def _require(params: dict[str, str], name: str) -> str:
    value = params.get(name)
    if value is None or value == "":
        raise WMTSException(
            400, "MissingParameterValue", name.upper(),
            f"Missing required parameter {name.upper()}",
        )
    return value


def _check_org_slug(org_slug: str) -> None:
    """Refuse an organisation segment that could re-shape a URL it is spliced into.

    The segment arrives in this endpoint's own path and leaves in two more —
    the tile route GetTile dispatches to, and the Django address
    GetCapabilities is fetched from — so its shape is the same question
    :func:`_parse_layer` asks of the ``LAYER`` triple, asked once at the door
    because both branches splice it. Reported as absent rather than invalid:
    no organisation is spelt this way, and which institutions exist here is
    not this caller's business.
    """
    if not SLUG_RE.fullmatch(org_slug):
        raise WMTSException(
            404, "InvalidParameterValue", None, "No WMTS service at this address",
        )


def _parse_layer(params: dict[str, str]) -> tuple[str, str, str]:
    """Split ``LAYER`` into the ``catalog:collection:variable`` triple.

    The same split ``_wmts_scope`` made when the gate authorised this request;
    the org deliberately never appears here — it lives in the path, and a
    second spelling of the tenant could disagree with the first (#354).
    """
    layer = _require(params, "layer")
    parts = layer.split(":")
    # Slug-shaped parts only: the triple is spliced into the tile route's
    # *path*, and the gate authorised the decoded LAYER value — a part that
    # could re-shape the path would let the two read different addresses.
    if len(parts) != 3 or not all(SLUG_RE.fullmatch(part) for part in parts):
        raise WMTSException(
            400, "InvalidParameterValue", "LAYER",
            f"Layer identifier must be catalog:collection:variable, got {layer!r}",
        )
    return parts[0], parts[1], parts[2]


def _parse_tile_coords(params: dict[str, str]) -> tuple[int, int, int]:
    """Validate TILEMATRIX/TILEROW/TILECOL against the WebMercatorQuad grid."""
    matrix = _require(params, "tilematrix")
    # Some clients qualify the matrix identifier with its set, e.g.
    # "WebMercatorQuad:5" — the qualified spelling names the same matrix.
    prefix = f"{TILE_MATRIX_SET}:"
    if matrix.startswith(prefix):
        matrix = matrix[len(prefix):]
    if not matrix.isdigit() or not 0 <= int(matrix) <= MAX_ZOOM:
        raise WMTSException(
            400, "InvalidParameterValue", "TILEMATRIX",
            f"TILEMATRIX must be a {TILE_MATRIX_SET} level between 0 and {MAX_ZOOM}",
        )
    zoom = int(matrix)

    coords = {}
    for name in ("tilerow", "tilecol"):
        value = _require(params, name)
        if not value.isdigit():
            raise WMTSException(
                400, "InvalidParameterValue", name.upper(),
                f"{name.upper()} must be a non-negative integer",
            )
        coords[name] = int(value)
    limit = 2 ** zoom
    for name, value in coords.items():
        if value >= limit:
            raise WMTSException(
                400, "TileOutOfRange", name.upper(),
                f"{name.upper()} {value} outside the grid at TILEMATRIX {zoom} (0..{limit - 1})",
            )
    return zoom, coords["tilerow"], coords["tilecol"]


def _validate_service_and_version(params: dict[str, str]) -> None:
    """The two parameters every operation on this endpoint shares.

    Validated only when sent: requiring them would lock out the sloppier
    legacy clients this endpoint exists for, while a *wrong* value still means
    the client wants something this is not.
    """
    service = params.get("service")
    if service is not None and service.lower() != "wmts":
        raise WMTSException(
            400, "InvalidParameterValue", "SERVICE", f"Service {service!r} not served here — this is WMTS",
        )
    version = params.get("version")
    if version is not None and version != "1.0.0":
        raise WMTSException(
            400, "InvalidParameterValue", "VERSION", f"Version {version!r} not supported — only 1.0.0",
        )


def _validate_tile_choices(params: dict[str, str]) -> None:
    """The grid and format a tile may name — exactly one honest value each.

    Asked of the two operations that address a tile: GetTile, and the
    GetFeatureInfo whose pixel is read off one. A client sending
    GetCapabilities has not yet been told which matrix set exists, so requiring
    one there would refuse every honest discovery request, and the FORMAT such
    a client sends names the document's own media type rather than a tile's.
    """
    tms = _require(params, "tilematrixset")
    if tms != TILE_MATRIX_SET:
        raise WMTSException(
            400, "InvalidParameterValue", "TILEMATRIXSET",
            f"TileMatrixSet {tms!r} not served — only {TILE_MATRIX_SET}",
        )
    fmt = params.get("format")
    if fmt is not None and fmt != TILE_FORMAT:
        raise WMTSException(
            400, "InvalidParameterValue", "FORMAT", f"Format {fmt!r} not served — only {TILE_FORMAT}",
        )


def _resolve_style(params: dict[str, str]) -> Optional[str]:
    """The style a request names, with the two default spellings resolved away.

    Read the same way by both operations that take one, so a client cannot be
    shown one rendering and told the value of another (ADR 0023).
    """
    style = params.get("style", "")
    return None if style in DEFAULT_STYLE_ALIASES else style


def _time_query(params: dict[str, str]) -> dict[str, str]:
    """The time and reftime query the semantic routes address a COG by.

    ``TIME`` is required — every asset in the archive is a moment — while
    ``REFTIME`` is sent only when the client named one, because an observation
    collection has no forecast run and asking for a blank one would address
    nothing.
    """
    query = {"time": _require(params, "time")}
    reftime = params.get("reftime")
    if reftime:
        query["reftime"] = reftime
    return query


def _translate_tile_error(status_code: int, detail: str, style: Optional[str]) -> WMTSException:
    """Re-spell the semantic routes' refusal as a WMTS exception.

    Shared by both operations, which is what keeps an address that will not
    render from identifying with a different story. The status carries through
    untouched — a nonexistent (time, reftime) combination and an unknown style
    stay hard 404s here exactly as on the REST route (ADR 0023) — only the
    vocabulary changes. The locator is read
    from the detail's own wording, which lives two files away in this same
    service; a detail we cannot place still reports honestly, just without one.
    """
    detail_lower = detail.lower()
    if status_code == 404:
        if "config" in detail_lower:
            # A config 404 does not say whether the layer or the style was the
            # stranger — that distinction is catalog knowledge this service
            # deliberately lacks — so a styled request names both suspects and
            # no locator, rather than accusing the style of the layer's crime.
            if style:
                locator, text = None, "Unknown layer, or unknown style for it"
            else:
                locator, text = "LAYER", "Unknown layer"
        elif "storage" in detail_lower or "time" in detail_lower:
            locator = "TIME"
            text = "No tile for this TIME/REFTIME combination"
        else:
            locator, text = None, detail
        return WMTSException(404, "InvalidParameterValue", locator, text)
    if status_code == 400:
        locator = "REFTIME" if "reftime" in detail_lower else "TIME" if "time" in detail_lower else None
        return WMTSException(400, "InvalidParameterValue", locator, detail)
    if status_code == 503:
        return WMTSException(503, "NoApplicableCode", None, detail)
    return WMTSException(status_code, "NoApplicableCode", None, detail or "Tile request failed")


async def _reenter(request: Request, path: str, query: dict[str, str]) -> httpx.Response:
    """Ask this same application one of its own semantic routes.

    How both KVP operations reach the data: the org segment and the ``LAYER``
    triple are joined back into the very URL the capabilities document
    advertises, and the request is dispatched through this application rather
    than reimplemented beside it — the KVP binding cannot drift from the REST
    one because one is defined as the other.

    The base URL names nothing real: nothing leaves the process, and the
    reader that answers is the same reader a browser's tile request reaches.
    """
    transport = httpx.ASGITransport(app=request.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://wmts-kvp-shim") as client:
        return await client.get(path, params=query)


def _detail_of(response: httpx.Response) -> str:
    """The ``detail`` a refused semantic route explains itself with.

    FastAPI's own error shape, and the wording :func:`_translate_tile_error`
    reads a locator out of. A body that is not that shape yields nothing
    rather than a guess.
    """
    try:
        return response.json().get("detail", "")
    except ValueError:
        return ""


async def _dispatch_get_tile(request: Request, org_slug: str, params: dict[str, str]) -> Response:
    """Answer GetTile by re-entering the existing semantic tile route."""
    _validate_service_and_version(params)
    _validate_tile_choices(params)
    catalog, collection, variable = _parse_layer(params)
    zoom, row, col = _parse_tile_coords(params)

    style = _resolve_style(params)
    query = _time_query(params)
    if style:
        query["style"] = style

    path = f"/{org_slug}/{catalog}/{collection}/{variable}/tiles/{TILE_MATRIX_SET}/{zoom}/{col}/{row}.png"
    resp = await _reenter(request, path, query)

    if resp.status_code == 200:
        return Response(resp.content, media_type=resp.headers.get("content-type", TILE_FORMAT))
    raise _translate_tile_error(resp.status_code, _detail_of(resp), style)


def _validate_info_format(params: dict[str, str]) -> None:
    """The one media type an identify answer may be asked for.

    Validated only when sent, as SERVICE and VERSION are: the capabilities
    document offers exactly one InfoFormat, so a client that omitted it can
    only have meant that one, and refusing it would lock out a sloppy legacy
    client over a parameter with no second choice. A *named* format this does
    not serve is refused, because that client asked for something it will not
    get and would otherwise be handed JSON labelled as GML.
    """
    info_format = params.get("infoformat")
    if info_format and info_format != INFO_FORMAT:
        raise WMTSException(
            400, "InvalidParameterValue", "INFOFORMAT",
            f"InfoFormat {info_format!r} not served — only {INFO_FORMAT}",
        )


def _parse_pixel(params: dict[str, str], zoom: int) -> tuple[int, int]:
    """The ``I``/``J`` pixel within the tile, held to that tile's size.

    ``PointIJOutOfRange`` is WMTS 1.0's own exception code for a pixel outside
    the tile (Table 30), and this is a request that cannot be answered rather
    than a click that found nothing: a pixel off the tile names no point on
    the map at all, while a pixel *on* it that finds no data answers 200 with
    an empty value.
    """
    matrix = TILE_GRID.matrix(zoom)
    sizes = {"i": matrix.tileWidth, "j": matrix.tileHeight}
    pixel = {}
    for name, size in sizes.items():
        value = _require(params, name)
        if not value.isdigit():
            raise WMTSException(
                400, "InvalidParameterValue", name.upper(),
                f"{name.upper()} must be a non-negative integer",
            )
        if int(value) >= size:
            raise WMTSException(
                400, "PointIJOutOfRange", name.upper(),
                f"{name.upper()} {value} outside the tile (0..{size - 1})",
            )
        pixel[name] = int(value)
    return pixel["i"], pixel["j"]


def _pixel_lonlat(zoom: int, row: int, col: int, i: int, j: int) -> tuple[float, float]:
    """The lon/lat at the centre of pixel ``(i, j)`` of tile ``(col, row, zoom)``.

    Interpolated in the grid's own projected metres and only then converted,
    because the pixel grid is linear there and not in degrees — stepping
    across a tile in latitude directly would drift further from the pixel a
    client clicked the nearer the tile sits to a pole. The centre rather than
    the corner: the value being read belongs to the whole pixel, and its
    centre is the point furthest from mistaking a neighbour for it, at every
    zoom and on either edge of the grid.
    """
    matrix = TILE_GRID.matrix(zoom)
    bounds = TILE_GRID.xy_bounds(morecantile.Tile(x=col, y=row, z=zoom))
    x = bounds.left + (i + 0.5) * (bounds.right - bounds.left) / matrix.tileWidth
    y = bounds.top - (j + 0.5) * (bounds.top - bounds.bottom) / matrix.tileHeight
    lon, lat = TILE_GRID.lnglat(x, y)
    return lon, lat


async def _tile_config_for(address: tuple[str, str, str, str], style: Optional[str]) -> dict:
    """The rendering config for this layer, resolved as a tile request resolves it.

    Fetched for two reasons, neither of them rendering. It is what makes an
    unknown layer or an unknown style say so on this operation exactly as it
    does on GetTile (ADR 0023) — a point read alone would find no COG and
    blame the TIME. And it is the only per-request metadata this service
    holds, so a ``units`` or ``label`` Django one day puts in that payload
    reaches an identify answer without a second channel being built for it
    (#363); until it does, the answer carries the value alone.

    Run off the event loop because the underlying resolution is the tile
    routes' synchronous dependency, Redis call and all — the same function, so
    an address that renders is an address that identifies.
    """
    org_slug, catalog, collection, variable = address
    try:
        return await run_in_threadpool(
            dependencies.SemanticTileConfig,
            org_slug=org_slug, catalog_slug=catalog, collection_slug=collection,
            variable_slug=variable, style=style,
        )
    except HTTPException as exc:
        raise _translate_tile_error(exc.status_code, str(exc.detail or ""), style) from exc


async def _read_point(
        request: Request, address: tuple[str, str, str, str],
        lon: float, lat: float, query: dict[str, str], style: Optional[str],
) -> Optional[float]:
    """The raw physical value under ``(lon, lat)``, or ``None`` where there is none.

    Read through the point route, so the COG address comes from the same
    semantic path translation the tile did and an identify reads the very file
    the pixel under the cursor was drawn from. That route applies neither
    colormap nor rescale, which is what makes the number returned here the
    physical one rather than the 0–255 a pixel was painted with.

    ``None`` is the answer to a click that found no data — a nodata pixel,
    which arrives already masked, or a point outside the COG's own footprint,
    which the reader refuses. Neither is an error: a forecaster clicking the
    sea beside a land-only grid has done nothing wrong, and legacy identify
    tools read an error as a broken layer (#363). A *missing* file is
    different and stays a 404, because that is a TIME/REFTIME the archive does
    not hold rather than a place the grid does not cover.
    """
    path = f"/{'/'.join(address)}/point/{lon},{lat}"
    try:
        resp = await _reenter(request, path, query)
    except PointOutsideBounds:
        return None

    if resp.status_code != 200:
        raise _translate_tile_error(resp.status_code, _detail_of(resp), style)

    # The first band, because the whole pipeline behind this address is
    # one-band-per-variable: the rescale and colormap a tile is drawn with
    # describe band 1 and nothing else, so a second band would be a value no
    # rendering on the instance has ever shown.
    values = resp.json().get("values") or [None]
    value = values[0]
    # A masked pixel arrives as None already; a NaN one does not, and would
    # leave this endpoint as a token no strict JSON parser accepts. Both mean
    # the same thing to the person who clicked.
    if value is None or not math.isfinite(value):
        return None
    return value


async def _dispatch_get_feature_info(
        request: Request, org_slug: str, params: dict[str, str],
) -> Response:
    """Answer GetFeatureInfo with the value under the clicked pixel (#363).

    The answer echoes what was asked — layer, time, reftime and the lon/lat
    the tile coordinate and pixel resolved to — beside the value, so a
    forecaster can see that the number belongs to the point they clicked and
    the run they picked. Times are echoed as the client spelt them: this is a
    receipt for a request, not a restatement of the catalog.

    A click that found nothing keeps that whole shape and carries a null
    value. The client asked a well-formed question about a real place and the
    honest answer is "nothing here" — an exception would tell it the layer is
    broken.
    """
    _validate_service_and_version(params)
    _validate_tile_choices(params)
    _validate_info_format(params)
    catalog, collection, variable = _parse_layer(params)
    zoom, row, col = _parse_tile_coords(params)
    i, j = _parse_pixel(params, zoom)

    style = _resolve_style(params)
    query = _time_query(params)
    address = (org_slug, catalog, collection, variable)

    lon, lat = _pixel_lonlat(zoom, row, col, i, j)
    config = await _tile_config_for(address, style)
    value = await _read_point(request, address, lon, lat, query, style)

    payload = {
        "layer": params["layer"],
        "time": query["time"],
        "reftime": query.get("reftime"),
        "longitude": lon,
        "latitude": lat,
        "value": value,
    }
    # Only what the payload already holds (#363): this service invents no
    # metadata, and an identify answer is not worth a channel of its own.
    for name in ("units", "label"):
        if config.get(name):
            payload[name] = config[name]
    return JSONResponse(payload, media_type=INFO_FORMAT)


def _translate_capabilities_error(status_code: int) -> WMTSException:
    """Re-spell Django's refusal of the document as a WMTS exception.

    The 404 and the 401 carry through, because each is an answer to the client
    rather than a failure of this service. 404 is what Django gives for an
    organisation it does not serve, or one whose name disagrees with the host
    that was dialled — reported as absent there, and reported as absent here.
    401 is a credential Django refused, which is the one thing the caller can
    actually fix and so the one worth a locator. Anything else is Django
    misbehaving rather than the client, and this says so as the gateway it is
    being on this request.

    Behind the nginx gateway most of these are settled before this service is
    reached: the ``auth_request`` subrequest asks Django the same questions
    first, so an unknown organisation is denied there and a broken key comes
    back as nginx's own bare 401 rather than as a report — a gap on this
    whole endpoint, GetTile included, and one to close in the gateway rather
    than here (#372). These branches are what a direct caller gets,
    and what stays right if the gate's answers and Django's ever part.
    """
    if status_code == 404:
        return WMTSException(
            404, "InvalidParameterValue", None, "No WMTS service for this organisation",
        )
    if status_code in (401, 403):
        return WMTSException(
            status_code, "InvalidParameterValue", API_KEY_PARAM.upper(),
            "The presented credential was refused",
        )
    return WMTSException(
        502, "NoApplicableCode", None,
        f"The capabilities document could not be built (upstream status {status_code})",
    )


async def _proxy_get_capabilities(request: Request, org_slug: str, params: dict[str, str]) -> Response:
    """Answer GetCapabilities with Django's document, fetched and not read.

    Django owns the document because it is pure metadata (ADR 0013), and this
    endpoint answers with it so that discovery and tiles share the one URL a
    legacy client is given. Proxied rather than redirected: those clients are
    flaky on redirects, and this is one request per session.

    Three things travel with the fetch, and nothing else does. The
    organisation, in the path, spelt as ``wmts_capabilities_url`` writes it.
    The Host the client dialled, because Django resolves the organisation from
    it and makes every URL in the document absolute against that
    organisation's own site — fetched on a bare internal container name, the
    document would come back addressed to whichever site Django falls back to.
    And the caller's credential — the ``api_key`` parameter or an
    ``Authorization`` header — so a keyed request gets the private-inclusive,
    key-propagated document (#360); which credentials are worth anything, and
    which transport wins when both are presented, stays Django's decision.

    Not a session cookie, which is the third transport Django's own planes
    accept. Nothing on this endpoint is reached by a browser holding one — its
    clients are desktop GIS carrying a key — and forwarding a cookie would
    mean forwarding a ``Set-Cookie`` back through a proxy that has no session
    of its own to keep. A signed-in browser therefore gets the anonymous
    document here, and the REST capabilities URL is where its cookie counts.

    What comes back is handed on unparsed, unrewritten and uncached. This
    service holds no catalog knowledge to check it against, and a copy of a
    document that lists layers and their times would be a second, staler
    answer to a question Django already caches its own answer to (#361).
    """
    query = {}
    api_key = params.get(API_KEY_PARAM)
    if api_key:
        query[API_KEY_PARAM] = api_key
    forwarded = {
        name: value
        for name, value in (
            ("host", request.headers.get("host")),
            ("authorization", request.headers.get("authorization")),
        )
        if value
    }

    try:
        async with dependencies.django_client() as client:
            response = await client.get(
                CAPABILITIES_PATH.format(org_slug=org_slug),
                params=query, headers=forwarded,
            )
    except httpx.HTTPError as e:
        logger.warning("WMTS capabilities fetch failed for %s: %s", org_slug, e)
        raise WMTSException(
            503, "NoApplicableCode", None,
            "The capabilities document is unavailable — the metadata service could not be reached",
        )

    if response.status_code != 200:
        raise _translate_capabilities_error(response.status_code)

    headers = {}
    cache_control = response.headers.get("cache-control")
    if cache_control:
        # Django's own directive, carried rather than restated: a keyed
        # document is marked private there, and dropping that would leave a
        # caller's private layer listing cacheable by everything between the
        # two services. Nothing is added when Django sends nothing — deciding
        # how long its document keeps is not this side's decision to take.
        headers["Cache-Control"] = cache_control
    return Response(
        response.content,
        media_type=response.headers.get("content-type", "application/xml"),
        headers=headers,
    )


@router.get("/{org_slug}/wmts")
async def wmts_kvp(org_slug: str, request: Request) -> Response:
    """The org-scoped KVP endpoint, ``/titiler/{org}/wmts`` behind the proxy.

    The org arrives as a path segment and leaves as one — forwarded into the
    tile route it dispatches to, never resolved (ADR 0013). nginx already
    asked Django whether this exact request line may address this exact
    collection before it got here (ADR 0015).
    """
    try:
        _check_org_slug(org_slug)
        params = _collect_params(request)
        operation = _require(params, "request").lower()
        if operation == "gettile":
            return await _dispatch_get_tile(request, org_slug, params)
        if operation == "getcapabilities":
            _validate_service_and_version(params)
            return await _proxy_get_capabilities(request, org_slug, params)
        if operation == "getfeatureinfo":
            return await _dispatch_get_feature_info(request, org_slug, params)
        raise WMTSException(
            400, "InvalidParameterValue", "REQUEST",
            f"Unknown request {params['request']!r}",
        )
    except WMTSException as exc:
        if exc.status_code >= 500:
            logger.warning("WMTS KVP %s: %s", exc.code, exc.text)
        return exception_report(exc)
