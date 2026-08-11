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

Errors answer OGC ExceptionReport XML (OWS 1.1), never framework JSON: the
clients on this endpoint are legacy KVP speakers that can parse nothing else.
GetCapabilities (proxied to Django, #362) and GetFeatureInfo (#363) are later
slices; until then they answer an honest OperationNotSupported.
"""
import logging
import re
from typing import Optional
from xml.sax.saxutils import escape, quoteattr

import httpx
from fastapi import APIRouter, Request, Response

logger = logging.getLogger(__name__)

router = APIRouter()

OWS_NS = "http://www.opengis.net/ows/1.1"

#: The only grid and format the capabilities document advertises (#354);
#: anything else in a request is a client error, not a missing feature.
TILE_MATRIX_SET = "WebMercatorQuad"
TILE_FORMAT = "image/png"
MAX_ZOOM = 24

#: What the capabilities document advertises as the default style's
#: identifier, and what dimension-ignorant clients send on their own: both
#: resolve to the styleless request, whose Redis key is the alias Django keeps
#: mirroring the actual default (ADR 0023). Only an *unknown* style is a hard
#: 404 — these two spellings are the default, not a fallback to it.
DEFAULT_STYLE_ALIASES = ("", "default")


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
    if len(parts) != 3 or not all(re.fullmatch(r"[\w-]+", part) for part in parts):
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


def _validate_fixed_choices(params: dict[str, str]) -> None:
    """The parameters with exactly one honest value on this service.

    SERVICE and VERSION are validated only when sent: requiring them would
    lock out the sloppier legacy clients this endpoint exists for, while a
    *wrong* value still means the client wants something this is not.
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


def _translate_tile_error(status_code: int, detail: str, style: Optional[str]) -> WMTSException:
    """Re-spell the semantic tile route's refusal as a WMTS exception.

    The status carries through untouched — a nonexistent (time, reftime)
    combination and an unknown style stay hard 404s here exactly as on the
    REST route (ADR 0023) — only the vocabulary changes. The locator is read
    from the detail's own wording, which lives two files away in this same
    service; a detail we cannot place still reports honestly, just without one.
    """
    detail_lower = detail.lower()
    if status_code == 404:
        if "config" in detail_lower:
            locator = "STYLE" if style else "LAYER"
            text = "Unknown style for this layer" if style else "Unknown layer"
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


async def _dispatch_get_tile(request: Request, org_slug: str, params: dict[str, str]) -> Response:
    """Answer GetTile by re-entering the existing semantic tile route.

    The KVP triple and the org segment are joined back into the very URL the
    capabilities document's REST templates advertise, and the request is
    dispatched through this same ASGI application — the two bindings cannot
    drift apart because one is defined as the other.
    """
    _validate_fixed_choices(params)
    catalog, collection, variable = _parse_layer(params)
    zoom, row, col = _parse_tile_coords(params)

    style: Optional[str] = params.get("style", "")
    if style in DEFAULT_STYLE_ALIASES:
        style = None

    query = {"time": _require(params, "time")}
    reftime = params.get("reftime")
    if reftime:
        query["reftime"] = reftime
    if style:
        query["style"] = style

    path = f"/{org_slug}/{catalog}/{collection}/{variable}/tiles/{TILE_MATRIX_SET}/{zoom}/{col}/{row}.png"
    transport = httpx.ASGITransport(app=request.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://wmts-kvp-shim") as client:
        resp = await client.get(path, params=query)

    if resp.status_code == 200:
        return Response(resp.content, media_type=resp.headers.get("content-type", TILE_FORMAT))

    try:
        detail = resp.json().get("detail", "")
    except ValueError:
        detail = ""
    raise _translate_tile_error(resp.status_code, detail, style)


@router.get("/{org_slug}/wmts")
async def wmts_kvp(org_slug: str, request: Request) -> Response:
    """The org-scoped KVP endpoint, ``/titiler/{org}/wmts`` behind the proxy.

    The org arrives as a path segment and leaves as one — forwarded into the
    tile route it dispatches to, never resolved (ADR 0013). nginx already
    asked Django whether this exact request line may address this exact
    collection before it got here (ADR 0015).
    """
    try:
        params = _collect_params(request)
        operation = _require(params, "request").lower()
        if operation == "gettile":
            return await _dispatch_get_tile(request, org_slug, params)
        if operation in ("getcapabilities", "getfeatureinfo"):
            raise WMTSException(
                501, "OperationNotSupported", "REQUEST",
                f"{params['request']} is not implemented yet on this endpoint",
            )
        raise WMTSException(
            400, "InvalidParameterValue", "REQUEST",
            f"Unknown request {params['request']!r}",
        )
    except WMTSException as exc:
        if exc.status_code >= 500:
            logger.warning("WMTS KVP %s: %s", exc.code, exc.text)
        return exception_report(exc)
