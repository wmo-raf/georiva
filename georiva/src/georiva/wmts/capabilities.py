"""The org-wide WMTS capabilities document (#354, slice #356).

One document per organisation, listing every variable the requester may see as
a WMTS layer — the discovery surface a legacy GIS client pastes once and reads
everything through. Django owns it because it is pure metadata: which layers
exist and who may see them is exactly the knowledge ADR 0013/0015 keep out of
the tile servers, and the tiles themselves stay Titiler's.

Layer identifiers are ``catalog:collection:variable``; the organisation stays
in the URL path, never inside the identifier, because the document is already
per-organisation and repeating the tenant in every layer would let the two
spellings disagree. Every URL in the document comes from the machine-plane
builders (ADR 0013) — this module only makes them absolute against the host
the request dialled.

The document advertises ``WebMercatorQuad`` as the only TileMatrixSet,
``image/png`` as the only format, ``application/json`` as the only InfoFormat
an identify may ask for (#363), per-layer ``Time``/``Reftime`` dimensions
enumerated from the organisation's Items (#358), and each variable's named
styles with the real default marked (#359). A credentialed request widens the
listing through the same ``visible_to``, and a key that travelled as
``?api_key=`` is written into every advertised URL (#360) — so a legacy
client, which can fill placeholders but cannot append parameters, reaches its
private layers from one paste.

Two bindings are advertised, and a client reads whichever it speaks: modern
clients follow the per-layer REST ``ResourceURL`` templates, while a KVP-only
client reads ``OperationsMetadata`` for the org-scoped endpoint under the
Titiler prefix — the same address it fetched this document from, since that
endpoint proxies GetCapabilities straight back here (#362). Each layer carries
both a ``tile`` and a ``FeatureInfo`` template (#379), so neither binding is
shown an operation it has no address for.
"""
from xml.etree import ElementTree as ET

from georiva.accounts.authentication import query_presented_secret
from georiva.core.machine_plane import (
    WMTS_REFTIME_DIMENSION,
    WMTS_TIME_DIMENSION,
    wmts_capabilities_url,
    wmts_kvp_endpoint,
    wmts_layer_identifier,
    wmts_rest_featureinfo_template,
    wmts_rest_tile_template,
)
from georiva.core.models import Collection, Item, Variable
from georiva.core.utils import get_full_url_by_request, iso_utc_z
from georiva.organisations.access import scoped_queryset

WMTS_NS = "http://www.opengis.net/wmts/1.0"
OWS_NS = "http://www.opengis.net/ows/1.1"
XLINK_NS = "http://www.w3.org/1999/xlink"

ET.register_namespace("", WMTS_NS)
ET.register_namespace("ows", OWS_NS)
ET.register_namespace("xlink", XLINK_NS)

TILE_FORMAT = "image/png"
TILE_MATRIX_SET = "WebMercatorQuad"

#: The one media type an identify answer comes back as (#363), matching
#: ``INFO_FORMAT`` in the KVP shim that answers it. Advertising a second would
#: promise a rendering that endpoint refuses.
INFO_FORMAT = "application/json"

#: WebMercatorQuad as OGC defines it: 256px tiles from one world-covering tile
#: at level 0, halving in scale per level. 0–24 matches the morecantile
#: definition Titiler serves, so every TileMatrix advertised here is answerable.
TOP_LEFT_CORNER = "-20037508.3427892 20037508.3427892"
SCALE_DENOMINATOR_0 = 559082264.0287178
MAX_ZOOM = 24

#: The operations the KVP endpoint answers, in the order the document lists
#: them (#362, #363). All three are on the one org-scoped address, so a client
#: that pasted a single URL discovers layers, draws them and clicks them.
KVP_OPERATIONS = ("GetCapabilities", "GetTile", "GetFeatureInfo")


def _ows(tag):
    return f"{{{OWS_NS}}}{tag}"


def _wmts(tag):
    return f"{{{WMTS_NS}}}{tag}"


def visible_variables(request):
    """The variables ``request`` may discover, in a stable reading order.

    Visibility is the collection manager's ``visible_to`` and nothing looser
    (ADR 0014): anonymous callers see ``public`` collections only, a member of
    the dialled organisation — by session, header or ``?api_key=`` — sees its
    ``private`` collections too, and ``internal`` goes nowhere.
    ``scoped_queryset`` then pins the rows to the dialled organisation — the
    same double filter the STAC views apply.
    """
    return scoped_queryset(
        request,
        Variable.objects.filter(
            is_active=True,
            collection__catalog__is_active=True,
            collection__in=Collection.objects.visible_to(request),
        ),
    ).select_related("collection__catalog").prefetch_related("styles").order_by(
        "collection__catalog__slug", "collection__slug", "slug",
    )


def build_capabilities(request, organisation) -> bytes:
    """The WMTSCapabilities.xml body for ``organisation``, as ``request`` may see it."""
    # Only a query-carried key is written into the advertised URLs (#360) —
    # the accounts module owns which transports qualify. An *invalid* key
    # never reaches here at all: authentication already answered 401.
    api_key = query_presented_secret(request)
    root = ET.Element(_wmts("Capabilities"), {"version": "1.0.0"})

    service = ET.SubElement(root, _ows("ServiceIdentification"))
    ET.SubElement(service, _ows("Title")).text = f"{organisation.name} WMTS"
    ET.SubElement(service, _ows("ServiceType")).text = "OGC WMTS"
    ET.SubElement(service, _ows("ServiceTypeVersion")).text = "1.0.0"

    _append_operations_metadata(root, request, organisation, api_key)

    contents = ET.SubElement(root, _wmts("Contents"))
    dimensions_by_collection = {}
    for variable in visible_variables(request):
        collection = variable.collection
        if collection.pk not in dimensions_by_collection:
            dimensions_by_collection[collection.pk] = layer_dimensions(collection)
        _append_layer(
            contents, request, variable,
            dimensions_by_collection[collection.pk], api_key,
        )
    _append_tile_matrix_set(contents)

    ET.SubElement(root, _wmts("ServiceMetadataURL"), {
        f"{{{XLINK_NS}}}href": get_full_url_by_request(
            request, wmts_capabilities_url(organisation, api_key=api_key),
        ),
    })

    return ET.tostring(root, encoding="UTF-8", xml_declaration=True)


def kvp_dcp_href(request, organisation, api_key=None) -> str:
    """The KVP endpoint as an OWS DCP href — the prefix a client appends to.

    OWS 1.1 has clients concatenate their KVP parameters onto this href rather
    than parse it (the WMTS examples spell it ``…/maps.cgi?``), so it carries
    its own separator: ``?`` on a bare address, ``&`` after a keyed one, whose
    credential is already in the query (#360). The address itself still comes
    from the machine-plane builder (ADR 0013); only the trailing character is
    this document's, because it belongs to the encoding rather than the URL.
    """
    url = get_full_url_by_request(
        request, wmts_kvp_endpoint(organisation, api_key=api_key),
    )
    return f"{url}&" if "?" in url else f"{url}?"


def _append_operations_metadata(root, request, organisation, api_key=None):
    """Where a KVP-only client sends everything it asks for next (#362).

    The REST binding rides the per-layer ``ResourceURL`` templates, which a
    modern client reads; a KVP client learns its one address from here, and
    without this section it would discover layers it has no way to fetch. Both
    operations point at the same org-scoped endpoint under the Titiler prefix —
    the single paste-able URL, which proxies GetCapabilities back to this very
    view — and each declares KVP, because that endpoint answers no other
    encoding.
    """
    href = kvp_dcp_href(request, organisation, api_key)
    metadata = ET.SubElement(root, _ows("OperationsMetadata"))
    for name in KVP_OPERATIONS:
        operation = ET.SubElement(metadata, _ows("Operation"), {"name": name})
        http = ET.SubElement(ET.SubElement(operation, _ows("DCP")), _ows("HTTP"))
        get = ET.SubElement(http, _ows("Get"), {f"{{{XLINK_NS}}}href": href})
        constraint = ET.SubElement(get, _ows("Constraint"), {"name": "GetEncoding"})
        allowed = ET.SubElement(constraint, _ows("AllowedValues"))
        ET.SubElement(allowed, _ows("Value")).text = "KVP"


def layer_dimensions(collection):
    """The WMTS dimensions a layer over ``collection`` advertises (#358).

    Identifier → ``(values, default)``, in document order. A collection whose
    items carry a ``reference_time`` is a forecast and advertises two axes:
    ``Reftime`` lists every run newest-first and defaults to the newest, and
    ``Time`` lists that default run's valid times — so a dimension-ignorant
    client substituting defaults gets coherent latest-forecast tiles, never a
    time from one run against another run's reference. ``Time`` defaults to
    the run's first valid time (the analysis — the run's "now"), while an
    observation collection advertises ``Time`` alone, defaulting to the newest
    value it has. Enumeration is complete on purpose — the accepted
    document-size trade-off of #354 — and a collection with no items yet
    advertises nothing: there are no honest values to list.
    """
    reftimes = list(
        Item.objects.filter(collection=collection, reference_time__isnull=False)
        .order_by("-reference_time")
        .values_list("reference_time", flat=True)
        .distinct()
    )
    if reftimes:
        times = list(
            Item.objects.filter(collection=collection, reference_time=reftimes[0])
            .order_by("time")
            .values_list("time", flat=True)
        )
        return {
            WMTS_TIME_DIMENSION: (times, times[0]),
            WMTS_REFTIME_DIMENSION: (reftimes, reftimes[0]),
        }
    times = list(
        Item.objects.filter(collection=collection)
        .order_by("time")
        .values_list("time", flat=True)
    )
    if not times:
        return {}
    return {WMTS_TIME_DIMENSION: (times, times[-1])}


def _append_layer(contents, request, variable, dimensions, api_key=None):
    collection = variable.collection
    layer = ET.SubElement(contents, _wmts("Layer"))
    ET.SubElement(layer, _ows("Title")).text = f"{collection.name} — {variable.name}"

    extent = collection.spatial_extent
    if extent:
        bbox = ET.SubElement(layer, _ows("WGS84BoundingBox"))
        ET.SubElement(bbox, _ows("LowerCorner")).text = f"{extent[0]} {extent[1]}"
        ET.SubElement(bbox, _ows("UpperCorner")).text = f"{extent[2]} {extent[3]}"

    ET.SubElement(layer, _ows("Identifier")).text = wmts_layer_identifier(variable)

    # Named styles (#359): identifiers are the style slugs the tile route's
    # ``?style=`` reads — which styles exist stays Django's knowledge (ADR
    # 0023) — with the actual default marked. Prefetched rows, and the model's
    # ordering already puts the default first. The database forbids a second
    # default but not a missing one; then nothing is marked — ``isDefault``
    # defaults to false in the schema, and marking a style the styleless route
    # would not render is exactly the silent lie ADR 0023 rules out. A
    # variable with no styles at all keeps a placeholder entry instead: the
    # document needs a default, and the styleless tile route already renders
    # the variable's default.
    styles = list(variable.styles.all())
    for named in styles:
        attributes = {"isDefault": "true"} if named.is_default else {}
        style = ET.SubElement(layer, _wmts("Style"), attributes)
        ET.SubElement(style, _ows("Title")).text = named.name
        ET.SubElement(style, _ows("Identifier")).text = named.slug
    if not styles:
        style = ET.SubElement(layer, _wmts("Style"), {"isDefault": "true"})
        ET.SubElement(style, _ows("Identifier")).text = "default"

    ET.SubElement(layer, _wmts("Format")).text = TILE_FORMAT

    # What an identify may ask for (#363). A layer that lists no InfoFormat is
    # a layer whose clients hide their identify tool, so this is what makes
    # the operation reachable at all; listing only what the KVP endpoint
    # actually answers is what keeps the offer honest.
    ET.SubElement(layer, _wmts("InfoFormat")).text = INFO_FORMAT

    for identifier, (values, default) in dimensions.items():
        dimension = ET.SubElement(layer, _wmts("Dimension"))
        ET.SubElement(dimension, _ows("Identifier")).text = identifier
        ET.SubElement(dimension, _wmts("Default")).text = iso_utc_z(default)
        for value in values:
            ET.SubElement(dimension, _wmts("Value")).text = iso_utc_z(value)

    link = ET.SubElement(layer, _wmts("TileMatrixSetLink"))
    ET.SubElement(link, _wmts("TileMatrixSet")).text = TILE_MATRIX_SET

    ET.SubElement(layer, _wmts("ResourceURL"), {
        "format": TILE_FORMAT,
        "resourceType": "tile",
        "template": get_full_url_by_request(
            request,
            wmts_rest_tile_template(
                variable, dimensions, styled=bool(styles), api_key=api_key,
            ),
        ),
    })

    # The identify address, for the client that reads these templates and never
    # speaks KVP (#379). Without it the InfoFormat above is an offer with no
    # address behind it on this binding: a REST client would show its identify
    # tool and have nowhere to send the click. The same dimensions and style as
    # the tile template, because the pixel being explained belongs to the tile
    # drawn from the line above.
    ET.SubElement(layer, _wmts("ResourceURL"), {
        "format": INFO_FORMAT,
        "resourceType": "FeatureInfo",
        "template": get_full_url_by_request(
            request,
            wmts_rest_featureinfo_template(
                variable, dimensions, styled=bool(styles), api_key=api_key,
            ),
        ),
    })


def _append_tile_matrix_set(contents):
    tms = ET.SubElement(contents, _wmts("TileMatrixSet"))
    ET.SubElement(tms, _ows("Identifier")).text = TILE_MATRIX_SET
    ET.SubElement(tms, _ows("SupportedCRS")).text = "urn:ogc:def:crs:EPSG::3857"
    ET.SubElement(tms, _wmts("WellKnownScaleSet")).text = (
        "urn:ogc:def:wkss:OGC:1.0:GoogleMapsCompatible"
    )
    for zoom in range(MAX_ZOOM + 1):
        matrix = ET.SubElement(tms, _wmts("TileMatrix"))
        ET.SubElement(matrix, _ows("Identifier")).text = str(zoom)
        ET.SubElement(matrix, _wmts("ScaleDenominator")).text = repr(
            SCALE_DENOMINATOR_0 / 2 ** zoom
        )
        ET.SubElement(matrix, _wmts("TopLeftCorner")).text = TOP_LEFT_CORNER
        ET.SubElement(matrix, _wmts("TileWidth")).text = "256"
        ET.SubElement(matrix, _wmts("TileHeight")).text = "256"
        ET.SubElement(matrix, _wmts("MatrixWidth")).text = str(2 ** zoom)
        ET.SubElement(matrix, _wmts("MatrixHeight")).text = str(2 ** zoom)
