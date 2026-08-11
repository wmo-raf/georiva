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

This slice advertises the skeleton: ``WebMercatorQuad`` as the only
TileMatrixSet, ``image/png`` as the only format, a placeholder default style,
and a REST ``ResourceURL`` per layer pinned to the newest item's time. Time and
run dimensions, named styles, and credentialed (private-layer) documents arrive
in later slices of #354.
"""
from xml.etree import ElementTree as ET

from georiva.core.machine_plane import wmts_capabilities_url, wmts_rest_tile_template
from georiva.core.models import Collection, Item, Variable
from georiva.core.utils import get_full_url_by_request
from georiva.organisations.access import scoped_queryset

WMTS_NS = "http://www.opengis.net/wmts/1.0"
OWS_NS = "http://www.opengis.net/ows/1.1"
XLINK_NS = "http://www.w3.org/1999/xlink"

ET.register_namespace("", WMTS_NS)
ET.register_namespace("ows", OWS_NS)
ET.register_namespace("xlink", XLINK_NS)

TILE_FORMAT = "image/png"
TILE_MATRIX_SET = "WebMercatorQuad"

#: WebMercatorQuad as OGC defines it: 256px tiles from one world-covering tile
#: at level 0, halving in scale per level. 0–24 matches the morecantile
#: definition Titiler serves, so every TileMatrix advertised here is answerable.
TOP_LEFT_CORNER = "-20037508.3427892 20037508.3427892"
SCALE_DENOMINATOR_0 = 559082264.0287178
MAX_ZOOM = 24


def _ows(tag):
    return f"{{{OWS_NS}}}{tag}"


def _wmts(tag):
    return f"{{{WMTS_NS}}}{tag}"


def visible_variables(request):
    """The variables ``request`` may discover, in a stable reading order.

    Visibility is the collection manager's ``visible_to`` and nothing looser
    (ADR 0014): anonymous callers see ``public`` collections only, and this
    slice serves no other kind of caller. ``scoped_queryset`` then pins the
    rows to the dialled organisation — the same double filter the STAC views
    apply.
    """
    return scoped_queryset(
        request,
        Variable.objects.filter(
            is_active=True,
            collection__catalog__is_active=True,
            collection__in=Collection.objects.visible_to(request),
        ),
    ).select_related("collection__catalog").order_by(
        "collection__catalog__slug", "collection__slug", "slug",
    )


def layer_identifier(variable) -> str:
    """``catalog:collection:variable`` — the grammar ``scope_of`` reads back
    out of a KVP ``LAYER`` parameter (#355)."""
    collection = variable.collection
    return f"{collection.catalog.slug}:{collection.slug}:{variable.slug}"


def build_capabilities(request, organisation) -> bytes:
    """The WMTSCapabilities.xml body for ``organisation``, as ``request`` may see it."""
    root = ET.Element(_wmts("Capabilities"), {"version": "1.0.0"})

    service = ET.SubElement(root, _ows("ServiceIdentification"))
    ET.SubElement(service, _ows("Title")).text = f"{organisation.name} WMTS"
    ET.SubElement(service, _ows("ServiceType")).text = "OGC WMTS"
    ET.SubElement(service, _ows("ServiceTypeVersion")).text = "1.0.0"

    contents = ET.SubElement(root, _wmts("Contents"))
    latest_by_collection = {}
    for variable in visible_variables(request):
        collection = variable.collection
        if collection.pk not in latest_by_collection:
            latest_by_collection[collection.pk] = Item.objects.latest(collection)
        _append_layer(
            contents, request, variable, latest_by_collection[collection.pk],
        )
    _append_tile_matrix_set(contents)

    ET.SubElement(root, _wmts("ServiceMetadataURL"), {
        f"{{{XLINK_NS}}}href": get_full_url_by_request(
            request, wmts_capabilities_url(organisation),
        ),
    })

    return ET.tostring(root, encoding="UTF-8", xml_declaration=True)


def _append_layer(contents, request, variable, latest_item):
    collection = variable.collection
    layer = ET.SubElement(contents, _wmts("Layer"))
    ET.SubElement(layer, _ows("Title")).text = f"{collection.name} — {variable.name}"

    extent = collection.spatial_extent
    if extent:
        bbox = ET.SubElement(layer, _ows("WGS84BoundingBox"))
        ET.SubElement(bbox, _ows("LowerCorner")).text = f"{extent[0]} {extent[1]}"
        ET.SubElement(bbox, _ows("UpperCorner")).text = f"{extent[2]} {extent[3]}"

    ET.SubElement(layer, _ows("Identifier")).text = layer_identifier(variable)

    # One placeholder style until #354's styles slice: the schema requires a
    # default, and the tile route already renders the variable's real default
    # when no style is named.
    style = ET.SubElement(layer, _wmts("Style"), {"isDefault": "true"})
    ET.SubElement(style, _ows("Identifier")).text = "default"

    ET.SubElement(layer, _wmts("Format")).text = TILE_FORMAT

    link = ET.SubElement(layer, _wmts("TileMatrixSetLink"))
    ET.SubElement(link, _wmts("TileMatrixSet")).text = TILE_MATRIX_SET

    ET.SubElement(layer, _wmts("ResourceURL"), {
        "format": TILE_FORMAT,
        "resourceType": "tile",
        "template": get_full_url_by_request(
            request, wmts_rest_tile_template(variable, latest_item),
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
