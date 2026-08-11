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
``image/png`` as the only format, per-layer ``Time``/``Reftime`` dimensions
enumerated from the organisation's Items (#358), and each variable's named
styles with the real default marked (#359). Credentialed (private-layer)
documents arrive in later slices of #354.
"""
from xml.etree import ElementTree as ET

from georiva.core.machine_plane import (
    WMTS_REFTIME_DIMENSION,
    WMTS_TIME_DIMENSION,
    wmts_capabilities_url,
    wmts_layer_identifier,
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
    ).select_related("collection__catalog").prefetch_related("styles").order_by(
        "collection__catalog__slug", "collection__slug", "slug",
    )


def build_capabilities(request, organisation) -> bytes:
    """The WMTSCapabilities.xml body for ``organisation``, as ``request`` may see it."""
    root = ET.Element(_wmts("Capabilities"), {"version": "1.0.0"})

    service = ET.SubElement(root, _ows("ServiceIdentification"))
    ET.SubElement(service, _ows("Title")).text = f"{organisation.name} WMTS"
    ET.SubElement(service, _ows("ServiceType")).text = "OGC WMTS"
    ET.SubElement(service, _ows("ServiceTypeVersion")).text = "1.0.0"

    contents = ET.SubElement(root, _wmts("Contents"))
    dimensions_by_collection = {}
    for variable in visible_variables(request):
        collection = variable.collection
        if collection.pk not in dimensions_by_collection:
            dimensions_by_collection[collection.pk] = layer_dimensions(collection)
        _append_layer(
            contents, request, variable, dimensions_by_collection[collection.pk],
        )
    _append_tile_matrix_set(contents)

    ET.SubElement(root, _wmts("ServiceMetadataURL"), {
        f"{{{XLINK_NS}}}href": get_full_url_by_request(
            request, wmts_capabilities_url(organisation),
        ),
    })

    return ET.tostring(root, encoding="UTF-8", xml_declaration=True)


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


def _append_layer(contents, request, variable, dimensions):
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
    # ordering already puts the default first. A variable with no styles keeps
    # a placeholder entry: the schema requires a default, and the styleless
    # tile route already renders the variable's default.
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
            wmts_rest_tile_template(variable, dimensions, styled=bool(styles)),
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
