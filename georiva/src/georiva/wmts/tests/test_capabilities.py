"""The org-wide capabilities document, held to what discovery may reveal.

WMTS exists to be pasted into clients GeoRiva does not control, so everything
here asserts at the HTTP boundary — status, parsed XML, headers — following the
STAC visibility tests: the document lists exactly the collections ``visible_to``
would serve this caller, and every URL in it is the machine-plane grammar read
back verbatim.
"""
from datetime import datetime, timezone
from urllib.parse import urlsplit

from django.test import TestCase
from django.urls import reverse
from xml.etree import ElementTree as ET

from georiva.core.machine_plane import (
    MachineScope,
    scope_of,
    wmts_capabilities_url,
    wmts_rest_tile_template,
)
from georiva.core.models import Catalog, Collection, Item, Unit, Variable
from georiva.organisations.testing import dial_org, make_organisation, org_host

NS = {
    "wmts": "http://www.opengis.net/wmts/1.0",
    "ows": "http://www.opengis.net/ows/1.1",
    "xlink": "http://www.w3.org/1999/xlink",
}


class WMTSCapabilitiesTests(TestCase):
    def setUp(self):
        dial_org(self.client)
        self.organisation = make_organisation()
        self.catalog = Catalog.objects.create(
            organisation=self.organisation,
            name="Forecast", slug="forecast", file_format="geotiff",
        )
        unit = Unit.objects.create(name="Celsius", symbol="C")

        self.public = Collection.objects.create(
            catalog=self.catalog, name="Temperature", slug="temperature",
            visibility=Collection.Visibility.PUBLIC,
            bounds=[20.0, -12.0, 52.0, 23.0],
        )
        self.variable = Variable.objects.create(
            collection=self.public, slug="t2m", name="2m Temperature",
            unit=unit, value_min=0, value_max=50,
        )
        Item.objects.create(
            collection=self.public,
            time=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc),
        )
        self.latest_item = Item.objects.create(
            collection=self.public,
            time=datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc),
        )

        private = Collection.objects.create(
            catalog=self.catalog, name="Members only", slug="members-only",
            visibility=Collection.Visibility.PRIVATE,
        )
        Variable.objects.create(
            collection=private, slug="t2m", name="private t2m",
            unit=unit, value_min=0, value_max=50,
        )
        internal = Collection.objects.create(
            catalog=self.catalog, name="Intermediate", slug="intermediate",
            visibility=Collection.Visibility.INTERNAL,
        )
        Variable.objects.create(
            collection=internal, slug="t2m", name="internal t2m",
            unit=unit, value_min=0, value_max=50,
        )

    def fetch(self):
        response = self.client.get(
            reverse("wmts:capabilities", args=[self.organisation.slug])
        )
        self.assertEqual(response.status_code, 200)
        return ET.fromstring(response.content)

    def layers(self, document):
        return document.findall("wmts:Contents/wmts:Layer", NS)

    def layer_identifiers(self, document):
        return [
            layer.findtext("ows:Identifier", namespaces=NS)
            for layer in self.layers(document)
        ]

    def test_the_route_spells_what_the_machine_plane_builder_writes(self):
        self.assertEqual(
            reverse("wmts:capabilities", args=[self.organisation.slug]),
            wmts_capabilities_url(self.organisation),
        )

    def test_public_variables_appear_private_and_internal_do_not(self):
        identifiers = self.layer_identifiers(self.fetch())
        self.assertEqual(identifiers, ["forecast:temperature:t2m"])

    def test_a_private_collection_widens_nothing_for_an_anonymous_caller(self):
        """Same fixture, flipped the other way: making the public collection
        private empties the document rather than 404ing it."""
        Collection.objects.filter(pk=self.public.pk).update(
            visibility=Collection.Visibility.PRIVATE,
        )
        self.assertEqual(self.layer_identifiers(self.fetch()), [])

    def test_layers_advertise_webmercatorquad_and_png_only(self):
        document = self.fetch()
        (layer,) = self.layers(document)
        self.assertEqual(
            [f.text for f in layer.findall("wmts:Format", NS)], ["image/png"],
        )
        self.assertEqual(
            [
                link.findtext("wmts:TileMatrixSet", namespaces=NS)
                for link in layer.findall("wmts:TileMatrixSetLink", NS)
            ],
            ["WebMercatorQuad"],
        )
        matrix_sets = document.findall("wmts:Contents/wmts:TileMatrixSet", NS)
        self.assertEqual(
            [t.findtext("ows:Identifier", namespaces=NS) for t in matrix_sets],
            ["WebMercatorQuad"],
        )

    def test_the_resource_url_is_the_machine_plane_template_made_absolute(self):
        (layer,) = self.layers(self.fetch())
        resource = layer.find("wmts:ResourceURL", NS)
        self.assertEqual(resource.get("resourceType"), "tile")
        self.assertEqual(resource.get("format"), "image/png")
        self.assertEqual(
            resource.get("template"),
            f"http://{org_host()}"
            + wmts_rest_tile_template(self.variable, self.latest_item),
        )

    def test_a_filled_template_scopes_back_to_the_advertised_collection(self):
        """The templates must resolve against the routes the gateway actually
        authorises: substituting the placeholders yields a URI ``scope_of``
        reads as this collection, so a client following the document lands on
        a working tile route."""
        (layer,) = self.layers(self.fetch())
        template = layer.find("wmts:ResourceURL", NS).get("template")
        split = urlsplit(template.format(TileMatrix=6, TileCol=38, TileRow=32))
        self.assertEqual(
            scope_of(f"{split.path}?{split.query}"),
            MachineScope("test-org", "forecast", "temperature"),
        )

    def test_a_layer_without_items_still_appears_with_a_bare_template(self):
        Item.objects.all().delete()
        (layer,) = self.layers(self.fetch())
        self.assertNotIn("?", layer.find("wmts:ResourceURL", NS).get("template"))

    def test_the_document_carries_the_collections_extent(self):
        (layer,) = self.layers(self.fetch())
        bbox = layer.find("ows:WGS84BoundingBox", NS)
        self.assertEqual(bbox.findtext("ows:LowerCorner", namespaces=NS), "20.0 -12.0")
        self.assertEqual(bbox.findtext("ows:UpperCorner", namespaces=NS), "52.0 23.0")

    def test_the_document_is_served_as_xml(self):
        response = self.client.get(
            reverse("wmts:capabilities", args=[self.organisation.slug])
        )
        self.assertEqual(response["Content-Type"], "application/xml")

    def test_anothers_org_path_on_this_host_is_absent(self):
        """The host is the authority and the path may only agree with it —
        the same rule the tile-config callback enforces (#354, story 25)."""
        make_organisation("other-org")
        response = self.client.get(
            reverse("wmts:capabilities", args=["other-org"])
        )
        self.assertEqual(response.status_code, 404)

    def test_an_unknown_org_segment_is_absent_too(self):
        response = self.client.get(reverse("wmts:capabilities", args=["nowhere"]))
        self.assertEqual(response.status_code, 404)
