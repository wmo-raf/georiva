"""The capabilities document against the WMTS 1.0.0 schema itself.

The whole point of WMTS is clients GeoRiva does not control, many of them old
and strict, so structural spot-checks are not enough: the document is validated
against OGC's own ``wmtsGetCapabilities_response.xsd``. The schema tree is
vendored under ``schemas/`` and every absolute ``schemaLocation`` resolves
there, so the test never touches the network.
"""
from datetime import datetime, timezone
from pathlib import Path
from unittest import skipUnless
from urllib.parse import urlsplit

from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Catalog, Collection, Item, Unit, Variable
from georiva.organisations.testing import dial_org, make_organisation

try:
    from lxml import etree
except ImportError:  # pragma: no cover
    etree = None

SCHEMA_ROOT = Path(__file__).parent / "schemas"
ENTRY_SCHEMA = SCHEMA_ROOT / "schemas.opengis.net/wmts/1.0/wmtsGetCapabilities_response.xsd"


@skipUnless(etree is not None, "lxml unavailable")
class WMTSSchemaValidityTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        class LocalSchemas(etree.Resolver):
            def resolve(self, url, public_id, context):
                split = urlsplit(url)
                if split.scheme in ("http", "https"):
                    local = SCHEMA_ROOT / split.netloc / split.path.lstrip("/")
                    return self.resolve_filename(str(local), context)
                return None

        parser = etree.XMLParser()
        parser.resolvers.add(LocalSchemas())
        cls.schema = etree.XMLSchema(etree.parse(str(ENTRY_SCHEMA), parser))

    def setUp(self):
        dial_org(self.client)
        self.organisation = make_organisation()
        catalog = Catalog.objects.create(
            organisation=self.organisation,
            name="Forecast", slug="forecast", file_format="geotiff",
        )
        unit = Unit.objects.create(name="Celsius", symbol="C")
        collection = Collection.objects.create(
            catalog=catalog, name="Temperature", slug="temperature",
            visibility=Collection.Visibility.PUBLIC,
            bounds=[20.0, -12.0, 52.0, 23.0],
        )
        Variable.objects.create(
            collection=collection, slug="t2m", name="2m Temperature",
            unit=unit, value_min=0, value_max=50,
        )
        Item.objects.create(
            collection=collection,
            time=datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc),
        )

    def validate(self):
        response = self.client.get(
            reverse("wmts:capabilities", args=[self.organisation.slug])
        )
        self.assertEqual(response.status_code, 200)
        document = etree.fromstring(response.content)
        self.assertTrue(
            self.schema.validate(document), self.schema.error_log,
        )

    def test_a_populated_document_is_schema_valid(self):
        self.validate()

    def test_an_empty_document_is_schema_valid_too(self):
        """No visible layers is a legitimate document, not a degenerate one —
        a fresh organisation must still paste cleanly into a client."""
        Collection.objects.update(visibility=Collection.Visibility.INTERNAL)
        self.validate()
