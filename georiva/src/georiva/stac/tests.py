"""
STAC serving must expose only `public` collections — `internal` derivation
intermediates are read by the engine but never served.
"""
from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Catalog, Collection, Unit, Variable
from georiva.organisations.testing import dial_org, make_organisation


class STACVisibilityTests(TestCase):
    def setUp(self):
        # The API roots are per organisation and resolved from the Host, so the
        # client has to dial the org that owns these fixtures.
        dial_org(self.client)
        self.catalog = Catalog.objects.create(organisation=make_organisation(), 
            name="CMIP6", slug="cmip6", file_format="geotiff"
        )
        self.unit = Unit.objects.create(name="Celsius", symbol="C")

        self.public = Collection.objects.create(
            catalog=self.catalog, name="Tas", slug="tas",
            visibility=Collection.Visibility.PUBLIC,
        )
        Variable.objects.create(
            collection=self.public, slug="tas", name="tas",
            unit=self.unit, value_min=0, value_max=50,
        )

        self.internal = Collection.objects.create(
            catalog=self.catalog, name="Tas anomaly", slug="tas-anomaly",
            visibility=Collection.Visibility.INTERNAL,
        )
        Variable.objects.create(
            collection=self.internal, slug="tas", name="tas anomaly",
            unit=self.unit, value_min=-25, value_max=25,
        )

    def test_collection_list_excludes_internal(self):
        response = self.client.get(
            reverse("stac:collection-list", args=["cmip6"])
        )
        self.assertEqual(response.status_code, 200)

        ids = {c["id"] for c in response.json()["collections"]}
        self.assertIn("tas/tas", ids)
        self.assertNotIn("tas-anomaly/tas", ids)

    def test_collection_detail_404_for_internal(self):
        ok = self.client.get(
            reverse("stac:collection-detail", args=["cmip6", "tas", "tas"])
        )
        self.assertEqual(ok.status_code, 200)

        hidden = self.client.get(
            reverse(
                "stac:collection-detail",
                args=["cmip6", "tas-anomaly", "tas"],
            )
        )
        self.assertEqual(hidden.status_code, 404)
