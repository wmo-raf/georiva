"""
EDR serving must expose only `public` collections — `internal` derivation
intermediates are read by the engine but never served.
"""
from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Catalog, Collection


class EDRVisibilityTests(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CMIP6", slug="cmip6", file_format="geotiff"
        )
        self.public = Collection.objects.create(
            catalog=self.catalog, name="Tas", slug="tas",
            visibility=Collection.Visibility.PUBLIC,
        )
        self.internal = Collection.objects.create(
            catalog=self.catalog, name="Tas anomaly", slug="tas-anomaly",
            visibility=Collection.Visibility.INTERNAL,
        )

    def test_collection_list_excludes_internal(self):
        response = self.client.get(reverse("edr:collection-list"))
        self.assertEqual(response.status_code, 200)

        ids = {c["id"] for c in response.json()["collections"]}
        self.assertIn("tas", ids)
        self.assertNotIn("tas-anomaly", ids)

    def test_collection_detail_404_for_internal(self):
        ok = self.client.get(
            reverse("edr:collection-detail", args=["tas"])
        )
        self.assertEqual(ok.status_code, 200)

        hidden = self.client.get(
            reverse("edr:collection-detail", args=["tas-anomaly"])
        )
        self.assertEqual(hidden.status_code, 404)
