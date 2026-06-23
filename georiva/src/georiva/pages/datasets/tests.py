"""
Public dataset pages must not surface `internal` collections — they are
derivation intermediates, read by the engine but never served.
"""
from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Catalog, Collection


class DatasetVisibilityTests(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CMIP6", slug="cmip6", file_format="geotiff"
        )
        Collection.objects.create(
            catalog=self.catalog, name="Tas", slug="tas",
            visibility=Collection.Visibility.PUBLIC,
        )
        Collection.objects.create(
            catalog=self.catalog, name="Tas anomaly", slug="tas-anomaly",
            visibility=Collection.Visibility.INTERNAL,
        )

    def test_available_dates_404_for_internal(self):
        ok = self.client.get(
            reverse("datasets:collection-available-dates",
                    args=["cmip6", "tas"])
        )
        self.assertEqual(ok.status_code, 200)

        hidden = self.client.get(
            reverse("datasets:collection-available-dates",
                    args=["cmip6", "tas-anomaly"])
        )
        self.assertEqual(hidden.status_code, 404)
