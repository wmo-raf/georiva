import json

from django.contrib.auth import get_user_model
from django.test import TestCase

from georiva.core.models import Catalog, Collection
from georiva.ingestion.models import DataArrival

User = get_user_model()

DASHBOARD_URL = "/admin/api/ingestion/dashboard/"
ARRIVALS_URL = "/admin/api/ingestion/collections/{}/arrivals/"


def _setup_collection(catalog_slug="cat", collection_slug="col"):
    catalog = Catalog.objects.create(name=catalog_slug, slug=catalog_slug, file_format="grib2")
    return Collection.objects.create(name=collection_slug, slug=collection_slug, catalog=catalog)


class DashboardLastRunFromDataArrivalTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin", "admin@test.com", "pw")
        self.client.force_login(self.user)
        self.collection = _setup_collection()

    def test_dashboard_returns_last_run_at_from_data_arrival(self):
        arrival = DataArrival.objects.create(
            trigger=DataArrival.Trigger.MANUAL_UPLOAD,
            status=DataArrival.Status.COMPLETED,
            collection=self.collection,
        )

        response = self.client.get(DASHBOARD_URL)
        self.assertEqual(response.status_code, 200)

        data = response.json()
        col = next(c for c in data["collections"] if c["id"] == self.collection.pk)

        self.assertEqual(col["last_run_at"], arrival.started_at.isoformat())
        self.assertEqual(col["last_run_status"], DataArrival.Status.COMPLETED)

    def test_manual_and_scheduled_arrivals_return_same_response_shape(self):
        manual_col = _setup_collection("cat-m", "col-m")
        scheduled_col = _setup_collection("cat-s", "col-s")

        DataArrival.objects.create(
            trigger=DataArrival.Trigger.MANUAL_UPLOAD,
            status=DataArrival.Status.COMPLETED,
            collection=manual_col,
        )
        DataArrival.objects.create(
            trigger=DataArrival.Trigger.SCHEDULED,
            status=DataArrival.Status.COMPLETED,
            collection=scheduled_col,
        )

        response = self.client.get(DASHBOARD_URL)
        data = response.json()
        by_id = {c["id"]: c for c in data["collections"]}

        manual_entry = by_id[manual_col.pk]
        scheduled_entry = by_id[scheduled_col.pk]

        # Both have the same keys
        self.assertEqual(set(manual_entry.keys()), set(scheduled_entry.keys()))
        # Both have last_run_at populated
        self.assertIsNotNone(manual_entry["last_run_at"])
        self.assertIsNotNone(scheduled_entry["last_run_at"])


class CollectionArrivalsAPITests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin2", "a@b.com", "pw")
        self.client.force_login(self.user)
        self.collection = _setup_collection("cat2", "col2")

    def test_arrivals_endpoint_returns_data_arrival_history(self):
        DataArrival.objects.create(
            trigger=DataArrival.Trigger.SCHEDULED,
            status=DataArrival.Status.COMPLETED,
            collection=self.collection,
            files_fetched=3,
            bytes_transferred=1024,
        )

        response = self.client.get(ARRIVALS_URL.format(self.collection.pk))
        self.assertEqual(response.status_code, 200)

        data = response.json()
        self.assertIn("arrivals", data)
        self.assertEqual(len(data["arrivals"]), 1)

        entry = data["arrivals"][0]
        self.assertEqual(entry["trigger"], DataArrival.Trigger.SCHEDULED)
        self.assertEqual(entry["status"], DataArrival.Status.COMPLETED)
        self.assertEqual(entry["files_fetched"], 3)
        self.assertEqual(entry["bytes_transferred"], 1024)
