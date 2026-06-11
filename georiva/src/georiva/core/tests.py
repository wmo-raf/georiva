from datetime import datetime, timezone

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Catalog, Collection, Item
from georiva.ingestion.models import DataArrival, FileIngestion

User = get_user_model()


def _setup():
    catalog = Catalog.objects.create(name="Models", slug="models", file_format="grib2")
    collection = Collection.objects.create(catalog=catalog, name="Surface", slug="surface")
    return catalog, collection


def _make_arrival(catalog):
    return DataArrival.objects.create(
        trigger=DataArrival.Trigger.MANUAL_UPLOAD,
        catalog=catalog,
    )


def _make_item(collection, source_file, t=None):
    if t is None:
        t = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return Item.objects.create(collection=collection, time=t, source_file=source_file)


def _make_fi(bucket, file_path, status, arrival, error=""):
    fi = FileIngestion.objects.create(
        bucket=bucket,
        file_path=file_path,
        status=status,
        data_arrival=arrival,
        error=error,
    )
    return fi


class CollectionItemsIngestionBadgeTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin_ci", "ci@test.com", "pw")
        self.client.force_login(self.user)
        self.catalog, self.collection = _setup()
        self.url = reverse("collection_items_list", args=[self.collection.pk])

    def test_completed_ingestion_shows_completed_badge(self):
        arrival = _make_arrival(self.catalog)
        _make_item(self.collection, "mybucket:models/surface/file.grib")
        _make_fi("mybucket", "models/surface/file.grib", FileIngestion.Status.COMPLETED, arrival)

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "w-status-tag--primary")

    def test_failed_ingestion_shows_failed_badge_with_error(self):
        arrival = _make_arrival(self.catalog)
        _make_item(self.collection, "mybucket:models/surface/failed.grib")
        _make_fi(
            "mybucket", "models/surface/failed.grib",
            FileIngestion.Status.FAILED, arrival, error="Decoding error",
        )

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "w-status-tag--critical")
        self.assertContains(response, "Decoding error")

    def test_item_with_no_ingestion_shows_dash(self):
        _make_item(self.collection, "")

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "w-text-grey-400")

    def test_multiple_items_from_same_source_file_all_show_status(self):
        arrival = _make_arrival(self.catalog)
        _make_item(self.collection, "mybucket:models/surface/multi.grib", t=datetime(2024, 1, 1, tzinfo=timezone.utc))
        _make_item(self.collection, "mybucket:models/surface/multi.grib", t=datetime(2024, 1, 2, tzinfo=timezone.utc))
        _make_fi("mybucket", "models/surface/multi.grib", FileIngestion.Status.COMPLETED, arrival)

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content.decode().count("w-status-tag--primary ci-log-tag"), 2)
