from datetime import timedelta

from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.utils import timezone

from georiva.core.models import Catalog, Collection
from georiva.core.storage import BucketType
from georiva.ingestion.models import DataArrival, FileIngestion, FileIngestionJob
from georiva.sources.models import DataFeed, DataFeedCollectionLink

User = get_user_model()

DASHBOARD_URL = "/admin/api/ingestion/dashboard/"
ARRIVALS_URL = "/admin/api/ingestion/collections/{}/arrivals/"
LOGS_URL = "/admin/api/ingestion/collections/{}/ingestion-logs/"
JOBS_URL = "/admin/api/ingestion/collections/{}/ingestion-jobs/"


def _setup_collection(catalog_slug="cat", collection_slug="col"):
    catalog = Catalog.objects.create(name=catalog_slug, slug=catalog_slug, file_format="grib2")
    return Collection.objects.create(name=collection_slug, slug=collection_slug, catalog=catalog)


def _make_arrival(catalog, trigger=DataArrival.Trigger.MANUAL_UPLOAD,
                  status=DataArrival.Status.COMPLETED, **kwargs):
    return DataArrival.objects.create(trigger=trigger, status=status, catalog=catalog, **kwargs)


def _make_file_ingestion(collection, file_path=None, status=FileIngestion.Status.COMPLETED,
                         **kwargs):
    arrival = _make_arrival(collection.catalog)
    fi = FileIngestion.objects.create(
        bucket=BucketType.SOURCES,
        file_path=file_path or f"{collection.catalog.slug}/{collection.slug}/file.grib2",
        status=status,
        data_arrival=arrival,
        **kwargs,
    )
    fi.collections.add(collection)
    return fi


class DashboardLastRunFromDataArrivalTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin", "admin@test.com", "pw")
        self.client.force_login(self.user)
        self.collection = _setup_collection()

    def test_dashboard_returns_last_run_at_from_data_arrival(self):
        arrival = _make_arrival(
            self.collection.catalog,
            status=DataArrival.Status.COMPLETED,
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

        _make_arrival(manual_col.catalog, trigger=DataArrival.Trigger.MANUAL_UPLOAD,
                      status=DataArrival.Status.COMPLETED)
        _make_arrival(scheduled_col.catalog, trigger=DataArrival.Trigger.SCHEDULED,
                      status=DataArrival.Status.COMPLETED)

        response = self.client.get(DASHBOARD_URL)
        data = response.json()
        by_id = {c["id"]: c for c in data["collections"]}

        manual_entry = by_id[manual_col.pk]
        scheduled_entry = by_id[scheduled_col.pk]

        self.assertEqual(set(manual_entry.keys()), set(scheduled_entry.keys()))
        self.assertIsNotNone(manual_entry["last_run_at"])
        self.assertIsNotNone(scheduled_entry["last_run_at"])

    def test_collection_with_no_arrivals_has_null_last_run_fields(self):
        response = self.client.get(DASHBOARD_URL)
        data = response.json()
        col = next(c for c in data["collections"] if c["id"] == self.collection.pk)

        self.assertIsNone(col["last_run_at"])
        self.assertIsNone(col["last_run_status"])

    def test_inactive_collection_excluded_from_dashboard(self):
        inactive = _setup_collection("cat-off", "col-off")
        inactive.is_active = False
        inactive.save()

        response = self.client.get(DASHBOARD_URL)
        ids = [c["id"] for c in response.json()["collections"]]
        self.assertNotIn(inactive.pk, ids)

    def test_type_field_is_automated_when_data_feed_link_exists(self):
        automated_col = _setup_collection("cat-auto", "col-auto")
        feed = DataFeed.objects.create(name="Test Feed")
        DataFeedCollectionLink.objects.create(data_feed=feed, collection=automated_col)

        response = self.client.get(DASHBOARD_URL)
        by_id = {c["id"]: c for c in response.json()["collections"]}

        self.assertEqual(by_id[automated_col.pk]["type"], "automated")
        self.assertEqual(by_id[self.collection.pk]["type"], "manual")

    def test_derived_status_is_empty_when_no_file_ingestion_logs(self):
        response = self.client.get(DASHBOARD_URL)
        col = next(c for c in response.json()["collections"] if c["id"] == self.collection.pk)
        self.assertEqual(col["status"], "empty")

    def test_derived_status_is_ok_when_completed_file_ingestion_linked_via_m2m(self):
        _make_file_ingestion(self.collection, status=FileIngestion.Status.COMPLETED)

        response = self.client.get(DASHBOARD_URL)
        col = next(c for c in response.json()["collections"] if c["id"] == self.collection.pk)
        self.assertEqual(col["status"], "ok")

    def test_derived_status_is_failed_when_failed_file_ingestion_linked_via_m2m(self):
        _make_file_ingestion(self.collection, status=FileIngestion.Status.FAILED)

        response = self.client.get(DASHBOARD_URL)
        col = next(c for c in response.json()["collections"] if c["id"] == self.collection.pk)
        self.assertEqual(col["status"], "failed")

    def test_grib_collection_shows_success_sparkline_via_m2m(self):
        _make_file_ingestion(self.collection, status=FileIngestion.Status.COMPLETED)

        response = self.client.get(DASHBOARD_URL)
        col = next(c for c in response.json()["collections"] if c["id"] == self.collection.pk)
        today_entry = col["sparkline"][-1]
        self.assertEqual(today_entry["status"], "success")

    def test_grib_collection_shows_failed_sparkline_when_no_items_created(self):
        _make_file_ingestion(self.collection, status=FileIngestion.Status.FAILED)

        response = self.client.get(DASHBOARD_URL)
        col = next(c for c in response.json()["collections"] if c["id"] == self.collection.pk)
        today_entry = col["sparkline"][-1]
        self.assertEqual(today_entry["status"], "failed")

    def test_multi_collection_grib_independent_sparkline_statuses(self):
        """A single GRIB file serving two collections shows each collection's own status."""
        catalog = Catalog.objects.create(name="shared-cat", slug="shared-cat", file_format="grib2")
        col_a = Collection.objects.create(name="col-a", slug="col-a", catalog=catalog)
        col_b = Collection.objects.create(name="col-b", slug="col-b", catalog=catalog)

        arrival = _make_arrival(catalog)

        fi_a = FileIngestion.objects.create(
            bucket=BucketType.SOURCES,
            file_path="shared-cat/col-a/file.grib2",
            status=FileIngestion.Status.COMPLETED,
            data_arrival=arrival,
        )
        fi_a.collections.add(col_a)

        fi_b = FileIngestion.objects.create(
            bucket=BucketType.SOURCES,
            file_path="shared-cat/col-b/file.grib2",
            status=FileIngestion.Status.FAILED,
            data_arrival=arrival,
        )
        fi_b.collections.add(col_b)

        response = self.client.get(DASHBOARD_URL)
        by_id = {c["id"]: c for c in response.json()["collections"]}

        self.assertEqual(by_id[col_a.pk]["sparkline"][-1]["status"], "success")
        self.assertEqual(by_id[col_b.pk]["sparkline"][-1]["status"], "failed")

    def test_last_run_at_comes_from_most_recent_catalog_arrival(self):
        """last_run_at is the most recent DataArrival for the collection's catalog."""
        older = _make_arrival(
            self.collection.catalog,
            status=DataArrival.Status.COMPLETED,
            started_at=timezone.now() - timedelta(days=2),
        )
        newer = _make_arrival(
            self.collection.catalog,
            status=DataArrival.Status.FAILED,
        )

        response = self.client.get(DASHBOARD_URL)
        col = next(c for c in response.json()["collections"] if c["id"] == self.collection.pk)

        self.assertEqual(col["last_run_at"], newer.started_at.isoformat())
        self.assertEqual(col["last_run_status"], DataArrival.Status.FAILED)


class CollectionArrivalsAPITests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin2", "a@b.com", "pw")
        self.client.force_login(self.user)
        self.collection = _setup_collection("cat2", "col2")

    def test_arrivals_endpoint_returns_data_arrival_history(self):
        _make_arrival(
            self.collection.catalog,
            trigger=DataArrival.Trigger.SCHEDULED,
            status=DataArrival.Status.COMPLETED,
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

    def test_arrivals_returns_404_for_unknown_collection(self):
        response = self.client.get(ARRIVALS_URL.format(99999))
        self.assertEqual(response.status_code, 404)

    def test_arrivals_returns_empty_list_when_no_arrivals(self):
        response = self.client.get(ARRIVALS_URL.format(self.collection.pk))
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["arrivals"], [])

    def test_arrivals_duration_seconds_computed_from_started_and_finished_at(self):
        started = timezone.now()
        finished = started + timedelta(seconds=90)
        _make_arrival(
            self.collection.catalog,
            trigger=DataArrival.Trigger.SCHEDULED,
            status=DataArrival.Status.COMPLETED,
            started_at=started,
            finished_at=finished,
        )

        response = self.client.get(ARRIVALS_URL.format(self.collection.pk))
        entry = response.json()["arrivals"][0]
        self.assertEqual(entry["duration_seconds"], 90.0)

    def test_arrivals_duration_seconds_is_null_when_not_finished(self):
        _make_arrival(
            self.collection.catalog,
            trigger=DataArrival.Trigger.MANUAL_UPLOAD,
            status=DataArrival.Status.PROCESSING,
        )

        response = self.client.get(ARRIVALS_URL.format(self.collection.pk))
        entry = response.json()["arrivals"][0]
        self.assertIsNone(entry["duration_seconds"])

    def test_arrivals_from_different_catalog_not_included(self):
        other_collection = _setup_collection("other-cat", "other-col")
        _make_arrival(other_collection.catalog)

        response = self.client.get(ARRIVALS_URL.format(self.collection.pk))
        data = response.json()
        self.assertEqual(data["arrivals"], [])


class CollectionIngestionLogsAPITests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin3", "c@d.com", "pw")
        self.client.force_login(self.user)
        self.collection = _setup_collection("cat3", "col3")

    def test_ingestion_logs_returns_file_ingestion_history(self):
        _make_file_ingestion(
            self.collection,
            status=FileIngestion.Status.COMPLETED,
            items_created=2,
            assets_created=4,
        )

        response = self.client.get(LOGS_URL.format(self.collection.pk))
        self.assertEqual(response.status_code, 200)

        data = response.json()
        self.assertIn("logs", data)
        self.assertEqual(len(data["logs"]), 1)

        entry = data["logs"][0]
        self.assertEqual(entry["status"], FileIngestion.Status.COMPLETED)
        self.assertEqual(entry["items_created"], 2)
        self.assertEqual(entry["assets_created"], 4)

    def test_ingestion_logs_returns_empty_list_when_no_m2m_link(self):
        # FileIngestion exists but is not linked to this collection
        arrival = _make_arrival(self.collection.catalog)
        FileIngestion.objects.create(
            bucket=BucketType.SOURCES,
            file_path="cat3/col3/unlinked.grib2",
            data_arrival=arrival,
        )

        response = self.client.get(LOGS_URL.format(self.collection.pk))
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["logs"], [])

    def test_ingestion_logs_returns_404_for_unknown_collection(self):
        response = self.client.get(LOGS_URL.format(99999))
        self.assertEqual(response.status_code, 404)

    def test_ingestion_logs_for_grib_collection_linked_via_m2m(self):
        """GRIB produces items for multiple collections; each collection only sees its own logs."""
        catalog = Catalog.objects.create(name="grib-cat", slug="grib-cat", file_format="grib2")
        col_x = Collection.objects.create(name="col-x", slug="col-x", catalog=catalog)
        col_y = Collection.objects.create(name="col-y", slug="col-y", catalog=catalog)

        arrival = _make_arrival(catalog)
        fi = FileIngestion.objects.create(
            bucket=BucketType.SOURCES,
            file_path="grib-cat/multi.grib2",
            status=FileIngestion.Status.COMPLETED,
            data_arrival=arrival,
        )
        fi.collections.set([col_x, col_y])

        resp_x = self.client.get(LOGS_URL.format(col_x.pk))
        resp_y = self.client.get(LOGS_URL.format(col_y.pk))

        self.assertEqual(len(resp_x.json()["logs"]), 1)
        self.assertEqual(len(resp_y.json()["logs"]), 1)


def _make_job(fi, **kwargs):
    ct = ContentType.objects.get_for_model(FileIngestionJob)
    return FileIngestionJob.objects.create(
        content_type=ct,
        file_path=fi.file_path,
        bucket=fi.bucket,
        file_ingestion=fi,
        **kwargs,
    )


class CollectionIngestionJobsAPITests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin4", "e@f.com", "pw")
        self.client.force_login(self.user)
        self.collection = _setup_collection("cat4", "col4")

    def test_ingestion_jobs_returns_job_history(self):
        fi = _make_file_ingestion(self.collection, items_created=1, assets_created=2)
        _make_job(fi)

        response = self.client.get(JOBS_URL.format(self.collection.pk))
        self.assertEqual(response.status_code, 200)

        data = response.json()
        self.assertIn("jobs", data)
        self.assertEqual(len(data["jobs"]), 1)

        entry = data["jobs"][0]
        self.assertEqual(entry["file_path"], fi.file_path)
        self.assertEqual(entry["bucket"], fi.bucket)
        self.assertEqual(entry["items_created"], 1)
        self.assertEqual(entry["assets_created"], 2)

    def test_has_active_true_when_job_is_pending(self):
        fi = _make_file_ingestion(self.collection,
                                   file_path="cat4/col4/pending.grib2")
        _make_job(fi)

        response = self.client.get(JOBS_URL.format(self.collection.pk))
        data = response.json()
        self.assertTrue(data["has_active"])

    def test_has_active_false_when_all_jobs_finished(self):
        fi = _make_file_ingestion(self.collection, file_path="cat4/col4/done.grib2")
        _make_job(fi, state="finished")

        response = self.client.get(JOBS_URL.format(self.collection.pk))
        data = response.json()
        self.assertFalse(data["has_active"])

    def test_ingestion_jobs_returns_empty_list_when_no_jobs(self):
        response = self.client.get(JOBS_URL.format(self.collection.pk))
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["jobs"], [])
        self.assertFalse(data["has_active"])

    def test_ingestion_jobs_returns_404_for_unknown_collection(self):
        response = self.client.get(JOBS_URL.format(99999))
        self.assertEqual(response.status_code, 404)
