from datetime import timedelta

from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from django.utils import timezone

from georiva.core.models import Catalog, Collection
from georiva.core.storage import BucketType
from georiva.ingestion.models import FileIngestion, FileIngestionJob
from georiva.sources.models import DataFeed, DataFeedCollectionLink

User = get_user_model()

DASHBOARD_URL = "/admin/api/ingestion/dashboard/"
LOGS_URL = "/admin/api/ingestion/collections/{}/ingestion-logs/"
JOBS_URL = "/admin/api/ingestion/collections/{}/ingestion-jobs/"
FETCH_RUNS_URL = "/admin/api/ingestion/collections/{}/fetch-runs/"
UPLOAD_SESSIONS_URL = "/admin/api/ingestion/collections/{}/upload-sessions/"


def _setup_collection(catalog_slug="cat", collection_slug="col"):
    catalog = Catalog.objects.create(name=catalog_slug, slug=catalog_slug, file_format="grib2")
    return Collection.objects.create(name=collection_slug, slug=collection_slug, catalog=catalog)


def _make_file_ingestion(collection, file_path=None, status=FileIngestion.Status.COMPLETED,
                         **kwargs):
    fi = FileIngestion.objects.create(
        bucket=BucketType.SOURCES,
        file_path=file_path or f"{collection.catalog.slug}/{collection.slug}/file.grib2",
        status=status,
        **kwargs,
    )
    fi.collections.add(collection)
    return fi


class DashboardCatalogGroupedTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin_cg", "cg@test.com", "pw")
        self.client.force_login(self.user)
        self.catalog = Catalog.objects.create(name="CHIRPS", slug="chirps", file_format="grib2")
        self.collection = Collection.objects.create(name="Daily", slug="daily", catalog=self.catalog)

    def test_dashboard_returns_catalogs_key(self):
        response = self.client.get(DASHBOARD_URL)
        self.assertIn("catalogs", response.json())

    def test_collections_nested_under_parent_catalog(self):
        response = self.client.get(DASHBOARD_URL)
        catalogs = response.json()["catalogs"]
        cat = next(c for c in catalogs if c["id"] == self.catalog.pk)
        col_ids = [c["id"] for c in cat["collections"]]
        self.assertIn(self.collection.pk, col_ids)

    def test_catalog_entry_has_required_fields(self):
        response = self.client.get(DASHBOARD_URL)
        cat = next(c for c in response.json()["catalogs"] if c["id"] == self.catalog.pk)
        for field in ("id", "slug", "name", "status", "summary", "collections"):
            self.assertIn(field, cat)

    def test_catalog_status_is_failed_when_any_child_is_failed(self):
        _make_file_ingestion(self.collection, status=FileIngestion.Status.FAILED)
        col2 = Collection.objects.create(name="Monthly", slug="monthly", catalog=self.catalog)
        _make_file_ingestion(col2, status=FileIngestion.Status.COMPLETED)

        response = self.client.get(DASHBOARD_URL)
        cat = next(c for c in response.json()["catalogs"] if c["id"] == self.catalog.pk)
        self.assertEqual(cat["status"], "failed")

    def test_catalog_status_is_empty_when_all_children_are_empty(self):
        response = self.client.get(DASHBOARD_URL)
        cat = next(c for c in response.json()["catalogs"] if c["id"] == self.catalog.pk)
        self.assertEqual(cat["status"], "empty")

    def test_catalog_status_is_ok_when_no_child_is_failed(self):
        _make_file_ingestion(self.collection, status=FileIngestion.Status.COMPLETED)

        response = self.client.get(DASHBOARD_URL)
        cat = next(c for c in response.json()["catalogs"] if c["id"] == self.catalog.pk)
        self.assertEqual(cat["status"], "ok")

    def test_catalog_summary_counts_are_accurate(self):
        col2 = Collection.objects.create(name="Monthly", slug="monthly", catalog=self.catalog)
        col3 = Collection.objects.create(name="Weekly", slug="weekly", catalog=self.catalog)
        _make_file_ingestion(self.collection, status=FileIngestion.Status.COMPLETED)
        _make_file_ingestion(col2, status=FileIngestion.Status.FAILED)
        # col3 has no FileIngestion → empty

        response = self.client.get(DASHBOARD_URL)
        cat = next(c for c in response.json()["catalogs"] if c["id"] == self.catalog.pk)
        self.assertEqual(cat["summary"]["ok"], 1)
        self.assertEqual(cat["summary"]["failed"], 1)
        self.assertEqual(cat["summary"]["empty"], 1)

    def test_catalog_with_no_active_collections_excluded(self):
        empty_catalog = Catalog.objects.create(name="Empty", slug="empty-cat", file_format="grib2")
        col = Collection.objects.create(name="col", slug="col-e", catalog=empty_catalog)
        col.is_active = False
        col.save()

        response = self.client.get(DASHBOARD_URL)
        catalog_ids = [c["id"] for c in response.json()["catalogs"]]
        self.assertNotIn(empty_catalog.pk, catalog_ids)

    def test_per_collection_fields_preserved_in_nested_entry(self):
        response = self.client.get(DASHBOARD_URL)
        cat = next(c for c in response.json()["catalogs"] if c["id"] == self.catalog.pk)
        col = next(c for c in cat["collections"] if c["id"] == self.collection.pk)
        for field in ("id", "slug", "name", "type", "status", "sparkline", "last_run_at", "item_count"):
            self.assertIn(field, col)


def _find_collection_in_response(data, collection_pk):
    for cat in data["catalogs"]:
        for col in cat["collections"]:
            if col["id"] == collection_pk:
                return col
    return None


def _all_collections_in_response(data):
    return [col for cat in data["catalogs"] for col in cat["collections"]]


class DashboardCollectionListTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin", "admin@test.com", "pw")
        self.client.force_login(self.user)
        self.collection = _setup_collection()

    def test_collection_with_no_file_ingestions_has_null_last_run_fields(self):
        response = self.client.get(DASHBOARD_URL)
        col = _find_collection_in_response(response.json(), self.collection.pk)

        self.assertIsNone(col["last_run_at"])
        self.assertIsNone(col["last_run_status"])

    def test_last_run_at_comes_from_most_recent_file_ingestion(self):
        older = _make_file_ingestion(
            self.collection, file_path="cat/col/old.grib2",
            status=FileIngestion.Status.COMPLETED,
        )
        newer = _make_file_ingestion(
            self.collection, file_path="cat/col/new.grib2",
            status=FileIngestion.Status.FAILED,
        )
        newer.created_at = older.created_at + timedelta(seconds=10)
        newer.save(update_fields=["created_at"])

        response = self.client.get(DASHBOARD_URL)
        col = _find_collection_in_response(response.json(), self.collection.pk)

        self.assertEqual(col["last_run_status"], FileIngestion.Status.FAILED)

    def test_inactive_collection_excluded_from_dashboard(self):
        inactive = _setup_collection("cat-off", "col-off")
        inactive.is_active = False
        inactive.save()

        response = self.client.get(DASHBOARD_URL)
        ids = [c["id"] for c in _all_collections_in_response(response.json())]
        self.assertNotIn(inactive.pk, ids)

    def test_type_field_is_automated_when_data_feed_link_exists(self):
        automated_col = _setup_collection("cat-auto", "col-auto")
        feed = DataFeed.objects.create(name="Test Feed")
        DataFeedCollectionLink.objects.create(data_feed=feed, collection=automated_col)

        response = self.client.get(DASHBOARD_URL)
        by_id = {c["id"]: c for c in _all_collections_in_response(response.json())}

        self.assertEqual(by_id[automated_col.pk]["type"], "automated")
        self.assertEqual(by_id[self.collection.pk]["type"], "manual")

    def test_derived_status_is_empty_when_no_file_ingestion_logs(self):
        response = self.client.get(DASHBOARD_URL)
        col = _find_collection_in_response(response.json(), self.collection.pk)
        self.assertEqual(col["status"], "empty")

    def test_derived_status_is_ok_when_completed_file_ingestion_linked_via_m2m(self):
        _make_file_ingestion(self.collection, status=FileIngestion.Status.COMPLETED)

        response = self.client.get(DASHBOARD_URL)
        col = _find_collection_in_response(response.json(), self.collection.pk)
        self.assertEqual(col["status"], "ok")

    def test_derived_status_is_failed_when_failed_file_ingestion_linked_via_m2m(self):
        _make_file_ingestion(self.collection, status=FileIngestion.Status.FAILED)

        response = self.client.get(DASHBOARD_URL)
        col = _find_collection_in_response(response.json(), self.collection.pk)
        self.assertEqual(col["status"], "failed")

    def test_grib_collection_shows_success_sparkline_via_m2m(self):
        _make_file_ingestion(self.collection, status=FileIngestion.Status.COMPLETED)

        response = self.client.get(DASHBOARD_URL)
        col = _find_collection_in_response(response.json(), self.collection.pk)
        self.assertEqual(col["sparkline"][-1]["status"], "success")

    def test_grib_collection_shows_failed_sparkline_when_no_items_created(self):
        _make_file_ingestion(self.collection, status=FileIngestion.Status.FAILED)

        response = self.client.get(DASHBOARD_URL)
        col = _find_collection_in_response(response.json(), self.collection.pk)
        self.assertEqual(col["sparkline"][-1]["status"], "failed")

    def test_multi_collection_grib_independent_sparkline_statuses(self):
        catalog = Catalog.objects.create(name="shared-cat", slug="shared-cat", file_format="grib2")
        col_a = Collection.objects.create(name="col-a", slug="col-a", catalog=catalog)
        col_b = Collection.objects.create(name="col-b", slug="col-b", catalog=catalog)

        fi_a = FileIngestion.objects.create(
            bucket=BucketType.SOURCES,
            file_path="shared-cat/col-a/file.grib2",
            status=FileIngestion.Status.COMPLETED,
        )
        fi_a.collections.add(col_a)

        fi_b = FileIngestion.objects.create(
            bucket=BucketType.SOURCES,
            file_path="shared-cat/col-b/file.grib2",
            status=FileIngestion.Status.FAILED,
        )
        fi_b.collections.add(col_b)

        response = self.client.get(DASHBOARD_URL)
        by_id = {c["id"]: c for c in _all_collections_in_response(response.json())}

        self.assertEqual(by_id[col_a.pk]["sparkline"][-1]["status"], "success")
        self.assertEqual(by_id[col_b.pk]["sparkline"][-1]["status"], "failed")


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
        FileIngestion.objects.create(
            bucket=BucketType.SOURCES,
            file_path="cat3/col3/unlinked.grib2",
        )

        response = self.client.get(LOGS_URL.format(self.collection.pk))
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["logs"], [])

    def test_ingestion_logs_returns_404_for_unknown_collection(self):
        response = self.client.get(LOGS_URL.format(99999))
        self.assertEqual(response.status_code, 404)

    def test_failed_records_appear_before_non_failed_records(self):
        # The completed record is created AFTER the failed one — so plain
        # most-recent-first ordering would put it first. Pinning must override this.
        fi_fail = _make_file_ingestion(
            self.collection, file_path="cat3/col3/fail.grib2",
            status=FileIngestion.Status.FAILED,
        )
        fi_ok = _make_file_ingestion(
            self.collection, file_path="cat3/col3/ok.grib2",
            status=FileIngestion.Status.COMPLETED,
        )
        fi_ok.created_at = fi_fail.created_at + timedelta(seconds=5)
        fi_ok.save(update_fields=["created_at"])

        response = self.client.get(LOGS_URL.format(self.collection.pk))
        logs = response.json()["logs"]
        statuses = [l["status"] for l in logs]
        failed_indices = [i for i, s in enumerate(statuses) if s == "failed"]
        non_failed_indices = [i for i, s in enumerate(statuses) if s != "failed"]
        self.assertTrue(all(f < nf for f in failed_indices for nf in non_failed_indices))

    def test_within_failed_group_most_recent_first(self):
        fi1 = _make_file_ingestion(
            self.collection, file_path="cat3/col3/fail1.grib2",
            status=FileIngestion.Status.FAILED,
        )
        fi2 = _make_file_ingestion(
            self.collection, file_path="cat3/col3/fail2.grib2",
            status=FileIngestion.Status.FAILED,
        )
        fi2.created_at = fi1.created_at + timedelta(seconds=10)
        fi2.save(update_fields=["created_at"])

        response = self.client.get(LOGS_URL.format(self.collection.pk))
        logs = response.json()["logs"]
        self.assertEqual(logs[0]["id"], fi2.pk)
        self.assertEqual(logs[1]["id"], fi1.pk)

    def test_ingestion_logs_for_grib_collection_linked_via_m2m(self):
        catalog = Catalog.objects.create(name="grib-cat", slug="grib-cat", file_format="grib2")
        col_x = Collection.objects.create(name="col-x", slug="col-x", catalog=catalog)
        col_y = Collection.objects.create(name="col-y", slug="col-y", catalog=catalog)

        fi = FileIngestion.objects.create(
            bucket=BucketType.SOURCES,
            file_path="grib-cat/multi.grib2",
            status=FileIngestion.Status.COMPLETED,
        )
        fi.collections.set([col_x, col_y])

        resp_x = self.client.get(LOGS_URL.format(col_x.pk))
        resp_y = self.client.get(LOGS_URL.format(col_y.pk))

        self.assertEqual(len(resp_x.json()["logs"]), 1)
        self.assertEqual(len(resp_y.json()["logs"]), 1)


class CollectionFetchRunsAPITests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin5", "g@h.com", "pw")
        self.client.force_login(self.user)
        catalog = Catalog.objects.create(name="cat5", slug="cat5", file_format="grib2")
        self.collection = Collection.objects.create(name="col5", slug="col5", catalog=catalog)
        self.feed = DataFeed.objects.create(name="Test Feed")
        DataFeedCollectionLink.objects.create(data_feed=self.feed, collection=self.collection)

    def test_fetch_runs_returns_200(self):
        response = self.client.get(FETCH_RUNS_URL.format(self.collection.pk))
        self.assertEqual(response.status_code, 200)

    def test_fetch_runs_returns_fetch_runs_key(self):
        response = self.client.get(FETCH_RUNS_URL.format(self.collection.pk))
        self.assertIn("fetch_runs", response.json())

    def test_fetch_runs_includes_runs_for_collection_feed(self):
        from georiva.sources.models import FetchRun
        run = FetchRun.objects.create(data_feed=self.feed)

        response = self.client.get(FETCH_RUNS_URL.format(self.collection.pk))
        data = response.json()
        self.assertEqual(len(data["fetch_runs"]), 1)
        entry = data["fetch_runs"][0]
        self.assertEqual(entry["id"], run.pk)
        self.assertEqual(entry["status"], "running")
        self.assertIn("started_at", entry)
        self.assertIn("files_fetched", entry)
        self.assertIn("files_skipped", entry)
        self.assertIn("files_failed", entry)
        self.assertIn("bytes_transferred", entry)
        self.assertEqual(entry["data_feed_name"], self.feed.name)

    def test_fetch_runs_excludes_runs_from_unrelated_feeds(self):
        from georiva.sources.models import FetchRun
        other_feed = DataFeed.objects.create(name="Other Feed")
        FetchRun.objects.create(data_feed=other_feed)

        response = self.client.get(FETCH_RUNS_URL.format(self.collection.pk))
        self.assertEqual(len(response.json()["fetch_runs"]), 0)

    def test_fetch_runs_returns_404_for_unknown_collection(self):
        response = self.client.get(FETCH_RUNS_URL.format(99999))
        self.assertEqual(response.status_code, 404)

    def test_fetch_runs_duration_seconds_set_when_run_finished(self):
        from django.utils import timezone
        from georiva.sources.models import FetchRun
        run = FetchRun.objects.create(data_feed=self.feed)
        run.finished_at = run.started_at + timedelta(seconds=42)
        run.save(update_fields=["finished_at"])

        response = self.client.get(FETCH_RUNS_URL.format(self.collection.pk))
        entry = response.json()["fetch_runs"][0]
        self.assertAlmostEqual(entry["duration_seconds"], 42, delta=1)

    def test_fetch_runs_duration_seconds_null_when_run_not_finished(self):
        from georiva.sources.models import FetchRun
        FetchRun.objects.create(data_feed=self.feed)

        response = self.client.get(FETCH_RUNS_URL.format(self.collection.pk))
        entry = response.json()["fetch_runs"][0]
        self.assertIsNone(entry["duration_seconds"])

    def test_fetch_runs_does_not_include_run_time_field(self):
        from georiva.sources.models import FetchRun
        FetchRun.objects.create(data_feed=self.feed)

        response = self.client.get(FETCH_RUNS_URL.format(self.collection.pk))
        entry = response.json()["fetch_runs"][0]
        self.assertNotIn("run_time", entry)


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
        fi = _make_file_ingestion(self.collection, file_path="cat4/col4/pending.grib2")
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


def _make_upload_session(catalog, user=None, file_path=None, collection=None):
    """Create an UploadSession with one UploadedFile, optionally linked via FileIngestion."""
    from georiva.ingestion.models import UploadSession, UploadedFile
    session = UploadSession.objects.create(catalog=catalog, user=user)
    fp = file_path or f"{catalog.slug}/file.grib2"
    uf = UploadedFile.objects.create(session=session, original_filename="file.grib2", file_path=fp)
    if collection is not None:
        fi = FileIngestion.objects.create(bucket=BucketType.SOURCES, file_path=fp)
        fi.collections.add(collection)
    return session


class CollectionUploadSessionsAPITests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin_us", "us@test.com", "pw")
        self.client.force_login(self.user)
        self.catalog = Catalog.objects.create(name="cat-us", slug="cat-us", file_format="grib2")
        self.collection = Collection.objects.create(name="col-us", slug="col-us", catalog=self.catalog)

    def test_upload_sessions_returns_200_with_upload_sessions_key(self):
        response = self.client.get(UPLOAD_SESSIONS_URL.format(self.collection.pk))
        self.assertEqual(response.status_code, 200)
        self.assertIn("upload_sessions", response.json())

    def test_upload_sessions_returns_sessions_linked_via_file_ingestion_m2m(self):
        session = _make_upload_session(self.catalog, collection=self.collection)
        response = self.client.get(UPLOAD_SESSIONS_URL.format(self.collection.pk))
        ids = [s["id"] for s in response.json()["upload_sessions"]]
        self.assertIn(session.pk, ids)

    def test_upload_sessions_excludes_unlinked_sessions(self):
        other_collection = Collection.objects.create(
            name="other", slug="other-col", catalog=self.catalog
        )
        _make_upload_session(self.catalog, collection=other_collection)
        response = self.client.get(UPLOAD_SESSIONS_URL.format(self.collection.pk))
        self.assertEqual(len(response.json()["upload_sessions"]), 0)

    def test_upload_sessions_returns_404_for_unknown_collection(self):
        response = self.client.get(UPLOAD_SESSIONS_URL.format(99999))
        self.assertEqual(response.status_code, 404)

    def test_upload_sessions_duration_seconds_set_when_completed(self):
        from georiva.ingestion.models import UploadSession
        session = _make_upload_session(self.catalog, collection=self.collection)
        session.completed_at = session.started_at + timedelta(seconds=30)
        session.save(update_fields=["completed_at"])

        response = self.client.get(UPLOAD_SESSIONS_URL.format(self.collection.pk))
        entry = response.json()["upload_sessions"][0]
        self.assertAlmostEqual(entry["duration_seconds"], 30, delta=1)

    def test_upload_sessions_duration_seconds_null_when_not_completed(self):
        _make_upload_session(self.catalog, collection=self.collection)
        response = self.client.get(UPLOAD_SESSIONS_URL.format(self.collection.pk))
        entry = response.json()["upload_sessions"][0]
        self.assertIsNone(entry["duration_seconds"])

    def test_upload_sessions_uploaded_by_is_username(self):
        _make_upload_session(self.catalog, user=self.user, collection=self.collection)
        response = self.client.get(UPLOAD_SESSIONS_URL.format(self.collection.pk))
        entry = response.json()["upload_sessions"][0]
        self.assertEqual(entry["uploaded_by"], self.user.username)

    def test_upload_sessions_uploaded_by_is_null_for_anonymous(self):
        _make_upload_session(self.catalog, user=None, collection=self.collection)
        response = self.client.get(UPLOAD_SESSIONS_URL.format(self.collection.pk))
        entry = response.json()["upload_sessions"][0]
        self.assertIsNone(entry["uploaded_by"])
