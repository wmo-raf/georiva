"""A collection's model runs, and which is the latest complete one (ADR 0026, ADR 0036).

A forecast client opens the newest run that has *finished arriving*. A run still
arriving must never be offered, and a run a late file reopened is not complete
again until it closes — the client falls back to the run before it.
"""

from datetime import UTC, datetime, timedelta

from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Catalog, Collection
from georiva.ingestion.models import RunIngestion
from georiva.organisations.testing import dial_org, make_organisation

REF = datetime(2026, 9, 2, 0, 0, tzinfo=UTC)


class RunApiTests(TestCase):
    def setUp(self):
        dial_org(self.client)
        self.catalog = Catalog.objects.create(
            organisation=make_organisation(), name="Models", slug="models", file_format="grib2"
        )
        self.collection = self._collection("ifs-surface")

    def _collection(self, slug, visibility=Collection.Visibility.PUBLIC, is_forecast=True):
        return Collection.objects.create(
            catalog=self.catalog, name=slug, slug=slug, visibility=visibility, is_forecast=is_forecast
        )

    def _run(self, hours, status=RunIngestion.Status.CLOSED, collection=None, closed_by="declared_set", **extra):
        return RunIngestion.objects.create(
            collection=collection or self.collection,
            reference_time=REF + timedelta(hours=hours),
            status=status,
            closed_by=closed_by if status == RunIngestion.Status.CLOSED else "",
            **extra,
        )

    def _get(self, collection=None):
        slug = (collection or self.collection).slug
        return self.client.get(reverse("run-list", args=["models", slug]))

    def test_latest_complete_is_newest_closed_run(self):
        self._run(0)
        newest = self._run(12)
        body = self._get().json()
        self.assertEqual(body["latest_complete"]["reference_time"], newest.reference_time.isoformat())
        self.assertEqual(len(body["runs"]), 2)
        self.assertEqual(body["runs"][0]["reference_time"], newest.reference_time.isoformat())

    def test_run_still_arriving_is_not_latest_complete(self):
        done = self._run(0)
        self._run(12, status=RunIngestion.Status.OPEN)
        body = self._get().json()
        self.assertEqual(body["latest_complete"]["reference_time"], done.reference_time.isoformat())
        self.assertFalse(body["runs"][0]["complete"])
        self.assertTrue(body["runs"][1]["complete"])

    def test_reopened_run_falls_back_to_previous_complete_run(self):
        previous = self._run(0)
        late = self._run(12)
        RunIngestion.record_file(self.collection, late.reference_time)
        body = self._get().json()
        self.assertEqual(body["latest_complete"]["reference_time"], previous.reference_time.isoformat())
        reopened = body["runs"][0]
        self.assertFalse(reopened["complete"])
        self.assertEqual(reopened["revision"], 1)

    def test_no_complete_run_is_200_with_null(self):
        self._run(0, status=RunIngestion.Status.OPEN)
        response = self._get()
        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.json()["latest_complete"])

    def test_collection_without_runs_is_200_with_empty_list(self):
        body = self._get().json()
        self.assertEqual(body["runs"], [])
        self.assertIsNone(body["latest_complete"])

    def test_run_exposes_evidence_for_the_client_to_judge(self):
        self._run(0, closed_by="quiet_period", file_count=3, expected_file_count=5)
        run = self._get().json()["latest_complete"]
        self.assertEqual(run["closed_by"], "quiet_period")
        self.assertEqual(run["file_count"], 3)
        self.assertEqual(run["expected_file_count"], 5)
        self.assertIn("version", run)

    def test_internal_collection_is_not_discoverable(self):
        internal = self._collection("hidden", visibility=Collection.Visibility.INTERNAL)
        self._run(0, collection=internal)
        self.assertEqual(self._get(internal).status_code, 404)

    def test_private_collection_is_not_discoverable_by_anonymous(self):
        private = self._collection("members", visibility=Collection.Visibility.PRIVATE)
        self._run(0, collection=private)
        self.assertEqual(self._get(private).status_code, 404)

    def test_unknown_collection_is_404(self):
        response = self.client.get(reverse("run-list", args=["models", "nope"]))
        self.assertEqual(response.status_code, 404)

    def test_limit_caps_the_listing(self):
        for h in range(0, 60, 12):
            self._run(h)
        body = self.client.get(reverse("run-list", args=["models", "ifs-surface"]), {"limit": 2}).json()
        self.assertEqual(len(body["runs"]), 2)

    def test_closed_by_narrows_latest_complete(self):
        strong = self._run(0, closed_by="declared_set")
        self._run(12, closed_by="quiet_period")
        url = reverse("run-list", args=["models", "ifs-surface"])
        body = self.client.get(url, {"closed_by": "declared_set,upload_session"}).json()
        self.assertEqual(body["latest_complete"]["reference_time"], strong.reference_time.isoformat())
        self.assertEqual(len(body["runs"]), 2)

    def test_closed_by_with_no_match_is_null(self):
        self._run(0, closed_by="quiet_period")
        url = reverse("run-list", args=["models", "ifs-surface"])
        body = self.client.get(url, {"closed_by": "declared_set"}).json()
        self.assertIsNone(body["latest_complete"])

    def test_unknown_closed_by_is_400(self):
        response = self.client.get(reverse("run-list", args=["models", "ifs-surface"]), {"closed_by": "bogus"})
        self.assertEqual(response.status_code, 400)
