"""Chains a person starts begin with that person (ADR 0030, #426)."""

import uuid
from io import StringIO
from unittest.mock import MagicMock, PropertyMock, patch

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.management import call_command
from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Catalog, Collection
from georiva.core.storage import BucketType
from georiva.events.context import current_context
from georiva.ingestion.consumer import _handle_event
from georiva.ingestion.models import Delivery, FileIngestion, RunIngestion, RunIngestionClosed
from georiva.organisations.testing import dial_org, make_organisation

from .test_upload_page import SUBMIT_URL, _geotiff_setup, _mock_incoming_bucket

User = get_user_model()


def _post_upload(client, config, variable, **headers):
    return client.post(
        SUBMIT_URL.format(config.pk),
        {
            "variable_ids": [variable.pk],
            "times": [""],
            "files": [SimpleUploadedFile("20250115.tif", b"tiff-bytes")],
        },
        **headers,
    )


@patch("georiva.ingestion.tasks.process_incoming_file")
@patch("georiva.core.storage.StorageManager.incoming", new_callable=PropertyMock)
class UploadAttributionTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("uploader", "u@x.test", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)

    def _dispatched_contexts(self, task):
        seen = []
        task.delay = MagicMock(side_effect=lambda **kw: seen.append(current_context()))
        return seen

    def test_the_dispatch_carries_the_user_and_the_active_organisation(self, mock_incoming, task):
        mock_incoming.return_value = _mock_incoming_bucket()
        catalog, _, config, variable = _geotiff_setup()
        seen = self._dispatched_contexts(task)

        _post_upload(self.client, config, variable)

        (context,) = seen
        self.assertEqual(context.actor, f"user:{self.user.pk}")
        self.assertEqual(context.organisation_id, catalog.organisation_id)

    def test_an_inbound_correlation_header_names_the_chain(self, mock_incoming, task):
        mock_incoming.return_value = _mock_incoming_bucket()
        _, _, config, variable = _geotiff_setup()
        seen = self._dispatched_contexts(task)
        mine = uuid.uuid4()

        _post_upload(self.client, config, variable, HTTP_X_CORRELATION_ID=str(mine))

        self.assertEqual(seen[0].correlation_id, mine)
        self.assertEqual(Delivery.objects.get().correlation_id, mine)

    def test_a_malformed_correlation_header_is_ignored(self, mock_incoming, task):
        mock_incoming.return_value = _mock_incoming_bucket()
        _, _, config, variable = _geotiff_setup()
        seen = self._dispatched_contexts(task)

        _post_upload(self.client, config, variable, HTTP_X_CORRELATION_ID="not-a-uuid")

        self.assertEqual(len(seen), 1)

    def test_the_path_is_recorded_before_the_object_is_stored(self, mock_incoming, task):
        bucket = _mock_incoming_bucket()
        recorded_at_store = []
        original = bucket.save.side_effect

        def save(path, upload):
            recorded_at_store.append(Delivery.objects.filter(file_path=path).exists())
            return original(path, upload) if callable(original) else path

        bucket.save.side_effect = save
        mock_incoming.return_value = bucket
        _, _, config, variable = _geotiff_setup()

        _post_upload(self.client, config, variable)

        self.assertEqual(recorded_at_store, [True])

    def test_a_notification_that_wins_the_race_still_joins_the_users_chain(self, mock_incoming, task):
        bucket = _mock_incoming_bucket()
        original = bucket.save.side_effect
        adopted = []

        def save(path, upload):
            # The Bucket notification arrives while the view is still storing.
            with (
                patch("georiva.ingestion.consumer._resolve_origin", return_value=BucketType.INCOMING),
                patch("georiva.ingestion.consumer.process_incoming_file") as consumer_task,
            ):
                consumer_task.delay = MagicMock(side_effect=lambda **kw: adopted.append(current_context()))
                _handle_event({"s3": {"bucket": {"name": "georiva-incoming"}, "object": {"key": path}}})
            return original(path, upload) if callable(original) else path

        bucket.save.side_effect = save
        mock_incoming.return_value = bucket
        _, _, config, variable = _geotiff_setup()

        _post_upload(self.client, config, variable)

        (context,) = adopted
        self.assertEqual(context.actor, f"user:{self.user.pk}")
        self.assertEqual(context.correlation_id, Delivery.objects.get().correlation_id)


class AdminActionAttributionTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("operator", "o@x.test", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)
        catalog = Catalog.objects.create(
            organisation=make_organisation(), name="Models", slug="models", file_format="grib2"
        )
        collection = Collection.objects.create(catalog=catalog, name="Surface", slug="surface", is_forecast=True)
        self.run = RunIngestion.objects.create(collection=collection, reference_time="2026-09-02T12:00:00Z")

    def test_closing_a_run_by_hand_names_the_user(self):
        response = self.client.post(
            reverse("run_ingestion_detail", args=[self.run.pk]), {"action": "close"}, follow=False
        )

        self.assertEqual(response.status_code, 302)
        event = RunIngestionClosed.objects.get()
        self.assertEqual(event.closer, "manual")
        self.assertEqual(event.actor, f"user:{self.user.pk}")
        self.assertEqual(event.organisation_id, self.run.collection.catalog.organisation_id)

    def test_closing_twice_writes_one_event(self):
        url = reverse("run_ingestion_detail", args=[self.run.pk])
        self.client.post(url, {"action": "close"})
        self.client.post(url, {"action": "close"})
        self.assertEqual(RunIngestionClosed.objects.count(), 1)

    def test_a_forced_reingest_carries_the_acting_user_into_the_task_it_dispatches(self):
        from georiva.ingestion import unprocessed

        log = FileIngestion.objects.create(bucket=BucketType.SOURCES, file_path="test-org/models/x.grib2")
        seen = []
        with patch("georiva.ingestion.tasks.process_incoming_file") as task:
            task.delay = MagicMock(side_effect=lambda **kw: seen.append(current_context()))
            # What the admin's re-ingest action calls, run inside a request-shaped context.
            from georiva.events.context import new_context, use_context

            with use_context(new_context(actor=f"user:{self.user.pk}")):
                unprocessed.reingest_records([log])

        self.assertEqual(seen[0].actor, f"user:{self.user.pk}")


class ManagementCommandContextTests(TestCase):
    def test_run_recipe_records_under_a_named_context_instead_of_raising(self):
        seen = []

        def fake_run(recipe, selector, dispatch=True):
            seen.append(current_context())
            return []

        with (
            patch("georiva.processing.management.commands.run_recipe.run", fake_run),
            patch("georiva.processing.management.commands.run_recipe.recipe_registry") as registry,
        ):
            registry.get.return_value = MagicMock()
            call_command("run_recipe", "promotion", "--sync", stdout=StringIO())

        self.assertEqual(seen[0].actor, "system:run_recipe")

    def test_run_recipe_accepts_an_organisation(self):
        organisation = make_organisation("kenya")
        seen = []
        with (
            patch(
                "georiva.processing.management.commands.run_recipe.run",
                lambda recipe, selector, dispatch=True: seen.append(current_context()) or [],
            ),
            patch("georiva.processing.management.commands.run_recipe.recipe_registry") as registry,
        ):
            registry.get.return_value = MagicMock()
            call_command("run_recipe", "promotion", "--sync", "--organisation", "kenya", stdout=StringIO())

        self.assertEqual(seen[0].organisation_id, organisation.pk)


class PublicRoutesIgnoreTheHeaderTests(TestCase):
    def test_a_public_api_request_never_sets_a_context(self):
        seen = []
        from georiva.events.middleware import EventContextMiddleware

        middleware = EventContextMiddleware(lambda request: seen.append(current_context()) or MagicMock())
        request = MagicMock(path="/api/stac/", user=MagicMock(is_authenticated=True, pk=1))
        request.headers = {"X-Correlation-ID": str(uuid.uuid4())}
        middleware(request)

        self.assertEqual(seen, [None])
