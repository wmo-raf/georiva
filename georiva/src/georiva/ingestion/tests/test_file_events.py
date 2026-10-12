"""File ingestion and publication history as Domain events (ADR 0030, #423)."""

from contextlib import ExitStack
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from django.contrib.auth.models import Group, Permission
from django.test import TestCase, override_settings
from django.urls import reverse

from georiva.core.models import Catalog, Collection, ProductPublished, Unit, Variable
from georiva.core.storage import BucketType
from georiva.events.context import current_context
from georiva.events.testing import event_context
from georiva.ingestion.consumer import _handle_event
from georiva.ingestion.models import FileIngestion, FileIngestionFinished
from georiva.ingestion.service import IngestionService
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation
from georiva.organisations.testing import make_organisation
from georiva.organisations.tests.factories import PASSWORD, add_member, make_user

KEY = "kenya/chirps/rainfall/rain.tif"


def _tree(organisation, visibility=Collection.Visibility.PUBLIC, collection_slug="rainfall"):
    catalog, _ = Catalog.objects.get_or_create(
        organisation=organisation,
        slug="chirps",
        defaults={"name": "CHIRPS", "file_format": "geotiff", "clip_mode": "none"},
    )
    collection = Collection.objects.create(
        catalog=catalog, name=collection_slug, slug=collection_slug, visibility=visibility
    )
    unit, _ = Unit.objects.get_or_create(name="Millimetre", symbol="mm")
    Variable.objects.create(
        collection=collection, name="Rain", slug="rain", unit=unit, value_min=0, value_max=1, is_active=True
    )
    return catalog, collection


def _process(file_path, timestamps, *, fail=False):
    """Run ``process_file`` with the storage and format boundaries mocked."""
    # In production the consumer has registered the file before the task runs.
    FileIngestion.register(bucket=BucketType.SOURCES, file_path=file_path)
    plugin = MagicMock()
    plugin.get_timestamps.return_value = timestamps
    handler = MagicMock()
    if fail:
        handler.process_timestamp.side_effect = RuntimeError("cannot read band")
    else:
        handler.process_timestamp.side_effect = lambda **kw: (MagicMock(pk=1), [MagicMock(pk=1)], None, [])
    with (
        patch("georiva.ingestion.service.storage"),
        patch("georiva.ingestion.service.format_registry") as registry,
        patch("georiva.ingestion.service.IngestionHandler", return_value=handler),
        patch.object(IngestionService, "_get_first_variable_name", return_value="rain"),
    ):
        registry.get.return_value = plugin
        service = IngestionService()
        service._source_file_manager = MagicMock()
        return service.process_file(file_path, origin_bucket=BucketType.SOURCES)


def _finish(file_path, result):
    FileIngestion.register(bucket=BucketType.SOURCES, file_path=file_path)
    FileIngestion.acquire(BucketType.SOURCES, file_path, "w1")
    if result.success:
        FileIngestion.mark_completed(
            BucketType.SOURCES,
            file_path,
            items_created=len(result.items_created),
            assets_created=len(result.assets_created),
        )
    else:
        FileIngestion.mark_failed(BucketType.SOURCES, file_path, "; ".join(result.errors))


TIMES = [datetime(2026, 9, 2, h, tzinfo=UTC) for h in (0, 6, 12)]


class IngestionHistoryTests(TestCase):
    def setUp(self):
        self.kenya = make_organisation("kenya")
        self.catalog, self.collection = _tree(self.kenya)

    def test_a_completed_file_yields_one_finished_event_and_one_published_event_per_collection(self):
        with event_context(organisation_id=self.kenya.pk) as ctx:
            _finish(KEY, _process(KEY, TIMES))

        finished = FileIngestionFinished.objects.get()
        published = ProductPublished.objects.get()
        self.assertEqual(finished.outcome, "completed")
        self.assertEqual(finished.collections_touched, ["chirps/rainfall"])
        self.assertEqual(finished.items_created, 3)
        self.assertEqual(published.item_count, 3)  # one event, three Items
        self.assertEqual(published.valid_time_start, TIMES[0])
        self.assertEqual(published.valid_time_end, TIMES[-1])
        self.assertEqual(published.source_file, f"sources:{KEY}")
        self.assertEqual({finished.correlation_id, published.correlation_id}, {ctx.correlation_id})
        self.assertEqual({finished.organisation_id, published.organisation_id}, {self.kenya.pk})

    def test_a_failed_ingestion_yields_a_failed_event_and_no_published_event(self):
        with event_context(organisation_id=self.kenya.pk):
            _finish(KEY, _process(KEY, TIMES, fail=True))

        finished = FileIngestionFinished.objects.get()
        self.assertEqual(finished.outcome, "failed")
        self.assertIn("cannot read band", finished.error)
        self.assertEqual(finished.outcome_class, "failed")
        self.assertFalse(ProductPublished.objects.exists())

    def test_a_reingested_file_yields_a_second_event_and_leaves_the_first_untouched(self):
        with event_context(organisation_id=self.kenya.pk):
            _finish(KEY, _process(KEY, TIMES, fail=True))
        first = FileIngestionFinished.objects.get()

        FileIngestion.reset_for_reingest(BucketType.SOURCES, KEY)
        with event_context(organisation_id=self.kenya.pk):
            _finish(KEY, _process(KEY, TIMES))

        self.assertEqual(FileIngestionFinished.objects.count(), 2)
        first.refresh_from_db()
        self.assertEqual(first.outcome, "failed")

    def test_a_published_event_for_an_internal_collection_records_the_tier(self):
        _, internal = _tree(self.kenya, visibility=Collection.Visibility.INTERNAL, collection_slug="scratch")
        key = "kenya/chirps/scratch/x.tif"

        with event_context(organisation_id=self.kenya.pk):
            _finish(key, _process(key, TIMES))

        self.assertEqual(ProductPublished.objects.get().visibility, "internal")

    def test_the_run_row_counters_are_unchanged(self):
        with event_context(organisation_id=self.kenya.pk):
            _finish(KEY, _process(KEY, TIMES))
        log = FileIngestion.objects.get()
        self.assertEqual((log.status, log.items_created), ("completed", 3))


class ConsumerContextTests(TestCase):
    def setUp(self):
        self.kenya = make_organisation("kenya")
        Catalog.objects.create(organisation=self.kenya, name="CHIRPS", slug="chirps", file_format="grib2")

    def _handle(self, key):
        seen = []
        with (
            patch("georiva.ingestion.consumer._resolve_origin", return_value=BucketType.SOURCES),
            patch("georiva.ingestion.consumer.process_incoming_file") as task,
        ):
            task.delay = MagicMock(side_effect=lambda **kw: seen.append(current_context()))
            _handle_event({"s3": {"bucket": {"name": "georiva-sources"}, "object": {"key": key}}})
        return seen

    def test_a_notification_dispatches_under_a_fresh_context_for_the_keys_organisation(self):
        (context,) = self._handle("kenya/chirps/rainfall/rain.grib2")

        self.assertEqual(context.actor, "system:bucket")
        self.assertEqual(context.organisation_id, self.kenya.pk)
        self.assertIsNone(current_context())

    def test_each_notification_starts_its_own_chain(self):
        (first,) = self._handle("kenya/chirps/rainfall/a.grib2")
        (second,) = self._handle("kenya/chirps/rainfall/b.grib2")
        self.assertNotEqual(first.correlation_id, second.correlation_id)

    def test_a_refusal_inside_the_handler_is_recorded_under_the_same_chain(self):
        self._handle("atlantis/chirps/rainfall/rain.grib2")

        finished = FileIngestionFinished.objects.get()
        self.assertEqual(finished.actor, "system:bucket")
        self.assertIsNone(finished.organisation_id)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class HistoryPanelPageTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        _, cls.kenya_collection = _tree(cls.kenya)
        _, cls.uganda_collection = _tree(cls.uganda)
        for org, key in ((cls.kenya, KEY), (cls.uganda, "uganda/chirps/rainfall/rain.tif")):
            with event_context(organisation_id=org.pk):
                _finish(key, _process(key, TIMES))

    def setUp(self):
        user = make_user("amina")
        add_member(user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        group = Group.objects.create(name="everything")
        group.permissions.add(*Permission.objects.all())
        user.groups.add(group)
        self.client.login(username="amina", password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    def test_the_file_page_shows_its_own_history(self):
        log = FileIngestion.objects.get(file_path=KEY)
        response = self.client.get(reverse("file_ingestion_detail", args=[log.pk]))

        self.assertEqual(response.status_code, 200)
        types = [row.type_label for row in response.context["history"].rows]
        self.assertCountEqual(types, ["File ingestion finished", "Product published"])

    def test_another_organisations_file_page_is_not_found(self):
        log = FileIngestion.objects.get(file_path="uganda/chirps/rainfall/rain.tif")
        self.assertEqual(self.client.get(reverse("file_ingestion_detail", args=[log.pk])).status_code, 404)

    def test_the_collection_page_shows_only_this_organisations_publications(self):
        response = self.client.get(reverse("collection_items_list", args=[self.kenya_collection.pk]))

        self.assertEqual(response.status_code, 200)
        rows = response.context["history"].rows
        self.assertEqual([row.type_label for row in rows], ["Product published"])
        self.assertEqual({row.event.organisation_id for row in rows}, {self.kenya.pk})


class RefusalTests(TestCase):
    """A refused arrival is a recorded fact, owned by nobody when its address cannot be trusted (#424)."""

    def setUp(self):
        self.kenya = make_organisation("kenya")
        Catalog.objects.create(organisation=self.kenya, name="CHIRPS", slug="chirps", file_format="grib2")

    def _handle(self, key, **patches):
        with (
            patch("georiva.ingestion.consumer._resolve_origin", return_value=BucketType.SOURCES),
            patch("georiva.ingestion.consumer.process_incoming_file") as task,
        ):
            task.delay = MagicMock()
            with ExitStack() as stack:
                for name, replacement in patches.items():
                    stack.enter_context(patch(f"georiva.ingestion.consumer.{name}", replacement))
                _handle_event({"s3": {"bucket": {"name": "georiva-sources"}, "object": {"key": key}}})

    def test_an_unknown_organisation_yields_one_unattributed_refusal(self):
        self._handle("atlantis/chirps/rainfall/rain.grib2")

        event = FileIngestionFinished.objects.get()
        self.assertEqual(event.outcome, "refused")
        self.assertIsNone(event.organisation_id)
        self.assertEqual(event.file_path, "atlantis/chirps/rainfall/rain.grib2")
        self.assertIn("Unknown organisation", event.error)
        self.assertEqual(event.outcome_class, "refused")

    def test_an_unknown_catalog_in_a_known_organisation_is_unattributed_too(self):
        self._handle("kenya/nonesuch/rainfall/rain.grib2")

        self.assertIsNone(FileIngestionFinished.objects.get().organisation_id)

    def test_an_invalid_path_yields_no_event_and_no_row(self):
        self._handle("not-a-valid-key")

        self.assertFalse(FileIngestionFinished.objects.exists())
        self.assertFalse(FileIngestion.objects.exists())

    def test_a_refusal_inside_a_known_organisation_is_attributed_to_it(self):
        self._handle("kenya/chirps/rainfall/rain.grib2", _required_time_error=lambda catalog, key: "no valid time")

        event = FileIngestionFinished.objects.get()
        self.assertEqual(event.outcome, "refused")
        self.assertEqual(event.organisation_id, self.kenya.pk)

    def test_a_refusal_leaves_the_failed_row_it_always_left(self):
        self._handle("atlantis/chirps/rainfall/rain.grib2")
        self.assertEqual(FileIngestion.objects.get().status, FileIngestion.Status.FAILED)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class UnattributedListingTests(TestCase):
    """The one deliberately unscoped admin page, and an explicit exception to the URL sweep."""

    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        with event_context(organisation_id=None):
            FileIngestion.register(BucketType.SOURCES, "atlantis/x/y/z.tif")
            FileIngestion.mark_failed(
                BucketType.SOURCES, "atlantis/x/y/z.tif", "Unknown organisation 'atlantis'.", outcome="refused"
            )
        with event_context(organisation_id=cls.kenya.pk):
            FileIngestion.register(BucketType.SOURCES, "kenya/x/y/z.tif")
            FileIngestion.mark_failed(BucketType.SOURCES, "kenya/x/y/z.tif", "boom")

    def _login(self, *, superuser, host="kenya.georiva.test"):
        user = make_user("root" if superuser else "amina", superuser=superuser)
        add_member(user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        group = Group.objects.create(name=f"everything-{user.username}")
        group.permissions.add(*Permission.objects.all())
        user.groups.add(group)
        self.client.login(username=user.username, password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = host

    def test_a_superuser_sees_the_unattributed_event_and_only_that(self):
        self._login(superuser=True)
        response = self.client.get(reverse("events_unattributed"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "atlantis/x/y/z.tif")
        self.assertNotContains(response, "kenya/x/y/z.tif")

    def test_a_non_superuser_is_denied(self):
        self._login(superuser=False)
        response = self.client.get(reverse("events_unattributed"))

        # Wagtail turns a PermissionDenied into a redirect home with a message.
        self.assertIn(response.status_code, (302, 403))
        self.assertNotContains(response, "atlantis", status_code=response.status_code)

    def test_the_event_appears_on_no_tenant_scoped_surface_even_for_a_superuser(self):
        self._login(superuser=True)
        log = FileIngestion.objects.get(file_path="kenya/x/y/z.tif")
        page = self.client.get(reverse("file_ingestion_detail", args=[log.pk]))

        self.assertEqual(page.status_code, 200)
        self.assertEqual([row.event.organisation_id for row in page.context["history"].rows], [self.kenya.pk])
        self.assertNotContains(page, "atlantis")
