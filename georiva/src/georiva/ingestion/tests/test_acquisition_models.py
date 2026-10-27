from django.contrib.auth import get_user_model
from django.test import TestCase

from georiva.core.models import Catalog
from georiva.events.models import EventIndex
from georiva.ingestion.models import Delivery, DeliveryReceived, PushFeed, UploadSession
from georiva.organisations.testing import make_organisation

User = get_user_model()


def _push_feed():
    catalog = Catalog.objects.create(
        organisation=make_organisation(), name="Upload Test", slug="upload-test", file_format="grib2"
    )
    return PushFeed.objects.create(catalog=catalog, name="Upload Test", valid_time_format="YYYYMMDD")


def _session(feed=None, status="active"):
    return UploadSession.objects.create(push_feed=feed or _push_feed(), status=status)


def _delivery(session, name):
    return Delivery.receive(push_feed=session.push_feed, filename=name, file_path="", session=session)


class UploadSessionTransitionTests(TestCase):
    def setUp(self):
        self.feed = _push_feed()

    def test_active_to_failed(self):
        session = _session(self.feed)
        session.mark_failed()
        session.refresh_from_db()
        self.assertEqual(session.status, "failed")
        self.assertIsNotNone(session.completed_at)

    def test_active_to_cancelled(self):
        session = _session(self.feed)
        session.mark_cancelled()
        session.refresh_from_db()
        self.assertEqual(session.status, "cancelled")
        self.assertIsNotNone(session.completed_at)


class DeliveryTests(TestCase):
    def setUp(self):
        self.session = _session()
        self.user = User.objects.create_user("sender", "s@x.test", "pw")

    def test_received_to_accepted(self):
        d = _delivery(self.session, "rain_2024.grib")
        self.assertEqual(d.status, "received")

        d.mark_accepted(file_path="catalog/rain_2024.grib", bytes=2048)
        d.refresh_from_db()
        self.assertEqual(d.status, "accepted")
        self.assertEqual(d.file_path, "catalog/rain_2024.grib")
        self.assertEqual(d.bytes, 2048)
        self.assertIsNotNone(d.accepted_at)

    def test_each_delivery_starts_its_own_correlation(self):
        a = _delivery(self.session, "a.grib")
        b = _delivery(self.session, "b.grib")
        self.assertNotEqual(a.correlation_id, b.correlation_id)

    def test_receiving_records_the_first_event_of_the_chain(self):
        d = Delivery.receive(
            push_feed=self.session.push_feed,
            filename="a.grib",
            file_path="org/cat/a.grib",
            session=self.session,
            sent_by=self.user,
        )
        event = DeliveryReceived.objects.get()
        self.assertEqual(event.correlation_id, d.correlation_id)
        self.assertEqual(event.delivery, d)
        self.assertEqual(event.actor, f"user:{self.user.pk}")
        self.assertEqual(event.organisation_id, self.session.push_feed.catalog.organisation_id)
        self.assertTrue(EventIndex.objects.filter(event_id=event.pk, correlation_id=d.correlation_id).exists())


class UploadSessionAutoCompleteTests(TestCase):
    def setUp(self):
        self.session = _session()

    def test_auto_completes_when_all_deliveries_accepted(self):
        d1 = _delivery(self.session, "a.grib")
        d2 = _delivery(self.session, "b.grib")
        d1.mark_accepted(file_path="a.grib", bytes=100)
        d2.mark_accepted(file_path="b.grib", bytes=200)
        self.session.refresh_from_db()
        self.assertEqual(self.session.status, "completed")
        self.assertIsNotNone(self.session.completed_at)

    def test_does_not_auto_complete_while_a_delivery_is_received(self):
        d1 = _delivery(self.session, "a.grib")
        _delivery(self.session, "b.grib")
        d1.mark_accepted(file_path="a.grib", bytes=100)
        self.session.refresh_from_db()
        self.assertEqual(self.session.status, "active")
