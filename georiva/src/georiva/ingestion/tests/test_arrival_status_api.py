from django.test import TestCase

from georiva.ingestion.models import DataArrival

ARRIVAL_STATUS_URL = "/api/arrivals/{}/status/"


class ArrivalStatusEndpointTests(TestCase):

    def test_known_arrival_returns_id_status_error_message(self):
        arrival = DataArrival.objects.create(
            trigger=DataArrival.Trigger.MANUAL_UPLOAD,
            status=DataArrival.Status.COMPLETED,
            error_message="",
        )

        response = self.client.get(ARRIVAL_STATUS_URL.format(arrival.pk))

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["id"], arrival.pk)
        self.assertEqual(data["status"], DataArrival.Status.COMPLETED)
        self.assertEqual(data["error_message"], "")

    def test_unknown_arrival_returns_404(self):
        response = self.client.get(ARRIVAL_STATUS_URL.format(99999))
        self.assertEqual(response.status_code, 404)

    def test_error_message_populated_when_set(self):
        arrival = DataArrival.objects.create(
            trigger=DataArrival.Trigger.MANUAL_UPLOAD,
            status=DataArrival.Status.FAILED,
            error_message="MinIO write failed: connection refused",
        )

        response = self.client.get(ARRIVAL_STATUS_URL.format(arrival.pk))

        data = response.json()
        self.assertEqual(data["error_message"], "MinIO write failed: connection refused")

    def test_terminal_status_failed(self):
        arrival = DataArrival.objects.create(
            trigger=DataArrival.Trigger.MANUAL_UPLOAD,
            status=DataArrival.Status.FAILED,
        )
        response = self.client.get(ARRIVAL_STATUS_URL.format(arrival.pk))
        self.assertEqual(response.json()["status"], DataArrival.Status.FAILED)

    def test_terminal_status_partial(self):
        arrival = DataArrival.objects.create(
            trigger=DataArrival.Trigger.MANUAL_UPLOAD,
            status=DataArrival.Status.PARTIAL,
        )
        response = self.client.get(ARRIVAL_STATUS_URL.format(arrival.pk))
        self.assertEqual(response.json()["status"], DataArrival.Status.PARTIAL)

    def test_terminal_status_empty(self):
        arrival = DataArrival.objects.create(
            trigger=DataArrival.Trigger.MANUAL_UPLOAD,
            status=DataArrival.Status.EMPTY,
        )
        response = self.client.get(ARRIVAL_STATUS_URL.format(arrival.pk))
        self.assertEqual(response.json()["status"], DataArrival.Status.EMPTY)

    def test_response_has_exactly_three_keys(self):
        arrival = DataArrival.objects.create(
            trigger=DataArrival.Trigger.MANUAL_UPLOAD,
            status=DataArrival.Status.PENDING,
        )
        response = self.client.get(ARRIVAL_STATUS_URL.format(arrival.pk))
        self.assertEqual(set(response.json().keys()), {"id", "status", "error_message"})
