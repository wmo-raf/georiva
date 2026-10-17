"""
``_collect_rows`` on a forecast collection (issue #412): the axis is the
newest run only, never the union of overlapping runs.
"""

from datetime import UTC, datetime, timedelta
from unittest import mock

from django.test import TestCase

from georiva.core.models import Asset, Item
from georiva.organisations.provisioning import provision_organisation
from georiva.virtual_zarr.models import VirtualZarrManifest
from georiva.virtual_zarr.tasks import _collect_rows
from georiva.virtual_zarr.tests.test_queue_rebuild import add_variable, build_collection

T0 = datetime(2026, 9, 1, tzinfo=UTC)


class CollectRowsTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        org = provision_organisation(name="Kenya Met", slug="kenya")
        cls.collection = build_collection(org, slug="fc")
        cls.variable = add_variable(cls.collection, slug="t2m")
        cls.manifest = VirtualZarrManifest.objects.create(variable=cls.variable)

    def _item(self, hours, ref_hours=None):
        item = Item.objects.create(
            collection=self.collection,
            time=T0 + timedelta(hours=hours),
            reference_time=None if ref_hours is None else T0 + timedelta(hours=ref_hours),
        )
        Asset.objects.create(
            item=item,
            variable=self.variable,
            format=Asset.Format.COG,
            href=f"fc/{hours}-{ref_hours}.tif",
        )
        return item

    def _collect(self):
        config = mock.Mock()
        config.url_for.side_effect = lambda href: f"http://minio/{href}"
        return _collect_rows(self.manifest, config)[0]

    def test_forecast_collection_uses_newest_run_only(self):
        self._item(0, 0)
        self._item(6, 0)
        self._item(6, 6)
        self._item(12, 6)
        self.collection.is_forecast = True
        self.collection.save()

        times = [r.time for r in self._collect()]

        self.assertEqual(times, [T0 + timedelta(hours=6), T0 + timedelta(hours=12)])

    def test_newest_run_without_a_cog_for_this_variable_is_ignored(self):
        self._item(0, 0)
        self._item(6, 0)
        # Newest run exists but has no COG for this variable yet.
        Item.objects.create(
            collection=self.collection,
            time=T0 + timedelta(hours=6),
            reference_time=T0 + timedelta(hours=6),
        )
        self.collection.is_forecast = True
        self.collection.save()

        times = [r.time for r in self._collect()]

        self.assertEqual(times, [T0, T0 + timedelta(hours=6)])

    def test_observational_collection_keeps_every_item(self):
        self._item(0)
        self._item(6)

        self.assertEqual(len(self._collect()), 2)
