"""The local Upstream store a test builds and reads through core's values sessions.

`georiva.harvest.testing.LocalStore` is test support shared with plugins: a
forecast store on disk, named arrays over a run axis, steps each array holds
chosen by the test, and the rows a store harvest would have made pointing at it.
"""

from datetime import UTC, datetime, timedelta

import numpy as np
from django.test import TestCase

from georiva.core import values
from georiva.core.models import Asset, Item
from georiva.harvest.testing import LocalStore, StoreArray, StoreVariable
from georiva.organisations.testing import make_organisation

RUNS = [datetime(2026, 5, 1, 0, tzinfo=UTC), datetime(2026, 5, 1, 12, tzinfo=UTC)]
LEADS = [timedelta(hours=0), timedelta(hours=3)]


def temperature(run, step):
    return np.full((4, 8), 280.0 + 10 * run + step, dtype=np.float32)


def rain(run, step):
    field = np.zeros((4, 8), dtype=np.float32)
    field[0, 0] = 1.0 + run + step  # the north-west cell
    return field


class LocalStoreTests(TestCase):
    PINS = {"s3://noaa-gfs-bdp-pds/": "us-east-1"}

    def setUp(self):
        self.store = LocalStore(
            self,
            runs=RUNS,
            leads=LEADS,
            arrays=[
                StoreArray("t2m", values=temperature),
                StoreArray("tp", values=rain, held=lambda run, step: step > 0),
            ],
        )

    def test_each_array_reads_its_own_fields_north_up(self):
        with values.session() as s:
            self.assertEqual(s.field(self.store.at("t2m", 1, 1)).data[2, 3], 291.0)
            self.assertEqual(s.field(self.store.at("tp", 0, 1)).data[0, 0], 2.0)

    def test_a_step_an_array_does_not_hold_is_not_held(self):
        with self.assertRaises(values.StepNotHeld):
            values.field(self.store.at("tp", 1, 0))
        self.assertEqual(values.field(self.store.at("t2m", 1, 0)).data[0, 0], 290.0)

    def test_the_harvested_rows_point_at_the_store_and_read_through_their_feed(self):
        harvested = self.store.harvest(
            make_organisation(),
            variables={
                "t2m": StoreVariable("temperature", source_unit="K", unit="degC"),
                "tp": StoreVariable("precipitation", source_unit="kg m-2", unit="mm", water=True),
            },
            pins=self.PINS,
        )

        self.assertEqual(harvested.feed.pinned_chunk_locations, self.PINS)
        self.assertEqual(harvested.feed.store_href, self.store.href)
        rain_variable = harvested.variables["tp"]
        self.assertTrue(rain_variable.water)
        self.assertEqual((rain_variable.source_unit.symbol, rain_variable.unit.symbol), ("kg m-2", "mm"))
        self.assertEqual({m.asset_key: m.variable for m in harvested.feed.variable_mappings.all()}, harvested.variables)
        self.assertEqual(Item.objects.filter(collection=harvested.collection).count(), 4)
        self.assertEqual(Asset.objects.filter(variable=harvested.variables["t2m"]).count(), 4)
        self.assertEqual(Asset.objects.filter(variable=rain_variable).count(), 2)

        asset = Asset.objects.get(variable=rain_variable, item__reference_time=RUNS[1], item__time=RUNS[1] + LEADS[1])
        self.assertEqual((asset.format, asset.href, asset.store_array), (Asset.Format.ZARR, self.store.href, "tp"))
        self.assertEqual(values.field(asset).data[0, 0], 3.0)
