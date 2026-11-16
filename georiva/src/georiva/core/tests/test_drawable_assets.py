"""One rule for "Titiler can draw this asset" (#686).

A COG — ours or an upstream's — and a Store asset are drawn; nothing else is.
Every surface that offers a map, a thumbnail or a preview asks the same rule,
in Python (:attr:`Asset.is_drawable`) or in the database (:func:`drawable_q`),
and the two must never disagree.
"""

from datetime import UTC, datetime, timedelta

from django.test import TestCase

from georiva.core.models import Asset, Item
from georiva.core.storage.asset_location import drawable_q
from georiva.organisations.testing import make_org_tree, make_organisation

STORE = "https://data.upstream.example/gfs/v0.1.0.icechunk"

#: (href, format, drawable?)
CASES = [
    ("kenya/gfs/t2m/2026/03/01/t2m.tif", Asset.Format.COG, True),
    ("https://upstream.example/t2m.tif", Asset.Format.COG, True),
    (STORE, Asset.Format.ZARR, True),
    # A Zarr kept here is no Upstream store: there is no tile route to ask.
    ("kenya/gfs/t2m/2026/03/01/t2m.zarr", Asset.Format.ZARR, False),
    ("kenya/gfs/t2m/2026/03/01/t2m-raw.tif", Asset.Format.GEOTIFF, False),
    ("https://upstream.example/t2m.png", Asset.Format.PNG, False),
    ("kenya/gfs/t2m/2026/03/01/t2m.nc", Asset.Format.NETCDF, False),
]


class DrawableAssetTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        tree = make_org_tree(make_organisation())
        tree["item"].delete()
        cls.assets = {}
        for i, (href, fmt, drawable) in enumerate(CASES):
            item = Item.objects.create(
                collection=tree["collection"],
                time=datetime(2026, 3, 1, tzinfo=UTC) + timedelta(hours=i),
            )
            asset = Asset.objects.create(item=item, variable=tree["variable"], href=href, format=fmt)
            cls.assets[(href, fmt)] = (asset, drawable)

    def test_the_property_draws_a_cog_anywhere_and_a_store_asset(self):
        for (href, fmt), (asset, drawable) in self.assets.items():
            with self.subTest(href=href, format=fmt):
                self.assertIs(asset.is_drawable, drawable)

    def test_the_q_and_the_property_agree(self):
        by_property = {a.pk for a in Asset.objects.all() if a.is_drawable}
        by_q = set(Asset.objects.filter(drawable_q()).values_list("pk", flat=True))
        self.assertEqual(by_q, by_property)

    def test_the_q_agrees_through_a_relation(self):
        by_property = {a.item_id for a in Asset.objects.all() if a.is_drawable}
        by_q = set(Item.objects.filter(drawable_q("assets__")).values_list("pk", flat=True))
        self.assertEqual(by_q, by_property)
