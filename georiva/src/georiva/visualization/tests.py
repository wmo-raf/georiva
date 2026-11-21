"""The item preview page draws what Titiler can draw."""

from datetime import UTC, datetime

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Asset, Item
from georiva.organisations.testing import dial_org, make_org_tree, make_organisation

STORE = "https://data.upstream.example/gfs/v0.1.0.icechunk"


class ItemPreviewTests(TestCase):
    def setUp(self):
        dial_org(self.client)
        self.tree = make_org_tree(make_organisation())
        self.client.force_login(get_user_model().objects.create_superuser("root", "r@x.org", "pw"))

    def _preview(self, href, fmt):
        item = Item.objects.create(
            collection=self.tree["collection"],
            time=datetime(2026, 11, 16, 12, tzinfo=UTC),
            reference_time=datetime(2026, 11, 16, tzinfo=UTC),
        )
        Asset.objects.create(item=item, variable=self.tree["variable"], href=href, format=fmt)
        return self.client.get(reverse("item_preview", args=[item.pk]))

    def test_a_store_asset_is_drawn(self):
        response = self._preview(STORE, Asset.Format.ZARR)
        self.assertContains(response, 'data-visualizable="true"')
        self.assertContains(response, "encoded-preview.png")

    def test_a_zarr_kept_here_is_not_drawn(self):
        # A Zarr that is no Upstream store has no tile route to ask.
        response = self._preview("kenya/gfs/t2m/2026/11/16/t2m.zarr", Asset.Format.ZARR)
        self.assertContains(response, 'data-visualizable="false"')

    def test_an_assets_statistics_are_shown_in_the_served_unit_it_names(self):
        """Statistics are stored in the source unit and shown served (ADR 0046 §15):
        a spread by the scale alone."""
        from georiva.core.models import Unit

        variable = self.tree["variable"]
        variable.unit = Unit.objects.get_or_create(symbol="degF", defaults={"name": "test °F"})[0]
        variable.correct_source_unit(Unit.objects.get_or_create(symbol="K", defaults={"name": "test K"})[0])
        variable.save()
        item = Item.objects.create(collection=self.tree["collection"], time=datetime(2026, 11, 16, 12, tzinfo=UTC))
        Asset.objects.create(
            item=item,
            variable=variable,
            href="kenya/t.tif",
            format=Asset.Format.COG,
            stats_min=273.15,
            stats_max=373.15,
            stats_mean=323.15,
            stats_std=10.0,
        )

        response = self.client.get(reverse("item_preview", args=[item.pk]))

        [asset] = response.context["assets"]
        self.assertEqual(asset.served_stats, {"min": 32.0, "max": 212.0, "mean": 122.0, "std": 18.0})
        self.assertContains(response, 'data-asset-units="degF"')
        self.assertContains(response, ">212.00<")

    def test_a_store_assets_grid_is_not_said_to_be_none(self):
        # A Store asset's grid is the store's; the row has no width or height of its own (#685).
        response = self._preview(STORE, Asset.Format.ZARR)
        self.assertNotContains(response, "None×None")
