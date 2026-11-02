"""Every surface unscales the texture over the variable's range (ADR 0022 §4),
even when the default style's stops span less of it — the ``tp`` case."""

from datetime import UTC, datetime

from django.test import TestCase
from django.urls import reverse

from georiva.core.machine_plane.render_contract import render_contract
from georiva.core.models import Asset, Collection, Item
from georiva.core.testing import make_style
from georiva.organisations.testing import dial_org, make_org_tree, make_organisation

RANGE = [0.0, 300.0]


class RenderContractTests(TestCase):
    def setUp(self):
        dial_org(self.client)
        tree = make_org_tree(make_organisation())
        self.variable = tree["variable"]
        self.collection = tree["collection"]
        self.collection.visibility = Collection.Visibility.PUBLIC
        self.collection.save()
        self.variable.value_min, self.variable.value_max = 0.0, 300.0
        self.variable.save()
        make_style(
            self.variable,
            stops=[{"value": 0.0, "color": "#0000ff"}, {"value": 100.0, "color": "#ff0000"}],
        )
        self.item = Item.objects.create(
            collection=self.collection,
            time=datetime(2026, 4, 1, tzinfo=UTC),
            bounds=[0.0, 0.0, 10.0, 10.0],
        )
        Asset.objects.create(item=self.item, variable=self.variable, href="x.tif", format=Asset.Format.COG)

    def test_the_contract_unscales_over_the_range_and_legends_over_the_stops(self):
        contract = render_contract(self.variable)
        self.assertEqual(contract.image_unscale, RANGE)
        self.assertEqual(contract.legend_range, (0.0, 100.0))
        self.assertEqual(contract.palette, [[0.0, [0, 0, 255]], [100.0, [255, 0, 0]]])

    def test_a_styleless_variable_legends_over_the_range(self):
        self.variable.styles.all().delete()
        contract = render_contract(self.variable)
        self.assertEqual(contract.image_unscale, RANGE)
        self.assertEqual(contract.legend_range, (0.0, 300.0))

    def test_the_edr_block_carries_image_unscale_and_keeps_the_legend_keys(self):
        response = self.client.get(reverse("edr:collection-detail", args=[self.collection.slug]))
        xg = response.json()["parameter_names"][self.variable.slug]["x-georiva"]
        self.assertEqual(xg["image_unscale"], RANGE)
        self.assertEqual((xg["palette_min"], xg["palette_max"]), (0.0, 100.0))

    def test_the_stac_visual_asset_unscales_over_the_range(self):
        from georiva.stac.serializers import STACItemSerializer

        data = STACItemSerializer(self.item, context={"variable": self.variable}).data
        self.assertEqual(data["assets"][f"{self.variable.slug}_visual"]["imageUnscale"], RANGE)

    def test_the_styling_preview_unscales_over_the_range(self):
        from georiva.core.views.styling import _preview_context

        self.assertEqual(_preview_context(self.variable)["config"]["imageUnscale"], RANGE)

    def test_the_item_preview_page_unscales_over_the_range(self):
        from django.contrib.auth import get_user_model

        self.client.force_login(get_user_model().objects.create_superuser("root", "r@x.org", "pw"))
        response = self.client.get(reverse("item_preview", args=[self.item.pk]))
        self.assertContains(response, "data-asset-image-unscale='[0.0, 300.0]'")
