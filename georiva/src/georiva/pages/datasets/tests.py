"""
Public dataset pages must not surface `internal` collections — they are
derivation intermediates, read by the engine but never served.
"""
import json

from django.test import TestCase, override_settings
from django.urls import reverse

from georiva.core.models import Asset, Catalog, Collection
from georiva.core.testing import ANALYST_STOPS, make_style
from georiva.organisations.provisioning import provision_organisation
from georiva.organisations.testing import dial_org, make_org_tree, make_organisation


class DatasetVisibilityTests(TestCase):
    def setUp(self):
        # Dataset pages are per organisation and resolved from the Host.
        dial_org(self.client)
        self.catalog = Catalog.objects.create(organisation=make_organisation(),
            name="CMIP6", slug="cmip6", file_format="geotiff"
        )
        Collection.objects.create(
            catalog=self.catalog, name="Tas", slug="tas",
            visibility=Collection.Visibility.PUBLIC,
        )
        Collection.objects.create(
            catalog=self.catalog, name="Tas anomaly", slug="tas-anomaly",
            visibility=Collection.Visibility.INTERNAL,
        )

    def test_available_dates_404_for_internal(self):
        ok = self.client.get(
            reverse("datasets:collection-available-dates",
                    args=["cmip6", "tas"])
        )
        self.assertEqual(ok.status_code, 200)

        hidden = self.client.get(
            reverse("datasets:collection-available-dates",
                    args=["cmip6", "tas-anomaly"])
        )
        self.assertEqual(hidden.status_code, 404)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class ItemDetailMachinePlaneConfigTests(TestCase):
    """The map's config block, which is where Django hands tenancy to the client.

    The item map talks to MinIO and Martin directly — neither of which can see
    the Host that decided which organisation this page belongs to. So the org
    reaches them only through this JSON block, and two things have to hold: it
    must parse (the Martin URL carries query params, and HTML-escaping their
    ``&`` would break ``JSON.parse`` on a page that otherwise looks fine), and
    every address in it must name *this* portal's organisation.
    """

    @classmethod
    def setUpTestData(cls):
        cls.organisation = provision_organisation(name="Kenya Met", slug="kenya")
        from georiva.pages.datasets.models import DatasetsIndexPage

        cls.index = DatasetsIndexPage.objects.descendant_of(
            cls.organisation.site.root_page
        ).get()

        tree = make_org_tree(cls.organisation)
        cls.catalog = tree["catalog"]
        cls.collection = tree["collection"]
        cls.item = tree["item"]
        cls.variable = tree["variable"]
        # The map only offers the choropleth for a collection that has levels,
        # and only lists an item that has a COG for the active variable.
        Collection.objects.filter(pk=cls.collection.pk).update(boundary_stats_levels=[1])
        cls.collection.refresh_from_db()
        Asset.objects.filter(item=cls.item).update(format=Asset.Format.COG)
        from georiva.core.models import Item

        Item.objects.filter(pk=cls.item.pk).update(bounds=[20.0, -12.0, 52.0, 23.0])
        cls.item.refresh_from_db()

    def setUp(self):
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    def _config(self):
        url = f"{self.index.url}{self.catalog.slug}/{self.collection.slug}/items/{self.item.pk}/"
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200, url)
        body = response.content.decode()
        start = body.index('id="grItemConfig"')
        start = body.index(">", start) + 1
        raw = body[start:body.index("</script>", start)]
        return json.loads(raw)

    def test_the_config_block_is_parseable_json(self):
        """The `&` in the Martin URL must survive as `&`, not as `&amp;`."""
        self.assertIn("martinBoundaryStatsUrl", self._config())

    def test_the_martin_url_names_this_portals_organisation(self):
        url = self._config()["martinBoundaryStatsUrl"]
        self.assertIn("/martin/boundary_stats/{z}/{x}/{y}?", url)
        self.assertIn("org=kenya", url)
        self.assertIn(f"catalog={self.catalog.slug}", url)
        self.assertIn(f"collection={self.collection.slug}", url)

    def test_texture_urls_are_injected_per_variable(self):
        """The map reads server-built encoded-texture URLs (ADR 0021) — it
        never rebuilds the machine-plane grammar client-side."""
        config = self._config()
        self.assertIn(self.variable.slug, config["textureUrls"])
        url = config["textureUrls"][self.variable.slug]
        self.assertIn(
            f"/titiler/kenya/{self.catalog.slug}/{self.collection.slug}/"
            f"{self.variable.slug}/encoded-preview.png?", url,
        )
        self.assertIn("v=", url)

    def test_item_bounds_place_the_overlay(self):
        """The texture is georeferenced by the item's own bounds, not the
        collection extent (a derived collection may have none at all)."""
        self.assertEqual(self._config()["itemBounds"], [20.0, -12.0, 52.0, 23.0])

    def test_the_map_serves_the_default_style_only(self):
        """Dataset pages stay on the default style (ADR 0023): an alternate
        named style must not change the palette the map layer carries."""
        make_style(
            self.variable, "official",
            stops=[{"value": 0.0, "color": "#000000"},
                   {"value": 50.0, "color": "#ffffff"}],
        )
        make_style(self.variable, "analyst", is_default=False, stops=ANALYST_STOPS)
        url = f"{self.index.url}{self.catalog.slug}/{self.collection.slug}/items/{self.item.pk}/"
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        layers = {
            layer["slug"]: layer for layer in response.context["map_layers"]
        }
        self.assertEqual(
            layers[self.variable.slug]["palette"],
            [[0.0, [0, 0, 0]], [50.0, [255, 255, 255]]],
        )
