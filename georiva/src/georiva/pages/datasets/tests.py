"""
Public dataset pages must not surface `internal` collections — they are
derivation intermediates, read by the engine but never served.
"""

import json

from adminboundarymanager.models import AdminBoundary
from django.contrib.gis.geos import MultiPolygon, Polygon
from django.test import TestCase, override_settings
from django.urls import reverse

from georiva.core.models import Asset, Catalog, Collection
from georiva.core.testing import ANALYST_STOPS, make_style
from georiva.organisations.provisioning import provision_organisation
from georiva.organisations.testing import dial_org, make_org_tree, make_organisation


def _boundary(*, level, gid):
    """An admin boundary at one level — geometry is irrelevant here, since the
    page asks only whether rows exist, not where they fall."""
    return AdminBoundary.objects.create(
        name_0="Testland",
        gid_0=gid,
        level=level,
        geom=MultiPolygon(Polygon.from_bbox((-170, -80, 170, 80))),
    )


class DatasetVisibilityTests(TestCase):
    def setUp(self):
        # Dataset pages are per organisation and resolved from the Host.
        dial_org(self.client)
        self.catalog = Catalog.objects.create(
            organisation=make_organisation(), name="CMIP6", slug="cmip6", file_format="geotiff"
        )
        Collection.objects.create(
            catalog=self.catalog,
            name="Tas",
            slug="tas",
            visibility=Collection.Visibility.PUBLIC,
        )
        Collection.objects.create(
            catalog=self.catalog,
            name="Tas anomaly",
            slug="tas-anomaly",
            visibility=Collection.Visibility.INTERNAL,
        )

    def test_available_dates_404_for_internal(self):
        ok = self.client.get(reverse("datasets:collection-available-dates", args=["cmip6", "tas"]))
        self.assertEqual(ok.status_code, 200)

        hidden = self.client.get(reverse("datasets:collection-available-dates", args=["cmip6", "tas-anomaly"]))
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

        cls.index = DatasetsIndexPage.objects.descendant_of(cls.organisation.site.root_page).get()

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
        raw = body[start : body.index("</script>", start)]
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
            f"/titiler/kenya/{self.catalog.slug}/{self.collection.slug}/{self.variable.slug}/encoded-preview.png?",
            url,
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
            self.variable,
            "official",
            stops=[{"value": 0.0, "color": "#000000"}, {"value": 50.0, "color": "#ffffff"}],
        )
        make_style(self.variable, "analyst", is_default=False, stops=ANALYST_STOPS)
        url = f"{self.index.url}{self.catalog.slug}/{self.collection.slug}/items/{self.item.pk}/"
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        layers = {layer["slug"]: layer for layer in response.context["map_layers"]}
        self.assertEqual(
            layers[self.variable.slug]["palette"],
            [[0.0, [0, 0, 0]], [50.0, [255, 255, 255]]],
        )


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class ItemDetailBoundaryAvailabilityTests(TestCase):
    """The Boundaries control promises a choropleth, so it may only appear where
    one can actually be drawn.

    Configuring ``boundary_stats_levels`` states an intent to aggregate; it says
    nothing about whether the run happened, covered this variable, or produced a
    usable number. The page therefore gates the control on rows that exist *and*
    are still wanted, and the assertions below walk each way those two can part
    company.
    """

    @classmethod
    def setUpTestData(cls):
        cls.organisation = provision_organisation(name="Kenya Met", slug="kenya")
        from georiva.pages.datasets.models import DatasetsIndexPage

        cls.index = DatasetsIndexPage.objects.descendant_of(cls.organisation.site.root_page).get()

        tree = make_org_tree(cls.organisation)
        cls.catalog = tree["catalog"]
        cls.collection = tree["collection"]
        cls.item = tree["item"]
        cls.variable = tree["variable"]

        # A second variable, so "aggregated" can be true of one and false of the
        # other — the case a per-collection flag cannot express.
        from georiva.core.models import Variable

        cls.other_variable = Variable.objects.create(
            collection=cls.collection,
            name="Rainfall",
            slug="rainfall",
            unit=cls.variable.unit,
            value_min=0,
            value_max=50,
        )
        Asset.objects.create(item=cls.item, variable=cls.other_variable, href="rain.tif")
        Asset.objects.filter(item=cls.item).update(format=Asset.Format.COG)

        Collection.objects.filter(pk=cls.collection.pk).update(boundary_stats_levels=[1, 2])
        cls.collection.refresh_from_db()

        cls.level_1 = _boundary(level=1, gid="TST1")
        cls.level_2 = _boundary(level=2, gid="TST2")

    def setUp(self):
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    # -- helpers -----------------------------------------------------------

    def _stats(self, variable, boundary, *, mean=1.0):
        from georiva.analysis.zonal_stats.models import BoundaryZonalStats

        return BoundaryZonalStats.objects.create(
            item=self.item,
            variable=variable,
            boundary=boundary,
            time=self.item.time,
            mean=mean,
        )

    def _response(self):
        url = f"{self.index.url}{self.catalog.slug}/{self.collection.slug}/items/{self.item.pk}/"
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200, url)
        return response

    def _availability(self):
        return self._response().context["boundary_levels_by_variable"]

    def _config(self):
        body = self._response().content.decode()
        start = body.index('id="grItemConfig"')
        start = body.index(">", start) + 1
        return json.loads(body[start : body.index("</script>", start)])

    # -- assertions --------------------------------------------------------

    def test_no_aggregates_means_no_boundaries_button(self):
        response = self._response()
        self.assertEqual(response.context["boundary_levels_by_variable"], {})
        self.assertEqual(response.context["boundary_stats_levels"], [])
        self.assertNotIn('data-mode="boundaries"', response.content.decode())

    def test_aggregates_bring_the_button_back(self):
        self._stats(self.variable, self.level_1)
        response = self._response()
        self.assertEqual(response.context["boundary_levels_by_variable"], {self.variable.slug: [1]})
        self.assertIn('data-mode="boundaries"', response.content.decode())

    def test_availability_is_per_variable(self):
        """One variable aggregated, one not — the map must be able to tell them
        apart client-side, since switching variables never reloads the page."""
        self._stats(self.variable, self.level_1)
        self.assertEqual(self._availability(), {self.variable.slug: [1]})

    def test_a_deconfigured_level_is_hidden_even_with_rows(self):
        """Removing a level from the collection is an instruction to stop showing
        it; rows that outlive the edit must not override the admin."""
        self._stats(self.variable, self.level_1)
        self._stats(self.variable, self.level_2)
        Collection.objects.filter(pk=self.collection.pk).update(boundary_stats_levels=[1])

        self.assertEqual(self._availability(), {self.variable.slug: [1]})

    def test_rows_with_no_usable_value_do_not_count(self):
        """A run that covered nothing leaves rows behind but nothing to draw, so
        the button would open a uniformly blank choropleth."""
        self._stats(self.variable, self.level_1, mean=None)

        self.assertEqual(self._availability(), {})

    def test_levels_are_reported_in_order(self):
        self._stats(self.variable, self.level_2)
        self._stats(self.variable, self.level_1)

        self.assertEqual(self._availability(), {self.variable.slug: [1, 2]})

    def test_the_config_block_survives_a_populated_availability_map(self):
        """The map reads availability out of the JSON config, so the block has
        to parse once it carries any.

        An empty dict renders as `{}` and parses whatever the template does with
        it; only a populated one has quoted keys to autoescape into `&quot;`, and
        that breaks JSON.parse for the whole block — the map never initialises,
        not just the boundary control.
        """
        self._stats(self.variable, self.level_1)

        self.assertEqual(self._config()["boundaryLevelsByVariable"], {self.variable.slug: [1]})
