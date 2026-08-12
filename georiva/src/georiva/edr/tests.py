"""
EDR serving must expose only `public` collections — `internal` derivation
intermediates are read by the engine but never served.
"""

from django.test import TestCase
from django.urls import reverse
from georiva.core.models import Catalog, Collection
from georiva.core.testing import ANALYST_STOPS, make_style
from georiva.organisations.testing import dial_org, make_organisation


class EDRVisibilityTests(TestCase):
    def setUp(self):
        # The API roots are per organisation and resolved from the Host, so the
        # client has to dial the org that owns these fixtures.
        dial_org(self.client)
        self.catalog = Catalog.objects.create(
            organisation=make_organisation(), name="CMIP6", slug="cmip6", file_format="geotiff"
        )
        self.public = Collection.objects.create(
            catalog=self.catalog,
            name="Tas",
            slug="tas",
            visibility=Collection.Visibility.PUBLIC,
        )
        self.internal = Collection.objects.create(
            catalog=self.catalog,
            name="Tas anomaly",
            slug="tas-anomaly",
            visibility=Collection.Visibility.INTERNAL,
        )

    def test_collection_list_excludes_internal(self):
        response = self.client.get(reverse("edr:collection-list"))
        self.assertEqual(response.status_code, 200)

        ids = {c["id"] for c in response.json()["collections"]}
        self.assertIn("tas", ids)
        self.assertNotIn("tas-anomaly", ids)

    def test_collection_detail_404_for_internal(self):
        ok = self.client.get(reverse("edr:collection-detail", args=["tas"]))
        self.assertEqual(ok.status_code, 200)

        hidden = self.client.get(reverse("edr:collection-detail", args=["tas-anomaly"]))
        self.assertEqual(hidden.status_code, 404)


class EDRStyledParameterTests(TestCase):
    """The ``x-georiva`` block keeps the legacy palette vocabulary across the
    palette→style migration (ADR 0022): same keys, same type words."""

    def setUp(self):
        dial_org(self.client)
        from georiva.organisations.testing import make_org_tree

        tree = make_org_tree(make_organisation())
        self.variable = tree["variable"]
        tree["collection"].visibility = Collection.Visibility.PUBLIC
        tree["collection"].save()

    def _x_georiva(self):
        response = self.client.get(reverse("edr:collection-detail", args=[self.variable.collection.slug]))
        self.assertEqual(response.status_code, 200)
        return response.json()["parameter_names"][self.variable.slug]["x-georiva"]

    def test_the_default_styles_snapshot_serves_as_the_palette(self):
        from georiva.core.models import ColorRamp, VariableStyle

        ramp = ColorRamp.objects.create(name="Categories", ramp_type=ColorRamp.RampType.QUALITATIVE)
        VariableStyle.objects.create(
            variable=self.variable,
            name="Official",
            slug="official",
            is_default=True,
            ramp=ramp,
            stops=[{"value": 0.0, "color": "#000000"}, {"value": 50.0, "color": "#ffffff"}],
        )
        x_georiva = self._x_georiva()
        self.assertEqual(x_georiva["palette"], [[0.0, [0, 0, 0]], [50.0, [255, 255, 255]]])
        self.assertEqual(x_georiva["palette_min"], 0.0)
        self.assertEqual(x_georiva["palette_max"], 50.0)
        self.assertEqual(x_georiva["palette_name"], "Official")
        # Clients predate the ramp catalog, whose "qualitative" they knew as
        # "categorical" — the key speaks their vocabulary.
        self.assertEqual(x_georiva["palette_type"], "categorical")

    def test_a_styleless_variable_still_serves_the_grayscale_fallback(self):
        x_georiva = self._x_georiva()
        self.assertEqual(len(x_georiva["palette"]), 11)
        self.assertEqual(x_georiva["palette_min"], self.variable.value_min)
        self.assertNotIn("palette_name", x_georiva)

    def test_an_alternate_style_leaves_the_output_on_the_default(self):
        """EDR serves the default style only (ADR 0023): a second named style
        must change nothing — same palette, same keys, and no index of the
        styles the machine plane knows about."""
        make_style(
            self.variable,
            "official",
            stops=[{"value": 0.0, "color": "#000000"}, {"value": 50.0, "color": "#ffffff"}],
        )
        make_style(self.variable, "analyst", is_default=False, stops=ANALYST_STOPS)
        response = self.client.get(reverse("edr:collection-detail", args=[self.variable.collection.slug]))
        parameter = response.json()["parameter_names"][self.variable.slug]
        x_georiva = parameter["x-georiva"]
        self.assertEqual(x_georiva["palette"], [[0.0, [0, 0, 0]], [50.0, [255, 255, 255]]])
        self.assertEqual(x_georiva["palette_name"], "Official")
        self.assertNotIn("styles", x_georiva)
        self.assertNotIn("renders", parameter)
