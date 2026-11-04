"""
EDR serving must expose only `public` collections — `internal` derivation
intermediates are read by the engine but never served.
"""

from datetime import UTC, datetime, timedelta

from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Catalog, Collection, Item
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

    def test_collection_advertises_no_data_query_it_cannot_serve(self):
        # EDR is metadata-only: a client reads data_queries to decide what a
        # collection supports, so listing one with no route sends it to a 404.
        body = self.client.get(reverse("edr:collection-detail", args=["tas"])).json()
        self.assertEqual(body["data_queries"], {})


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


class EDRRunsBoundTests(TestCase):
    """``x-georiva.runs`` carries a *bounded* window of recent runs (#600).

    One item row exists per ``(reference_time, time)``, so an unbounded runs
    list grows as runs × lead times — on a collection that retains its past
    forecasts that is thousands of entries in a document a viewer re-fetches on
    every model switch. The document answers with the newest few runs and says
    how many it was willing to send; a client that wants the rest goes to
    ``/api/runs/``, which is linked from the document.
    """

    REF = datetime(2026, 9, 2, 0, 0, tzinfo=UTC)

    def setUp(self):
        dial_org(self.client)
        self.catalog = Catalog.objects.create(
            organisation=make_organisation(), name="Models", slug="models", file_format="grib2"
        )
        self.collection = Collection.objects.create(
            catalog=self.catalog,
            name="IFS surface",
            slug="ifs-surface",
            visibility=Collection.Visibility.PUBLIC,
            is_forecast=True,
        )

    def _runs(self, count, lead_times=3):
        """``count`` runs twelve hours apart, each with ``lead_times`` valid times."""
        for run in range(count):
            reference_time = self.REF + timedelta(hours=12 * run)
            for lead in range(lead_times):
                Item.objects.create(
                    collection=self.collection,
                    reference_time=reference_time,
                    time=reference_time + timedelta(hours=6 * lead),
                )

    def _body(self, **params):
        url = reverse("edr:collection-detail", args=[self.collection.slug])
        response = self.client.get(url, params)
        self.assertEqual(response.status_code, 200)
        return response.json()

    def _reference_times(self, body):
        return [run["reference_time"] for run in body["x-georiva"]["runs"]]

    def test_only_the_newest_few_runs_are_served_by_default(self):
        self._runs(8)
        body = self._body()
        served = self._reference_times(body)
        self.assertEqual(len(served), 5)
        newest = self.REF + timedelta(hours=12 * 7)
        self.assertEqual(served[0], newest.isoformat())
        # Newest first, and the window is the *newest* five, not the first five.
        self.assertEqual(served, sorted(served, reverse=True))
        self.assertEqual(served[-1], (self.REF + timedelta(hours=12 * 3)).isoformat())
        self.assertEqual(body["x-georiva"]["runs_limit"], 5)

    def test_each_served_run_still_carries_all_its_valid_times(self):
        self._runs(2, lead_times=4)
        run = self._body()["x-georiva"]["runs"][0]
        newest = self.REF + timedelta(hours=12)
        self.assertEqual(
            run["valid_times"],
            [(newest + timedelta(hours=6 * lead)).isoformat() for lead in range(4)],
        )

    def test_a_client_may_ask_for_more_or_fewer_runs(self):
        self._runs(8)
        self.assertEqual(len(self._reference_times(self._body(runs_limit=2))), 2)
        self.assertEqual(len(self._reference_times(self._body(runs_limit=8))), 8)

    def test_a_runs_limit_that_makes_no_sense_falls_back_to_a_bound(self):
        self._runs(3)
        self.assertEqual(len(self._reference_times(self._body(runs_limit="all"))), 3)
        self.assertEqual(len(self._reference_times(self._body(runs_limit=0))), 1)
        self.assertEqual(len(self._reference_times(self._body(runs_limit=-4))), 1)

    def test_the_runs_limit_is_capped_server_side(self):
        self._runs(3)
        body = self._body(runs_limit=10_000)
        self.assertEqual(body["x-georiva"]["runs_limit"], 200)
        self.assertEqual(len(self._reference_times(body)), 3)

    def test_the_document_points_at_the_route_that_knows_every_run(self):
        self._runs(2)
        hrefs = [link["href"] for link in self._body()["links"]]
        route = reverse("run-list", args=[self.catalog.slug, self.collection.slug])
        self.assertTrue(any(href.endswith(route) for href in hrefs), hrefs)

    def test_a_collection_without_runs_gains_neither_runs_nor_a_bound(self):
        Item.objects.create(collection=self.collection, time=self.REF)
        x_georiva = self._body()["x-georiva"]
        self.assertNotIn("runs", x_georiva)
        self.assertNotIn("runs_limit", x_georiva)

    def test_the_flat_temporal_values_stay_the_latest_run_only(self):
        self._runs(3, lead_times=2)
        values = self._body()["extent"]["temporal"]["values"]
        newest = self.REF + timedelta(hours=24)
        self.assertEqual(values, [newest.isoformat(), (newest + timedelta(hours=6)).isoformat()])
