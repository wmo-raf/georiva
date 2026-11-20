"""
EDR serving must expose only `public` collections — `internal` derivation
intermediates are read by the engine but never served.
"""

from datetime import UTC, datetime, timedelta

from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.urls import reverse

from georiva.core.models import Catalog, Collection, Item
from georiva.core.testing import ANALYST_STOPS, make_style
from georiva.organisations.testing import dial_org, make_organisation

from .ids import edr_collection_id


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
        self.assertIn("cmip6.tas", ids)
        self.assertNotIn("cmip6.tas-anomaly", ids)

    def test_collection_detail_404_for_internal(self):
        ok = self.client.get(reverse("edr:collection-detail", args=["cmip6.tas"]))
        self.assertEqual(ok.status_code, 200)

        hidden = self.client.get(reverse("edr:collection-detail", args=["cmip6.tas-anomaly"]))
        self.assertEqual(hidden.status_code, 404)

    def test_collection_advertises_no_data_query_it_cannot_serve(self):
        # EDR is metadata-only: a client reads data_queries to decide what a
        # collection supports, so listing one with no route sends it to a 404.
        body = self.client.get(reverse("edr:collection-detail", args=["cmip6.tas"])).json()
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
        response = self.client.get(reverse("edr:collection-detail", args=[edr_collection_id(self.variable.collection)]))
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

    def test_a_parameter_names_the_served_unit_not_the_source_unit(self):
        """Values are stored as produced and served converted (ADR 0046): the
        parameter's unit is what a reader of the API is given."""
        from georiva.core.models import Unit

        self.variable.unit, _ = Unit.objects.get_or_create(symbol="°C", defaults={"name": "test °C"})
        self.variable.correct_source_unit(Unit.objects.get_or_create(symbol="K", defaults={"name": "test K"})[0])
        self.variable.save()

        response = self.client.get(reverse("edr:collection-detail", args=[edr_collection_id(self.variable.collection)]))
        parameter = response.json()["parameter_names"][self.variable.slug]
        self.assertEqual(parameter["unit"]["symbol"], "°C")

    def test_a_parameters_value_range_is_in_the_unit_it_names(self):
        """A number that leaves as a number is served (ADR 0046 §15): the range
        beside a °C label is in °C, though it is stored in K."""
        from georiva.core.models import Unit

        self.variable.unit, _ = Unit.objects.get_or_create(symbol="°C", defaults={"name": "test °C"})
        self.variable.correct_source_unit(Unit.objects.get_or_create(symbol="K", defaults={"name": "test K"})[0])
        self.variable.value_min, self.variable.value_max = 233.15, 323.15
        self.variable.save()

        x_georiva = self._x_georiva()
        self.assertEqual((x_georiva["value_min"], x_georiva["value_max"]), (-40.0, 50.0))

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
        response = self.client.get(reverse("edr:collection-detail", args=[edr_collection_id(self.variable.collection)]))
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
        url = reverse("edr:collection-detail", args=[edr_collection_id(self.collection)])
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
        self.assertEqual(body["x-georiva"]["runs_limit"], 20)
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

    def test_the_query_does_not_grow_with_the_runs_behind_it(self):
        # The window is two bounded queries, not one scan per run: a collection
        # that retains a month of forecasts costs what a fresh one costs.
        self._runs(2)
        with CaptureQueriesContext(connection) as small:
            self._body()
        Item.objects.all().delete()
        self._runs(12)
        with CaptureQueriesContext(connection) as large:
            self._body()
        self.assertEqual(len(large), len(small))


class EDRQualifiedCollectionIdTests(TestCase):
    """An EDR collection id is ``{catalog}.{collection}`` (#687).

    A collection slug is unique only within its catalog, so one organisation can
    hold two collections of the same slug — two feeds of one plugin in two
    catalogs. The bare slug still answers while it names one collection, by
    redirecting to the qualified id; when it names more, a 404 lists them.
    """

    def setUp(self):
        dial_org(self.client)
        organisation = make_organisation()
        self.gfs = Catalog.objects.create(organisation=organisation, name="GFS", slug="gfs", file_format="grib2")
        self.mirror = Catalog.objects.create(
            organisation=organisation, name="GFS mirror", slug="gfs-mirror", file_format="grib2"
        )
        self.forecast = Collection.objects.create(
            catalog=self.gfs, name="Forecast", slug="forecast", visibility=Collection.Visibility.PUBLIC
        )
        self.only = Collection.objects.create(
            catalog=self.gfs, name="Analysis", slug="analysis", visibility=Collection.Visibility.PUBLIC
        )

    def _twin(self, visibility=Collection.Visibility.PUBLIC):
        return Collection.objects.create(catalog=self.mirror, name="Forecast", slug="forecast", visibility=visibility)

    def _detail(self, collection_id, **params):
        return self.client.get(reverse("edr:collection-detail", args=[collection_id]), params)

    def test_a_collection_answers_at_its_qualified_id(self):
        response = self._detail("gfs.forecast")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["id"], "gfs.forecast")
        self_link = next(link for link in body["links"] if link["rel"] == "self")
        self.assertTrue(self_link["href"].endswith("/api/edr/collections/gfs.forecast/"), self_link)

    def test_the_list_names_each_collection_by_its_qualified_id(self):
        self._twin()
        response = self.client.get(reverse("edr:collection-list"))
        collections = response.json()["collections"]
        self.assertEqual(
            sorted(c["id"] for c in collections),
            ["gfs-mirror.forecast", "gfs.analysis", "gfs.forecast"],
        )
        for collection in collections:
            hrefs = [link["href"] for link in collection["links"]]
            self.assertTrue(any(href.endswith(f"/collections/{collection['id']}/") for href in hrefs), hrefs)

    def test_two_collections_sharing_a_slug_each_answer_at_their_own_id(self):
        twin = self._twin()
        first = self._detail("gfs.forecast").json()
        second = self._detail("gfs-mirror.forecast").json()
        self.assertEqual(first["x-georiva"]["catalog_slug"], "gfs")
        self.assertEqual(second["x-georiva"]["catalog_slug"], twin.catalog.slug)

    def test_a_bare_slug_naming_two_collections_is_a_404_naming_both(self):
        self._twin()
        response = self._detail("forecast")
        self.assertEqual(response.status_code, 404)
        ids = [candidate["id"] for candidate in response.json()["collections"]]
        self.assertEqual(sorted(ids), ["gfs-mirror.forecast", "gfs.forecast"])

    def test_a_bare_slug_unique_in_the_organisation_redirects_to_its_qualified_id(self):
        response = self._detail("analysis", runs_limit=2)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/api/edr/collections/gfs.analysis/?runs_limit=2")

    def test_a_collection_the_caller_cannot_see_does_not_make_a_slug_ambiguous(self):
        # Nor does the 404 leak the id of a collection this caller may not read.
        self._twin(visibility=Collection.Visibility.INTERNAL)
        response = self._detail("forecast")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/api/edr/collections/gfs.forecast/")

    def test_an_unknown_id_is_a_404(self):
        self.assertEqual(self._detail("nothing").status_code, 404)
        self.assertEqual(self._detail("gfs.nothing").status_code, 404)
        self.assertEqual(self._detail("nothing.forecast").status_code, 404)
        # Not an id at all: no route matches it.
        self.assertEqual(self.client.get("/api/edr/collections/gfs.forecast.extra/").status_code, 404)

    def test_another_organisations_collection_is_never_reached(self):
        other = make_organisation("elsewhere")
        other_catalog = Catalog.objects.create(organisation=other, name="GFS", slug="gfs", file_format="grib2")
        Collection.objects.create(
            catalog=other_catalog, name="Hidden", slug="hidden", visibility=Collection.Visibility.PUBLIC
        )
        Collection.objects.create(
            catalog=other_catalog, name="Their forecast", slug="forecast", visibility=Collection.Visibility.PUBLIC
        )
        # Its qualified id is free on this host, and its slug does not collide here.
        self.assertEqual(self._detail("gfs.hidden").status_code, 404)
        self.assertEqual(self._detail("hidden").status_code, 404)
        self.assertEqual(self._detail("forecast").status_code, 302)
        self.assertEqual(self._detail("gfs.forecast").json()["title"], "Forecast")


class EDRBareSlugSubRouteTests(TestCase):
    """The redirect keeps whatever follows the collection id, so a sub-route
    (``position``, ``area``, …) reached by its bare slug lands on the same
    sub-route of the qualified id."""

    def setUp(self):
        self.organisation = make_organisation()
        catalog = Catalog.objects.create(organisation=self.organisation, name="GFS", slug="gfs", file_format="grib2")
        self.collection = Collection.objects.create(
            catalog=catalog, name="Forecast", slug="forecast", visibility=Collection.Visibility.PUBLIC
        )

    def _resolve(self, path, collection_id):
        from django.contrib.auth.models import AnonymousUser
        from django.test import RequestFactory

        from .views import resolve_edr_collection

        # What OrganisationMiddleware attaches for an anonymous caller.
        request = RequestFactory().get(path)
        request.user = AnonymousUser()
        request.active_org = self.organisation
        request.active_org_role = None
        return resolve_edr_collection(request, collection_id, Collection.objects.all())

    def test_a_sub_route_redirects_with_its_path_and_query(self):
        response = self._resolve("/api/edr/collections/forecast/position/?coords=POINT(1 2)", "forecast")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/api/edr/collections/gfs.forecast/position/?coords=POINT(1%202)")

    def test_a_qualified_id_resolves_to_the_collection(self):
        resolved = self._resolve("/api/edr/collections/gfs.forecast/position/", "gfs.forecast")
        self.assertEqual(resolved, self.collection)
