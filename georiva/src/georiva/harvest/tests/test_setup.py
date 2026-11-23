"""
Setting a Harvest feed up: the Variables it creates come in the order the
operator chose them, so a harvested collection lists them that way (#763).
"""

from django.test import TestCase

from georiva.core.models import Catalog, Collection, Unit, Variable
from georiva.harvest.setup import FeedPlan, PlannedVariable, provision_harvest_feed
from georiva.organisations.testing import make_organisation


class SetupOrdersVariablesAsPlannedTests(TestCase):
    def setUp(self):
        self.organisation = make_organisation()
        self.kelvin, _ = Unit.objects.get_or_create(name="Kelvin", symbol="K")

    def planned(self, key, name):
        return PlannedVariable(asset_key=key, name=name, unit_id=self.kelvin.pk, value_min=0, value_max=1)

    def plan(self, *variables, catalog_id=None):
        return FeedPlan(
            name="AIFS",
            upstream_url="https://stac.upstream.example/api",
            upstream_collection_id="aifs",
            variables=list(variables),
            catalog_id=catalog_id,
            new_catalog_name="" if catalog_id else "Forecasts",
        )

    def test_variables_take_the_order_they_were_chosen_in(self):
        provision_harvest_feed(
            self.organisation,
            self.plan(
                self.planned("t2m", "2 metre temperature"),
                self.planned("msl", "Mean sea level pressure"),
                self.planned("tp", "Accumulated precipitation"),
            ),
        )

        listed = Variable.objects.order_by("sort_order").values_list("slug", "sort_order")
        self.assertEqual(list(listed), [("t2m", 0), ("msl", 1), ("tp", 2)])

    def test_a_variable_already_there_keeps_its_place(self):
        catalog = Catalog.objects.create(
            organisation=self.organisation, name="Forecasts", slug="forecasts", file_format=Catalog.FileFormat.GEOTIFF
        )
        collection = Collection.objects.create(catalog=catalog, name="AIFS", slug="aifs")
        Variable.objects.create(
            collection=collection,
            slug="msl",
            name="Curated pressure",
            source_unit=self.kelvin,
            unit=self.kelvin,
            value_min=0,
            value_max=1,
            sort_order=7,
        )

        provision_harvest_feed(
            self.organisation,
            self.plan(
                self.planned("t2m", "2 metre temperature"),
                self.planned("msl", "Pressure"),
                catalog_id=catalog.pk,
            ),
        )

        self.assertEqual(Variable.objects.get(slug="msl").sort_order, 7)
        self.assertEqual(Variable.objects.get(slug="t2m").sort_order, 0)
