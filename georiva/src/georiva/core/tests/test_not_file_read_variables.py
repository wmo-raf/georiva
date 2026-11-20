"""A Variable not read from files needs no sources.

Only a Variable read out of ingested files names its bands (`sources`), a
transform over them, and the unit they arrive in. A harvested collection's
Variables are reached through the feed's Variable mapping (ADR 0042 §7), and a
recipe's output Variables are written by the recipe (ADR 0010 §2) — so neither
must be refused for having no source, and the collection editor does not offer
those fields for them. Each says so by its Provenance: ``harvested_by``, or
``derived_by_recipe`` (#711).
"""

import importlib

from django.apps import apps
from django.core.exceptions import ValidationError
from django.test import TestCase
from django.urls import reverse
from wagtail.test.utils.form_data import inline_formset, nested_form_data

from georiva.core.derived_products import DerivedProductDefinition, OutputRef
from georiva.core.models import Catalog, Collection, Unit, Variable
from georiva.core.models.collection import FILE_READ_VARIABLE_FIELDS
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.testing import dial_org, make_organisation
from georiva.organisations.tests.factories import PASSWORD, grant_everything, make_user
from georiva.processing.recipes.climatology import ClimatologyRecipe
from georiva.sources.models import DataFeed, DerivedProduct, DerivedProductOutput
from georiva.sources.product_service import materialise_output_collections

backfill = importlib.import_module("georiva.core.migrations.0022_collection_derived_by_recipe")


class NotFileReadFixture:
    """Three collections in one catalog: one read from files, one harvested,
    one a derived product's output — each with one Variable that has no sources."""

    @classmethod
    def setUpTestData(cls):
        cls.org = make_organisation()
        cls.catalog = Catalog.objects.create(
            organisation=cls.org, name="Models", slug="models", file_format=Catalog.FileFormat.GEOTIFF
        )
        cls.feed = DataFeed.objects.create(name="Feed", catalog=cls.catalog)
        cls.unit, _ = Unit.objects.get_or_create(name="Celsius", symbol="C")

        cls.file_read = Collection.objects.create(catalog=cls.catalog, name="Files", slug="files")
        cls.harvested = Collection.objects.create(
            catalog=cls.catalog, name="Harvested", slug="harvested", harvested_by=cls.feed
        )
        cls.derived = Collection.objects.create(
            catalog=cls.catalog, name="Derived", slug="derived", derived_by_recipe="climatology"
        )

    def sourceless(self, collection, slug="tas"):
        return Variable(collection=collection, name=slug, slug=slug, source_unit=self.unit, unit=self.unit)


class ReadsVariablesFromFilesTests(NotFileReadFixture, TestCase):
    def test_a_plain_collection_reads_from_files(self):
        self.assertTrue(self.file_read.reads_variables_from_files)

    def test_a_harvested_collection_does_not(self):
        self.assertFalse(self.harvested.reads_variables_from_files)

    def test_a_recipes_output_does_not(self):
        self.assertFalse(self.derived.reads_variables_from_files)

    def test_a_binding_alone_is_not_provenance(self):
        """A product bound to a collection it did not create — an operator's,
        say — leaves that collection's Variables file-read (#711)."""
        product = DerivedProduct.objects.create(
            data_feed=self.feed, definition_key="clim", recipe_type="climatology", title="Climatology"
        )
        DerivedProductOutput.objects.create(
            product=product, role="value", output_key="files", collection=self.file_read
        )

        self.assertTrue(self.file_read.reads_variables_from_files)

    def test_an_unsaved_collection_reads_from_files(self):
        self.assertTrue(Collection(catalog=self.catalog, name="New", slug="new").reads_variables_from_files)


class VariableCleanTests(NotFileReadFixture, TestCase):
    def test_a_file_read_variable_still_needs_a_source(self):
        with self.assertRaises(ValidationError) as ctx:
            self.sourceless(self.file_read).clean()
        self.assertIn("sources", ctx.exception.message_dict)

    def test_a_harvested_variable_needs_none(self):
        self.sourceless(self.harvested).clean()

    def test_a_derived_output_variable_needs_none(self):
        self.sourceless(self.derived).clean()

    def test_the_range_rule_still_holds_for_a_harvested_variable(self):
        variable = self.sourceless(self.harvested)
        variable.value_min, variable.value_max = 10.0, 10.0
        with self.assertRaises(ValidationError) as ctx:
            variable.clean()
        self.assertIn("value_max", ctx.exception.message_dict)


class CollectionEditorTests(NotFileReadFixture, TestCase):
    """The collection editor, driven through the admin as an operator would."""

    def setUp(self):
        user = grant_everything(make_user("amina"))
        OrganisationMembership.objects.create(user=user, organisation=self.org, role=OrganisationMembership.Role.ADMIN)
        dial_org(self.client)
        self.client.login(username="amina", password=PASSWORD)

    def edit_url(self, collection):
        return reverse("collection:edit", args=[collection.pk])

    def posted(self, collection, variables):
        return nested_form_data(
            {
                "name": collection.name,
                "catalog": collection.catalog_id,
                "description": "",
                "bounds": "",
                "crs": collection.crs,
                "time_resolution": "",
                "is_active": "on",
                "visibility": collection.visibility,
                "sort_order": 0,
                "is_forecast": "on",
                "forecast_horizon_hours": "",
                "boundary_stats_levels": [],
                "slug": collection.slug,
                "variables": inline_formset(variables, initial=sum(1 for row in variables if row.get("id"))),
            }
        )

    def row(self, variable=None, slug="tas"):
        return {
            "id": variable.pk if variable else "",
            "name": variable.name if variable else slug,
            "slug": variable.slug if variable else slug,
            "is_active": "on",
            "description": "",
            "source_unit": self.unit.pk,
            "unit": self.unit.pk,
        }

    def errors(self, response):
        if response.context is None or "form" not in response.context:
            return ""
        form = response.context["form"]
        return " ".join([str(form.errors)] + [str(row.errors) for row in form.formsets["variables"].forms])

    def test_a_harvested_collection_saves_with_its_sourceless_variable(self):
        variable = Variable.objects.create(
            collection=self.harvested, name="tas", slug="tas", source_unit=self.unit, unit=self.unit
        )

        response = self.client.post(self.edit_url(self.harvested), self.posted(self.harvested, [self.row(variable)]))

        self.assertEqual(response.status_code, 302, self.errors(response))
        self.harvested.refresh_from_db()
        self.assertTrue(self.harvested.is_forecast)

    def test_a_variable_added_to_a_harvested_collection_needs_no_source(self):
        response = self.client.post(self.edit_url(self.harvested), self.posted(self.harvested, [self.row(slug="pr")]))

        self.assertEqual(response.status_code, 302, self.errors(response))
        self.assertTrue(self.harvested.variables.filter(slug="pr").exists())

    def test_a_file_read_collection_still_refuses_a_sourceless_variable(self):
        variable = Variable.objects.create(
            collection=self.file_read, name="tas", slug="tas", source_unit=self.unit, unit=self.unit
        )
        row = self.row(variable) | {"transform_type": Variable.TransformType.PASSTHROUGH, "source_unit": ""}
        row["sources"] = {"count": 0}  # an empty stream

        response = self.client.post(self.edit_url(self.file_read), self.posted(self.file_read, [row]))

        self.assertEqual(response.status_code, 200)
        self.assertIn("At least one source must be defined", self.errors(response))

    def test_the_file_reading_fields_are_not_offered_on_a_harvested_or_derived_collection(self):
        for collection in (self.harvested, self.derived):
            Variable.objects.create(
                collection=collection, name="tas", slug="tas", source_unit=self.unit, unit=self.unit
            )
            with self.subTest(collection=collection.slug):
                form = self.client.get(self.edit_url(collection)).context["form"]
                formset = form.formsets["variables"]
                for child in [*formset.forms, formset.empty_form]:
                    for name in FILE_READ_VARIABLE_FIELDS:
                        self.assertNotIn(name, child.fields)

    def test_they_are_still_offered_on_a_file_read_collection(self):
        Variable.objects.create(
            collection=self.file_read, name="tas", slug="tas", source_unit=self.unit, unit=self.unit
        )

        form = self.client.get(self.edit_url(self.file_read)).context["form"]

        for child in [*form.formsets["variables"].forms, form.formsets["variables"].empty_form]:
            for name in FILE_READ_VARIABLE_FIELDS:
                self.assertIn(name, child.fields)


class DerivationProvenanceTests(NotFileReadFixture, TestCase):
    """Who stamps ``derived_by_recipe``, and only on a row they create."""

    def definition(self, slug, recipe_type="climatology"):
        return DerivedProductDefinition(
            key="clim",
            recipe_type=recipe_type,
            label="Clim",
            description="",
            config_schema=(),
            inputs=(),
            outputs=(OutputRef(role="climatology", collection=slug),),
            trigger_mode="scheduled",
        )

    def test_materialising_a_product_stamps_the_output_it_creates(self):
        (collection,) = materialise_output_collections(self.feed, self.definition("normals"))

        self.assertEqual(collection.derived_by_recipe, "climatology")
        self.assertFalse(collection.reads_variables_from_files)

    def test_a_promotion_output_is_not_stamped(self):
        """Promotion copies into the feed's published collection, whose
        Variables are file-read and keep their sources — the #710 regression."""
        (collection,) = materialise_output_collections(self.feed, self.definition("published", "promotion"))

        self.assertEqual(collection.derived_by_recipe, "")
        self.assertTrue(collection.reads_variables_from_files)

    def test_an_unregistered_recipes_output_is_not_stamped(self):
        (collection,) = materialise_output_collections(self.feed, self.definition("orphan", "no-such-recipe"))

        self.assertEqual(collection.derived_by_recipe, "")

    def test_materialising_onto_an_existing_collection_leaves_it_as_it_is(self):
        (collection,) = materialise_output_collections(self.feed, self.definition("files"))

        self.assertEqual(collection.pk, self.file_read.pk)
        self.assertEqual(collection.derived_by_recipe, "")

    def test_the_core_climatology_recipe_stamps_the_collection_it_creates(self):
        unit = {"source_collection": "tas", "season": "DJF", "quantity": "value", "baseline": None}

        collection = ClimatologyRecipe()._published_collection(unit, self.catalog)

        self.assertEqual(collection.derived_by_recipe, "climatology")
        self.assertFalse(collection.reads_variables_from_files)

    def test_the_backfill_stamps_a_bound_output_with_its_products_recipe(self):
        bound = Collection.objects.create(catalog=self.catalog, name="Bound", slug="bound")
        product = DerivedProduct.objects.create(
            data_feed=self.feed, definition_key="anom", recipe_type="chirps-anomaly", title="Anomaly"
        )
        DerivedProductOutput.objects.create(product=product, role="anomaly", output_key="bound", collection=bound)

        backfill.stamp_bound_outputs(apps, None)

        bound.refresh_from_db()
        self.assertEqual(bound.derived_by_recipe, "chirps-anomaly")
        self.file_read.refresh_from_db()
        self.assertEqual(self.file_read.derived_by_recipe, "")
        self.derived.refresh_from_db()
        self.assertEqual(self.derived.derived_by_recipe, "climatology")

    def test_the_backfill_leaves_a_promotion_output_unstamped(self):
        published = Collection.objects.create(catalog=self.catalog, name="Published", slug="published")
        product = DerivedProduct.objects.create(
            data_feed=self.feed, definition_key="promote", recipe_type="promotion", title="Promotion"
        )
        DerivedProductOutput.objects.create(
            product=product, role="served", output_key="published", collection=published
        )

        backfill.stamp_bound_outputs(apps, None)

        published.refresh_from_db()
        self.assertEqual(published.derived_by_recipe, "")
        self.assertTrue(published.reads_variables_from_files)
