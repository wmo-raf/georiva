"""`datasets/0002` predicates, exercised on a multi-organisation instance.

The migration was written when an instance had exactly one `DatasetsIndexPage`,
so it reasoned about the model instance-wide. Every provisioned organisation now
has one, and the migration is responsible for exactly one of them: the index
under the first `HomePage` (the tree the central org later adopts). These tests
pin it to that page — it must not read another tenant's index as evidence its
own work is done, and must not delete one.
"""
import importlib

from django.test import TestCase, override_settings

from georiva.organisations.provisioning import provision_organisation
from georiva.pages.datasets.models import DatasetsIndexPage
from georiva.pages.home.models import HomePage

migration = importlib.import_module(
    "georiva.pages.datasets.migrations.0002_create_datasets_page"
)


# `apps` and `schema_editor` go unused: the migration imports the concrete
# models rather than reading them off `apps`, so there is nothing to pass.
def run_forward():
    migration.create_datasets_index_page(None, None)


def run_reverse():
    # Read off the operation rather than a named function — the reverse is
    # `RunPython.noop`, and what these tests pin is what a rollback does.
    migration.Migration.operations[0].reverse_code(None, None)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class DatasetsIndexMigrationTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        # The instance as the migrations leave it, plus one ordinary tenant.
        cls.central_home = HomePage.objects.get(slug="home")
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")

    def central_index(self):
        return DatasetsIndexPage.objects.child_of(self.central_home)

    def kenya_index(self):
        return DatasetsIndexPage.objects.child_of(self.kenya.site.root_page)

    def test_the_migration_targets_the_central_home_page(self):
        """What the rest of these tests assume: `first()` is the central home."""
        self.assertEqual(HomePage.objects.first(), self.central_home)

    def test_forward_is_idempotent_on_its_own_page(self):
        run_forward()

        self.assertEqual(self.central_index().count(), 1)

    def test_forward_rebuilds_its_own_index_though_another_org_has_one(self):
        """Another tenant's index is not evidence the first HomePage has one."""
        self.central_index().delete()

        run_forward()

        self.assertEqual(self.central_index().count(), 1)
        self.assertEqual(self.kenya_index().count(), 1)

    def test_reverse_deletes_nothing(self):
        """By rollback time these pages are edited content, on every tenant."""
        run_reverse()

        self.assertEqual(self.central_index().count(), 1)
        self.assertEqual(self.kenya_index().count(), 1)
