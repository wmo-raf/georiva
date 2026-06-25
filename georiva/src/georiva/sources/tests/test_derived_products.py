"""
DataFeed's derived-product declaration hook (ADR-0008, issue #143).

A feed declares the derived products it offers via get_derived_products(); the
base default is none. Plugins override to return DerivedProductDefinitions bound
to their configured collections.
"""
from django.test import TestCase

from georiva.core.models import Catalog
from georiva.sources.models import DataFeed


class GetDerivedProductsTests(TestCase):
    def test_defaults_to_empty_list(self):
        catalog = Catalog.objects.create(name="CHIRPS", slug="chirps", file_format="geotiff")
        feed = DataFeed.objects.create(name="Feed", catalog=catalog)

        self.assertEqual(feed.get_derived_products(), [])
