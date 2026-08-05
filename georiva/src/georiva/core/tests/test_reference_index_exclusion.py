"""Machine-generated data-plane models are excluded from Wagtail's reference
index (``wagtail_reference_index_ignore = True``).

These models are registered as snippets for their admin list views, but that
registration otherwise enrols them in the reference index — so every save (of
which the pipelines do thousands) would synchronously run Wagtail's
``update_reference_index_task`` in-process. They are machine data, not editorial
content whose "usage" or delete-protection matters, so they opt out.
"""
from django.test import SimpleTestCase
from wagtail.models import ReferenceIndex

from georiva.core.models import Item
from georiva.ingestion.models import FileIngestionJob
from georiva.staging.models import StagingCollection, StagingItem


class ReferenceIndexExclusionTests(SimpleTestCase):
    def test_data_plane_models_are_not_reference_indexed(self):
        for model in (Item, StagingItem, StagingCollection, FileIngestionJob):
            with self.subTest(model=model.__name__):
                self.assertFalse(
                    ReferenceIndex.is_indexed(model),
                    f"{model.__name__} should be excluded from the reference index",
                )

    def test_the_flag_is_declared_on_each_model(self):
        # A guard against the exclusion silently regressing to the Wagtail
        # default (indexed) if the attribute is removed.
        for model in (Item, StagingItem, StagingCollection, FileIngestionJob):
            with self.subTest(model=model.__name__):
                self.assertTrue(getattr(model, "wagtail_reference_index_ignore", False))
