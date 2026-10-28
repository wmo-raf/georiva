"""ADR 0034: every catalog has at most one feed, pull or push."""

from django.core.exceptions import ValidationError
from django.test import TestCase

from georiva.core.models import Catalog
from georiva.ingestion.models import PushFeed
from georiva.organisations.testing import make_organisation
from georiva.sources.models import DataFeed


def _catalog(slug="cat"):
    return Catalog.objects.create(organisation=make_organisation(), name=slug, slug=slug, file_format="grib2")


class OneFeedPerCatalogTests(TestCase):
    def test_a_push_feed_is_refused_for_a_pull_fed_catalog_and_names_the_pull_feed(self):
        catalog = _catalog()
        DataFeed.objects.create(name="CHIRPS pull", catalog=catalog)

        with self.assertRaises(ValidationError) as ctx:
            PushFeed.objects.create(catalog=catalog, name="WRF", valid_time_format="YYYYMMDD")

        self.assertIn("CHIRPS pull", " ".join(ctx.exception.messages))
        self.assertFalse(PushFeed.objects.exists())

    def test_a_pull_feed_is_refused_for_a_push_fed_catalog_and_names_the_push_feed(self):
        catalog = _catalog()
        PushFeed.objects.create(catalog=catalog, name="WRF push", valid_time_format="YYYYMMDD")

        with self.assertRaises(ValidationError) as ctx:
            DataFeed.objects.create(name="CHIRPS", catalog=catalog)

        self.assertIn("WRF push", " ".join(ctx.exception.messages))
        self.assertFalse(DataFeed.objects.exists())

    def test_full_clean_reports_it_for_forms(self):
        catalog = _catalog()
        DataFeed.objects.create(name="CHIRPS pull", catalog=catalog)

        with self.assertRaises(ValidationError) as ctx:
            PushFeed(catalog=catalog, name="WRF", valid_time_format="YYYYMMDD").full_clean()
        self.assertIn("catalog", ctx.exception.message_dict)

    def test_resaving_an_existing_feed_is_fine(self):
        feed = DataFeed.objects.create(name="CHIRPS", catalog=_catalog())
        feed.name = "CHIRPS renamed"
        feed.save()
        feed.full_clean()

    def test_a_pull_feed_without_a_catalog_is_not_blocked(self):
        DataFeed.objects.create(name="Orphan")

    def test_repointing_an_existing_feed_at_a_claimed_catalog_is_refused(self):
        claimed = _catalog("claimed")
        PushFeed.objects.create(catalog=claimed, name="WRF push", valid_time_format="YYYYMMDD")
        feed = DataFeed.objects.create(name="CHIRPS", catalog=_catalog("other"))

        feed.catalog = claimed
        with self.assertRaises(ValidationError):
            feed.save()
