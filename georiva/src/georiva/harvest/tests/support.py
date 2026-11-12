"""Shared fixtures for the harvest tests: a Harvest feed with nothing harvested yet."""

from datetime import UTC, datetime

from georiva.core.models import Catalog, Unit, Variable
from georiva.harvest.harvester import harvest
from georiva.harvest.models import HarvestFeed, VariableMapping
from georiva.harvest.testing import FakeUpstream
from georiva.organisations.testing import make_organisation

UPSTREAM_URL = "https://stac.upstream.example/api"
ASSET_HOST = "https://stac.upstream.example"


def day(n: int, month: int = 3) -> datetime:
    return datetime(2026, month, n, tzinfo=UTC)


def rain_href(n: int) -> str:
    return f"{ASSET_HOST}/chirps/2026/03/chirps-v2.0.2026.03.{n:02d}.tif"


class HarvestFeedMixin:
    """A feed over a fake Upstream, created the way a test must until the admin
    can: directly. `self.upstream` is the only thing here that is not real."""

    def make_feed(self, organisation=None, **fields):
        catalog = Catalog.objects.create(
            organisation=organisation or make_organisation(),
            name="CHIRPS",
            slug="chirps",
            file_format=Catalog.FileFormat.GEOTIFF,
        )
        return HarvestFeed.objects.create(
            name="CHIRPS upstream",
            catalog=catalog,
            upstream_url=UPSTREAM_URL,
            upstream_collection_id="chirps-daily",
            **fields,
        )

    def setUp(self):
        super().setUp()
        self.feed = self.make_feed()
        self.upstream = FakeUpstream("chirps-daily", title="CHIRPS daily rainfall")

    def harvest(self, **kwargs):
        return harvest(self.feed, upstream=self.upstream, **kwargs)

    def map_variable(self, asset_key="rainfall", slug="rain"):
        """Curate a Variable in the feed's collection and map `asset_key` to it."""
        unit, _ = Unit.objects.get_or_create(name="Millimetre", symbol="mm")
        variable = Variable.objects.create(
            collection=self.feed.ensure_collection(),
            slug=slug,
            name=slug.title(),
            unit=unit,
            value_min=0,
            value_max=100,
        )
        VariableMapping.objects.create(feed=self.feed, asset_key=asset_key, variable=variable)
        return variable
