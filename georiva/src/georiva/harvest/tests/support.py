"""Shared fixtures for the harvest tests: a Harvest feed with nothing harvested yet."""

from contextlib import contextmanager
from datetime import UTC, datetime
from unittest import mock

from django.db import connection
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from django_redis import get_redis_connection

from georiva.core.machine_plane.remote_assets import PUBLISHED_FIELD, REMOTE_ASSETS_KEY_PREFIX, variable_key
from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
from georiva.harvest.harvester import harvest
from georiva.harvest.models import HarvestFeed, VariableMapping
from georiva.harvest.testing import FakeUpstream
from georiva.organisations.testing import dial_org, make_organisation

UPSTREAM_URL = "https://stac.upstream.example/api"
ASSET_HOST = "https://stac.upstream.example"


def day(n: int, month: int = 3) -> datetime:
    return datetime(2026, month, n, tzinfo=UTC)


def rain_href(n: int) -> str:
    return f"{ASSET_HOST}/chirps/2026/03/chirps-v2.0.2026.03.{n:02d}.tif"


class HarvestFeedMixin:
    """A feed over a fake Upstream, created directly rather than through the
    admin's form. `self.upstream` is the only thing here that is not real."""

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


#: Titiler's own address for Django: a host that belongs to no organisation.
TITILER_DIALS = "georiva:8000"


def stamp(n: int) -> str:
    return f"2026-03-{n:02d}T00:00:00Z"


@contextmanager
def nothing_written(test):
    """Fail if the requests inside write a row or dispatch a task (ADR 0043 §5).

    A statistics computation is a Celery task here, so "no task was sent" is
    what "no statistics were computed" looks like from outside.
    """
    with (
        CaptureQueriesContext(connection) as queries,
        mock.patch("celery.app.task.Task.apply_async") as dispatched,
    ):
        yield
    writes = [q["sql"] for q in queries if q["sql"].lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))]
    test.assertEqual(writes, [], "a read-plane request wrote to the database")
    test.assertFalse(dispatched.called, "a read-plane request dispatched a task")


class RemoteCollectionMixin(HarvestFeedMixin):
    """A harvested collection: days 1 and 2 Remote, and — once
    :meth:`add_local_day` is called — a local day beside them (a Mixed collection)."""

    def setUp(self):
        super().setUp()
        self.redis = get_redis_connection("default")
        self._sweep_redis()
        self.addCleanup(self._sweep_redis)
        self.organisation = self.feed.catalog.organisation
        dial_org(self.client, self.organisation.slug)
        self.variable = self.map_variable("rainfall")
        self.upstream.add_item(day(1), {"rainfall": rain_href(1)})
        self.upstream.add_item(day(2), {"rainfall": rain_href(2)})
        self.harvest()
        self.collection = self.feed.collection

    def _sweep_redis(self):
        keys = list(self.redis.scan_iter(match=f"{REMOTE_ASSETS_KEY_PREFIX}:*"))
        if keys:
            self.redis.delete(*keys)

    def add_local_day(self, n: int = 3) -> Asset:
        """An asset whose bytes are in our bucket, as ingestion would have written it."""
        item = Item.objects.create(collection=self.collection, time=day(n), bounds=[33.0, -5.0, 42.0, 5.0])
        return Asset.objects.create(
            item=item,
            variable=self.variable,
            format=Asset.Format.COG,
            href=f"{self.organisation.slug}/chirps/chirps-daily/rain/2026/03/{n:02d}/rain_000000.tif",
            stats_min=0.0,
            stats_max=40.0,
        )

    def set_visibility(self, visibility):
        Collection.objects.filter(pk=self.collection.pk).update(visibility=visibility)

    def tile_config(self, query="", host=TITILER_DIALS):
        url = reverse("tile_config", args=[self.organisation.slug, "chirps", "chirps-daily", "rain"])
        return self.client.get(f"{url}{query}", HTTP_HOST=host)

    def published(self) -> dict:
        """The Variable's hash as Titiler reads it, without the marker."""
        fields = {k.decode(): v.decode() for k, v in self.redis.hgetall(variable_key(self.variable)).items()}
        fields.pop(PUBLISHED_FIELD, None)
        return fields
