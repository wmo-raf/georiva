"""A catalog that crops its maps and nothing else (ADR 0038).

``display`` clip mode stores the whole bounding box at ingestion and hands the
tile server an outline to crop what it draws. What is pinned: the outline travels
in the render config only for a catalog in that mode, the other modes carry none,
the stored-data side treats the mode as a plain bounding-box crop, and editing the
catalog re-warms the cache the tile server reads.
"""

import json

from adminboundarymanager.models import AdminBoundary
from django.contrib.gis.geos import MultiPolygon, Polygon
from django.test import TestCase
from django_redis import get_redis_connection

from georiva.core.machine_plane.palette_cache import build_variable_payload, display_boundary, variable_cache_key
from georiva.core.models import Catalog
from georiva.organisations.testing import make_org_tree, make_organisation

ClipMode = Catalog.ClipMode

#: A rectangle with a hole, so a test can tell the outline was carried whole.
OUTER = Polygon.from_bbox((33.0, -5.0, 42.0, 5.0))


def make_boundary():
    return AdminBoundary.objects.create(
        name_0="Testland",
        gid_0="TST",
        level=0,
        geom=MultiPolygon(OUTER),
    )


class DisplayBoundaryTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.tree = make_org_tree(make_organisation("kenya"), name="Kenya Forecast")
        cls.catalog = cls.tree["catalog"]
        cls.boundary = make_boundary()

    def use(self, mode, boundary=True):
        Catalog.objects.filter(pk=self.catalog.pk).update(clip_mode=mode, boundary=self.boundary if boundary else None)
        self.catalog.refresh_from_db()
        return self.catalog

    def test_a_display_catalog_hands_the_renderer_its_outline(self):
        outline = display_boundary(self.use(ClipMode.DISPLAY))

        self.assertIn(outline["type"], ("Polygon", "MultiPolygon"))
        polygons = [outline["coordinates"]] if outline["type"] == "Polygon" else outline["coordinates"]
        xs = [x for rings in polygons for ring in rings for x, _ in ring]
        self.assertEqual((min(xs), max(xs)), (33.0, 42.0))

    def test_the_outline_rides_in_the_variables_render_config(self):
        self.use(ClipMode.DISPLAY)
        variable = self.tree["variable"].__class__.objects.get(pk=self.tree["variable"].pk)

        self.assertIn("boundary", build_variable_payload(variable))

    def test_every_other_mode_carries_no_outline(self):
        for mode in (ClipMode.NONE, ClipMode.BBOX, ClipMode.MASK):
            with self.subTest(mode=mode):
                self.assertIsNone(display_boundary(self.use(mode)))

    def test_display_without_a_boundary_has_nothing_to_crop_to(self):
        self.assertIsNone(display_boundary(self.use(ClipMode.DISPLAY, boundary=False)))

    def test_the_outline_is_simplified(self):
        """A country outline is not hundreds of kilobytes in every Redis value."""
        edge = [(33 + i / 1000, -5 + (i % 2) * 1e-6) for i in range(0, 9000, 2)]
        dense = Polygon(edge + [(42, 5), (33, 5), (33, -5)])
        AdminBoundary.objects.filter(pk=self.boundary.pk).update(geom=MultiPolygon(dense.buffer(0)))
        self.boundary.refresh_from_db()

        outline = display_boundary(self.use(ClipMode.DISPLAY))

        polygons = [outline["coordinates"]] if outline["type"] == "Polygon" else outline["coordinates"]
        vertices = sum(len(ring) for rings in polygons for ring in rings)
        self.assertLess(vertices, 100)


class StoredDataSideTests(TestCase):
    """Ingestion reads ``clip_mode`` in two places, and ``display`` must be a bbox crop in both."""

    @classmethod
    def setUpTestData(cls):
        cls.tree = make_org_tree(make_organisation("kenya"), name="Kenya Forecast")
        cls.catalog = cls.tree["catalog"]
        Catalog.objects.filter(pk=cls.catalog.pk).update(clip_mode=ClipMode.DISPLAY, boundary=make_boundary())
        cls.catalog.refresh_from_db()

    def test_the_stored_extent_is_the_boundarys_bounding_box(self):
        collection = self.tree["collection"].__class__.objects.get(pk=self.tree["collection"].pk)

        self.assertEqual(collection.spatial_extent, [33.0, -5.0, 42.0, 5.0])

    def test_ingestion_crops_to_the_box_without_masking_to_the_shape(self):
        """The two expressions ``service.py`` and the derivation engine share."""
        clips = self.catalog.clip_mode != "none"
        masks = self.catalog.clip_mode == "mask"

        self.assertTrue(clips)
        self.assertFalse(masks)


class RewarmingTests(TestCase):
    """The tile server reads Redis directly, so an edit to the catalog has to reach it."""

    @classmethod
    def setUpTestData(cls):
        cls.tree = make_org_tree(make_organisation("kenya"), name="Kenya Forecast")
        cls.catalog = cls.tree["catalog"]
        cls.boundary = make_boundary()

    def setUp(self):
        self.redis = get_redis_connection("default")
        self.addCleanup(self._clear)
        self._clear()

    def _clear(self):
        keys = list(self.redis.scan_iter(match="georiva:palette:*"))
        if keys:
            self.redis.delete(*keys)

    def cached(self):
        variable = self.tree["variable"].__class__.objects.get(pk=self.tree["variable"].pk)
        return json.loads(self.redis.get(variable_cache_key(variable)))

    def test_choosing_display_puts_the_outline_in_the_cache(self):
        self.catalog.boundary = self.boundary
        self.catalog.clip_mode = ClipMode.DISPLAY
        self.catalog.save()

        self.assertIn("boundary", self.cached())

    def test_leaving_display_takes_it_out_again(self):
        self.catalog.boundary = self.boundary
        self.catalog.clip_mode = ClipMode.DISPLAY
        self.catalog.save()
        self.catalog.clip_mode = ClipMode.MASK
        self.catalog.save()

        self.assertNotIn("boundary", self.cached())


class TextureTokenTests(TestCase):
    """The encoded texture is cached as immutable under a ``v`` token; the crop is baked into it."""

    @classmethod
    def setUpTestData(cls):
        cls.tree = make_org_tree(make_organisation("kenya"), name="Kenya Forecast")
        cls.catalog = cls.tree["catalog"]
        cls.boundary = make_boundary()

    def token(self, mode, boundary=True):
        from georiva.core.machine_plane import render_config_token
        from georiva.core.models import Variable

        Catalog.objects.filter(pk=self.catalog.pk).update(clip_mode=mode, boundary=self.boundary if boundary else None)
        return render_config_token(Variable.objects.get(pk=self.tree["variable"].pk))

    def test_moving_to_display_is_a_different_url(self):
        """Or a browser keeps the uncropped texture it already fetched."""
        self.assertNotEqual(self.token(ClipMode.MASK), self.token(ClipMode.DISPLAY))

    def test_every_other_mode_keeps_the_url_it_had(self):
        tokens = {self.token(mode) for mode in (ClipMode.NONE, ClipMode.BBOX, ClipMode.MASK)}

        self.assertEqual(len(tokens), 1)

    def test_display_without_a_boundary_changes_nothing(self):
        self.assertEqual(self.token(ClipMode.DISPLAY, boundary=False), self.token(ClipMode.MASK))
