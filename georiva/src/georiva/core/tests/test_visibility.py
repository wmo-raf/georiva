from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Catalog, Collection, Unit, Variable
from georiva.organisations.testing import dial_org, make_organisation


class CollectionVisibilityTests(TestCase):
    """A Collection is `public` (served) by default; `internal` intermediates
    are never served but read freely by the derivation engine."""

    def setUp(self):
        self.catalog = Catalog.objects.create(
            organisation=make_organisation(), name="Models", slug="models", file_format="grib2"
        )

    def test_defaults_to_public(self):
        coll = Collection.objects.create(catalog=self.catalog, name="Surface", slug="surface")
        self.assertEqual(coll.visibility, Collection.Visibility.PUBLIC)
        self.assertEqual(coll.visibility, "public")

    def test_can_be_marked_internal(self):
        coll = Collection.objects.create(
            catalog=self.catalog,
            name="Anomaly",
            slug="anomaly",
            visibility=Collection.Visibility.INTERNAL,
        )
        coll.refresh_from_db()
        self.assertEqual(coll.visibility, "internal")


class TileConfigVisibilityTests(TestCase):
    """The internal tile-config endpoint must not serve internal collections."""

    def setUp(self):
        # Dial the org that owns the fixture: the endpoint takes its org from
        # the path, but a host that resolves still wins over it (#272), so the
        # bare `testserver` default would 404 on somebody else's org segment.
        self.organisation = make_organisation()
        dial_org(self.client)
        self.catalog = Catalog.objects.create(
            organisation=self.organisation, name="CMIP6", slug="cmip6", file_format="geotiff"
        )
        self.unit = Unit.objects.create(name="Celsius", symbol="C")

    def _variable(self, collection_slug, visibility):
        coll = Collection.objects.create(
            catalog=self.catalog,
            name=collection_slug,
            slug=collection_slug,
            visibility=visibility,
        )
        return Variable.objects.create(
            collection=coll,
            slug="tas",
            name="tas",
            unit=self.unit,
            value_min=0,
            value_max=50,
        )

    def _url(self, collection_slug, org_slug=None):
        return reverse(
            "tile_config",
            args=[org_slug or self.organisation.slug, "cmip6", collection_slug, "tas"],
        )

    def test_public_collection_served(self):
        self._variable("tas", Collection.Visibility.PUBLIC)
        self.assertEqual(self.client.get(self._url("tas")).status_code, 200)

    def test_internal_collection_404(self):
        self._variable("tas-anomaly", Collection.Visibility.INTERNAL)
        self.assertEqual(self.client.get(self._url("tas-anomaly")).status_code, 404)
