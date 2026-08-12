"""
Asset save/delete signal tests: COG changes keep the manifest state machine
in sync with reality, so NO_DATA manifests wake up when data arrives and
READY manifests go STALE when data is removed.
"""

from datetime import datetime, timezone

from django.test import TestCase

from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
from georiva.organisations.testing import make_organisation
from georiva.virtual_zarr.models import VirtualZarrManifest

UTC = timezone.utc


class AssetSignalTestCase(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            organisation=make_organisation(),
            name="CHIRPS",
            slug="chirps",
            file_format="geotiff",
        )
        self.collection = Collection.objects.create(
            catalog=self.catalog,
            name="Monthly",
            slug="chirps-monthly",
        )
        unit, _ = Unit.objects.get_or_create(name="Millimetre", defaults={"symbol": "mm"})
        self.variable = Variable.objects.create(
            collection=self.collection,
            slug="precipitation",
            name="Precipitation",
            unit=unit,
            value_min=0,
            value_max=500,
        )
        self.item = Item.objects.create(
            collection=self.collection,
            time=datetime(2026, 1, 1, tzinfo=UTC),
        )

    def _cog(self, href="k/a.tif") -> Asset:
        return Asset.objects.create(
            item=self.item,
            variable=self.variable,
            format=Asset.Format.COG,
            roles=["data"],
            href=href,
        )

    def _set_status(self, manifest, status) -> None:
        VirtualZarrManifest.objects.filter(pk=manifest.pk).update(status=status)

    def test_cog_save_creates_pending_manifest(self):
        self._cog()
        manifest = self.variable.virtual_zarr_manifest
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.PENDING)

    def test_cog_save_wakes_no_data_manifest(self):
        asset = self._cog()
        manifest = self.variable.virtual_zarr_manifest
        self._set_status(manifest, VirtualZarrManifest.Status.NO_DATA)

        asset.save()

        manifest.refresh_from_db()
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.STALE)

    def test_cog_delete_marks_ready_manifest_stale(self):
        asset = self._cog()
        manifest = self.variable.virtual_zarr_manifest
        self._set_status(manifest, VirtualZarrManifest.Status.READY)

        asset.delete()

        manifest.refresh_from_db()
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.STALE)

    def test_non_cog_delete_leaves_manifest_alone(self):
        self._cog()
        png = Asset.objects.create(
            item=self.item,
            variable=self.variable,
            format=Asset.Format.PNG,
            roles=["data"],
            href="k/a.png",
        )
        manifest = self.variable.virtual_zarr_manifest
        self._set_status(manifest, VirtualZarrManifest.Status.READY)

        png.delete()

        manifest.refresh_from_db()
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.READY)
