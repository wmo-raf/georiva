"""The sweep claims a manifest before dispatching it (#398).

``sweep_virtual_zarr_pending`` runs every 5 minutes and dispatches every
buildable manifest. The build task marked itself BUILDING when it *started*,
which on a queue with a backlog is much later than when it was queued — so a
manifest that waited more than 5 minutes was still PENDING when the next sweep
looked, and was dispatched again. And again. The IFS backlog in #398 was 30-40
minutes deep: seven sweeps, seven copies.

While the build ran on the concurrency-1 ingestion queue the copies merely ran
one after another, wasting a rebuild each. Moving these builds to
``georiva-processing`` puts four pool slots behind them, so the copies can now
overlap — two writers on one Icechunk repo, a ConflictError, and a manifest
flipped to FAILED by a race the docstrings claimed was impossible.

The fix is to claim at dispatch rather than at start: the status moves to
BUILDING in the same conditional UPDATE that decides whether to dispatch, so a
manifest can be in flight exactly once. Crash recovery is unchanged and now
covers the queued window too — a claim whose task never runs looks exactly like
a worker that died mid-build, and ``reset_stale_locks`` already handles that.
"""

from datetime import timedelta
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone

from georiva.core.models import Catalog, Collection, Unit, Variable
from georiva.organisations.testing import make_organisation
from georiva.virtual_zarr.models import VirtualZarrManifest
from georiva.virtual_zarr.tasks import (
    build_virtual_zarr_manifest,
    dispatch_build,
    sweep_virtual_zarr_pending,
)


class SweepDispatchTests(TestCase):
    def setUp(self):
        organisation = make_organisation()
        catalog = Catalog.objects.create(
            organisation=organisation,
            name="CHIRPS",
            slug="chirps",
            file_format="geotiff",
        )
        self.collection = Collection.objects.create(catalog=catalog, name="Monthly", slug="chirps-monthly")
        self.unit, _ = Unit.objects.get_or_create(name="Millimetre", defaults={"symbol": "mm"})

    def _manifest(self, *, slug="precipitation", status=VirtualZarrManifest.Status.PENDING, locked_at=None):
        variable = Variable.objects.create(
            collection=self.collection,
            slug=slug,
            name=slug,
            unit=self.unit,
            value_min=0,
            value_max=500,
        )
        return VirtualZarrManifest.objects.create(variable=variable, status=status, locked_at=locked_at)

    def _sweep(self):
        """Run one sweep, returning the manifest ids it dispatched.

        ``delay`` routes through ``apply_async``, so patching the latter catches
        a dispatch however it was written — positionally by ``delay`` or as the
        ``args`` keyword.
        """
        with patch.object(build_virtual_zarr_manifest, "apply_async") as dispatch:
            sweep_virtual_zarr_pending()

        return [(call.args[0] if call.args else call.kwargs["args"])[0] for call in dispatch.call_args_list]

    def test_a_buildable_manifest_is_dispatched(self):
        manifest = self._manifest()

        self.assertEqual(self._sweep(), [manifest.pk])

    def test_a_manifest_still_queued_is_not_dispatched_by_the_next_sweep(self):
        manifest = self._manifest()

        first = self._sweep()
        second = self._sweep()

        self.assertEqual(first, [manifest.pk])
        self.assertEqual(second, [], "the manifest was queued, not built — dispatching it again races the first copy")

    def test_dispatching_claims_the_manifest_so_it_is_no_longer_buildable(self):
        manifest = self._manifest()

        self._sweep()

        manifest.refresh_from_db()
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.BUILDING)
        self.assertIsNotNone(manifest.locked_at)
        self.assertNotIn(manifest, VirtualZarrManifest.get_buildable())

    def test_stale_and_failed_manifests_are_claimed_too(self):
        for status in (VirtualZarrManifest.Status.STALE, VirtualZarrManifest.Status.FAILED):
            with self.subTest(status=status):
                manifest = self._manifest(slug=f"var-{status}", status=status)

                self.assertIn(manifest.pk, self._sweep())

    def test_a_claim_whose_task_never_ran_is_recovered_by_a_later_sweep(self):
        manifest = self._manifest()
        self._sweep()

        VirtualZarrManifest.objects.filter(pk=manifest.pk).update(
            locked_at=timezone.now() - VirtualZarrManifest.LOCK_TIMEOUT - timedelta(minutes=1)
        )

        self.assertEqual(self._sweep(), [manifest.pk])

    def test_a_copy_whose_claim_was_recycled_stands_down(self):
        """The one duplicate claiming alone cannot prevent.

        A queue wait longer than ``LOCK_TIMEOUT`` lets ``reset_stale_locks``
        free the row and a later sweep dispatch a replacement. Both copies are
        then live, and whichever runs holding a stale claim must not build.
        """
        manifest = self._manifest()
        self._sweep()
        manifest.refresh_from_db()
        current_claim = manifest.locked_by

        build_virtual_zarr_manifest.apply(args=[manifest.pk, "sweep-deadbeef"])

        manifest.refresh_from_db()
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.BUILDING)
        self.assertEqual(manifest.locked_by, current_claim, "the stale copy took a lock it does not hold")

    # -- the operator's rebuild ----------------------------------------------

    def test_an_operator_rebuild_claims_a_ready_manifest_the_sweep_would_skip(self):
        """``build_virtual_zarr`` resolves --all and --collection to manifests in
        any status; naming one means rebuild it, READY included."""
        manifest = self._manifest(status=VirtualZarrManifest.Status.READY)

        with patch.object(build_virtual_zarr_manifest, "apply_async"):
            self.assertFalse(dispatch_build(manifest.pk))
            self.assertTrue(dispatch_build(manifest.pk, force=True))

        manifest.refresh_from_db()
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.BUILDING)

    def test_an_operator_rebuild_still_refuses_a_manifest_being_built(self):
        manifest = self._manifest(
            status=VirtualZarrManifest.Status.BUILDING,
            locked_at=timezone.now() - timedelta(minutes=5),
        )

        with patch.object(build_virtual_zarr_manifest, "apply_async"):
            self.assertFalse(dispatch_build(manifest.pk, force=True))

    def test_a_manifest_actively_building_is_left_to_its_worker(self):
        manifest = self._manifest(
            status=VirtualZarrManifest.Status.BUILDING,
            locked_at=timezone.now() - timedelta(minutes=5),
        )
        locked_at = manifest.locked_at

        self.assertEqual(self._sweep(), [])

        manifest.refresh_from_db()
        self.assertEqual(manifest.locked_at, locked_at)
