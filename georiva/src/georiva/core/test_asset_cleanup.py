"""Pure orphaned-asset-object selection (used by the cleanup_asset_orphans
management command). An orphan is a *raster/visual* object in the assets bucket
that no live Asset.href references — e.g. a file left behind when a re-derivation
rewrote an asset's href in place. Non-asset sidecars (.json metadata) are never
selected, so legitimate files are safe."""
from django.test import SimpleTestCase

from georiva.core.asset_cleanup import DELETABLE_EXTENSIONS, select_orphan_objects


class SelectOrphanObjectsTests(SimpleTestCase):
    def test_selects_raster_objects_no_live_href_points_at(self):
        objects = [
            "chirps/chirps-monthly/precip/2026/05/01/precip_000000.tif",   # live
            "chirps/chirps-monthly/precip/2026/05/01/precip_000000.png",   # live
            "chirps/chirps-monthly/precip/2026/05/01/precip_20260501T000000.tif",  # orphan
            "chirps/chirps-monthly/precip/2026/05/01/precip_20260501T000000.png",  # orphan
        ]
        live = {
            "chirps/chirps-monthly/precip/2026/05/01/precip_000000.tif",
            "chirps/chirps-monthly/precip/2026/05/01/precip_000000.png",
        }

        orphans = select_orphan_objects(objects, live, DELETABLE_EXTENSIONS)

        self.assertEqual(sorted(orphans), [
            "chirps/chirps-monthly/precip/2026/05/01/precip_20260501T000000.png",
            "chirps/chirps-monthly/precip/2026/05/01/precip_20260501T000000.tif",
        ])

    def test_never_selects_non_asset_sidecars(self):
        # A .json metadata sidecar (ingestion writes these; they are NOT Asset
        # rows) must never be treated as an orphan, even though no href points at
        # it.
        objects = [
            "cat/coll/v/2026/05/01/v_000000.tif",       # live
            "cat/coll/v/2026/05/01/v_000000.json",      # sidecar — keep
            "cat/coll/v/2026/05/01/v_stale.tif",        # orphan raster
        ]
        live = {"cat/coll/v/2026/05/01/v_000000.tif"}

        orphans = select_orphan_objects(objects, live, DELETABLE_EXTENSIONS)

        self.assertEqual(orphans, ["cat/coll/v/2026/05/01/v_stale.tif"])

    def test_a_referenced_object_is_never_an_orphan(self):
        objects = ["cat/coll/v/2026/05/01/v_000000.tif"]
        live = {"cat/coll/v/2026/05/01/v_000000.tif"}

        self.assertEqual(
            select_orphan_objects(objects, live, DELETABLE_EXTENSIONS), []
        )

    def test_extension_match_is_case_insensitive(self):
        objects = ["cat/coll/v/2026/05/01/v_stale.TIF"]
        self.assertEqual(
            select_orphan_objects(objects, set(), DELETABLE_EXTENSIONS),
            ["cat/coll/v/2026/05/01/v_stale.TIF"],
        )


class CommandSafetyGuardTests(SimpleTestCase):
    """The command refuses to scan the whole bucket without an explicit scope,
    so a bare invocation can never sweep everything by accident."""

    def test_no_scope_raises_rather_than_scanning_everything(self):
        from django.core.management import call_command
        from django.core.management.base import CommandError

        with self.assertRaises(CommandError):
            call_command("cleanup_asset_orphans")
