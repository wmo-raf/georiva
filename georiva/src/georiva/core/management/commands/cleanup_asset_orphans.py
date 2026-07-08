"""
Delete orphaned raster/visual objects from the assets bucket — files no live
``Asset.href`` references (see ``core.asset_cleanup``). The usual cause is a
re-derivation that rewrote an asset's href in place (e.g. a filename-scheme
change) and left the old object behind.

Safe by default:
  * previews only — pass ``--apply`` to actually delete;
  * a scope is required — ``--catalog`` (optionally with ``--collection``) or the
    explicit ``--all``, so an accidental bare run can't sweep the whole bucket;
  * only known asset object types are ever removed — ``.json`` metadata sidecars
    and any other non-asset files are left untouched.

Examples::

    georiva cleanup_asset_orphans --catalog chirps --collection chirps-monthly
    georiva cleanup_asset_orphans --catalog chirps --apply
    georiva cleanup_asset_orphans --all --apply
"""
from django.core.management.base import BaseCommand, CommandError

from georiva.core.asset_cleanup import DELETABLE_EXTENSIONS, select_orphan_objects
from georiva.core.models import Asset, Collection
from georiva.core.storage import storage


class Command(BaseCommand):
    help = (
        "Delete orphaned raster/visual objects from the assets bucket "
        "(files no Asset.href references). Previews unless --apply is given."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--catalog", metavar="SLUG",
            help="Scope to one catalog (by slug).",
        )
        parser.add_argument(
            "--collection", metavar="SLUG",
            help="Scope to one collection slug (within --catalog).",
        )
        parser.add_argument(
            "--all", action="store_true", dest="all_catalogs",
            help="Sweep every catalog. Required to run without --catalog.",
        )
        parser.add_argument(
            "--apply", action="store_true",
            help="Actually delete. Without it, orphans are only listed.",
        )

    def handle(self, *args, **options):
        catalog = options["catalog"]
        collection = options["collection"]
        apply = options["apply"]

        if not catalog and not options["all_catalogs"]:
            raise CommandError(
                "Refusing to scan the whole bucket without a scope. "
                "Pass --catalog SLUG (optionally --collection SLUG), or --all."
            )

        collections = Collection.objects.select_related("catalog")
        if catalog:
            collections = collections.filter(catalog__slug=catalog)
        if collection:
            collections = collections.filter(slug=collection)
        collections = list(collections)
        if not collections:
            raise CommandError("No collections matched the given scope.")

        total_orphans = 0
        total_bytes = 0
        for coll in collections:
            prefix = f"{coll.catalog.slug}/{coll.slug}"
            objects = [
                f["path"]
                for f in storage.assets.list_files(prefix, recursive=True)
            ]
            live = set(
                Asset.objects
                .filter(href__startswith=f"{prefix}/")
                .values_list("href", flat=True)
            )
            orphans = select_orphan_objects(objects, live, DELETABLE_EXTENSIONS)
            if not orphans:
                continue

            self.stdout.write(
                f"{coll.catalog.slug}/{coll.slug}: "
                f"{len(objects)} objects, {len(live)} live, "
                f"{len(orphans)} orphan(s)"
            )
            for path in orphans:
                size = _safe_size(path)
                total_bytes += size
                total_orphans += 1
                if apply:
                    storage.assets.delete(path)
                    self.stdout.write(f"  deleted {path}")
                else:
                    self.stdout.write(f"  would delete {path} ({size} bytes)")

        verb = "Deleted" if apply else "Would delete"
        prefixnote = "" if apply else " [DRY RUN — pass --apply to delete]"
        self.stdout.write(
            self.style.SUCCESS(
                f"{verb} {total_orphans} orphan object(s), "
                f"{total_bytes} bytes across {len(collections)} collection(s)."
                f"{prefixnote}"
            )
        )


def _safe_size(path: str) -> int:
    try:
        return storage.assets.size(path)
    except Exception:
        return 0
