from django.core.management.base import BaseCommand, CommandError

from georiva.core.models import Collection
from georiva.virtual_zarr.models import VirtualZarrManifest
from georiva.virtual_zarr.tasks import build_virtual_zarr_manifest, _run_build


class Command(BaseCommand):
    help = "Build or rebuild virtual Zarr manifests"
    
    def add_arguments(self, parser):
        parser.add_argument(
            "--collection",
            help="catalog_slug/collection_slug, e.g. chirps/chirps-monthly",
        )
        parser.add_argument(
            "--variable",
            help=(
                "Variable slug, e.g. precipitation. "
                "If omitted, all active variables in the collection are built."
            ),
        )
        parser.add_argument(
            "--all",
            action="store_true",
            help="Build manifests for all existing VirtualZarrManifest records.",
        )
        parser.add_argument(
            "--sync",
            action="store_true",
            help="Run synchronously (blocking) instead of dispatching to Celery.",
        )
    
    def handle(self, *args, **options):
        manifests = self._resolve_manifests(options)
        
        if not manifests:
            self.stdout.write("No manifests found.")
            return
        
        self.stdout.write(
            f"{'Sync' if options['sync'] else 'Async'} build for "
            f"{len(manifests)} manifest(s):\n"
        )
        
        for manifest in manifests:
            label = str(manifest)
            if options["sync"]:
                self.stdout.write(f"  [sync] {label}")
                manifest.mark_building("management-command")
                try:
                    _run_build(manifest)
                    self.stdout.write(self.style.SUCCESS(f"    ✓ READY"))
                except Exception as exc:
                    manifest.mark_failed(str(exc))
                    self.stdout.write(self.style.ERROR(f"    ✗ FAILED: {exc}"))
            else:
                build_virtual_zarr_manifest.apply_async(
                    args=[manifest.pk],
                    queue="georiva-ingestion",
                )
                self.stdout.write(f"  [async] {label} → dispatched to Celery")
    
    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------
    
    def _resolve_manifests(self, options) -> list:
        """
        Resolve the list of VirtualZarrManifest records to build.

        --all         → every existing manifest record
        --collection  → manifests for a specific collection, auto-creating
                        missing records for any active variables
        """
        if options["all"] and options["collection"]:
            raise CommandError("Pass either --all or --collection, not both.")
        
        if options["all"]:
            return list(
                VirtualZarrManifest.objects
                .select_related(
                    "variable",
                    "variable__collection",
                    "variable__collection__catalog",
                )
                .all()
            )
        
        if options["collection"]:
            return self._resolve_for_collection(
                options["collection"],
                variable_slug=options.get("variable"),
            )
        
        raise CommandError("Pass --collection <catalog/collection> or --all.")
    
    def _resolve_for_collection(
            self,
            collection_arg: str,
            variable_slug: str | None = None,
    ) -> list:
        """
        Return manifests for a collection, creating missing records as needed.

        If --variable is given, only that variable is considered.
        Auto-creates a VirtualZarrManifest record for any active variable
        that does not yet have one, so the first build can be triggered
        without a separate setup step.
        """
        parts = collection_arg.split("/")
        if len(parts) != 2:
            raise CommandError(
                "--collection must be catalog_slug/collection_slug, "
                f"got: {collection_arg!r}"
            )
        catalog_slug, collection_slug = parts
        
        try:
            collection = Collection.objects.select_related("catalog").get(
                catalog__slug=catalog_slug,
                slug=collection_slug,
            )
        except Collection.DoesNotExist:
            raise CommandError(f"Collection not found: {collection_arg}")
        
        # Active variables to consider — optionally filtered by slug
        variables_qs = collection.variables.filter(is_active=True)
        if variable_slug:
            variables_qs = variables_qs.filter(slug=variable_slug)
            if not variables_qs.exists():
                raise CommandError(
                    f"Variable {variable_slug!r} not found or inactive "
                    f"in {collection_arg}."
                )
        
        manifests = []
        for variable in variables_qs:
            obj, created = VirtualZarrManifest.objects.get_or_create(
                variable=variable,
                defaults={
                    "manifest_path": VirtualZarrManifest.make_manifest_path(variable),
                },
            )
            if created:
                self.stdout.write(
                    self.style.WARNING(f"  Created manifest record: {obj}")
                )
            manifests.append(obj)
        
        return manifests
