"""
Manually invoke a derivation recipe over a selector.

Usage:
    georiva run_recipe promotion --collection tas-ssp245 --sync
    georiva run_recipe promotion --staging-item-id 12 --staging-item-id 13
"""
from django.core.management.base import BaseCommand, CommandError

from georiva.processing.engine import run
from georiva.processing.registry import recipe_registry


class Command(BaseCommand):
    help = "Run a derivation recipe over a selector (manual invocation)."

    def add_arguments(self, parser):
        parser.add_argument("recipe_type")
        parser.add_argument("--collection", dest="collection_slug", default=None)
        parser.add_argument(
            "--staging-item-id", dest="staging_item_ids", type=int,
            action="append", default=None,
        )
        parser.add_argument(
            "--sync", action="store_true",
            help="Run units inline instead of dispatching to the queue.",
        )

    def handle(self, *args, **options):
        recipe = recipe_registry.get(options["recipe_type"])
        if recipe is None:
            raise CommandError(
                f"Unknown recipe '{options['recipe_type']}'. "
                f"Available: {', '.join(recipe_registry.all_types()) or '(none)'}"
            )

        selector = {}
        if options.get("collection_slug"):
            selector["collection_slug"] = options["collection_slug"]
        if options.get("staging_item_ids"):
            selector["staging_item_ids"] = options["staging_item_ids"]

        results = run(recipe, selector, dispatch=not options["sync"])

        if options["sync"]:
            by_status = {}
            for r in results:
                by_status[r.status] = by_status.get(r.status, 0) + 1
            self.stdout.write(self.style.SUCCESS(
                f"Ran {len(results)} unit(s): "
                + ", ".join(f"{k}={v}" for k, v in sorted(by_status.items()))
            ))
        else:
            self.stdout.write(self.style.SUCCESS(
                f"Dispatched {len(results)} unit(s) to georiva-processing."
            ))
