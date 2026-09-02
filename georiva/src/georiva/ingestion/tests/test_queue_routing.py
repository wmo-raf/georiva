"""What is allowed to sit in front of a file waiting to be ingested (#398).

The ingestion worker defaults to concurrency 1, which makes ``georiva-ingestion``
a strict FIFO: whatever is on it delays every file behind it. One ECMWF IFS
fetch (2 collections x 81 variables x 16 steps) fans out ~1,300 per-asset
follow-up tasks at ~1.5s each, and while those shared the ingestion queue the
*next* feed's ``process_staging_file`` waited 30-40 minutes behind bookkeeping
nobody was waiting on. Not a stall — new COGs simply appeared half an hour late
because derived bookkeeping held the line.

So what is pinned here is not "two tasks moved" but the admission rule that
keeps the queue fast, stated as a closed set: ``georiva-ingestion`` carries
fetch and extraction — the work whose latency *is* data availability — and
nothing else. A new task routed there has to be added to ``TIME_CRITICAL``
deliberately, which is the moment to ask whether it belongs.

The second half guards the way this was lost in the first place. A queue named
on a task declaration is only a default: ``apply_async(queue=...)`` silently
overrides it, and all four dispatch sites did exactly that. Declarations that
callers may contradict are not a routing policy, so no dispatch site names a
queue at all — the declaration is the single source of truth.
"""

import ast
from pathlib import Path

from django.test import SimpleTestCase, TestCase

from georiva.config.celery import app

INGESTION_QUEUE = "georiva-ingestion"
PROCESSING_QUEUE = "georiva-processing"

#: Tasks whose latency is data availability: until these run, a fetched file is
#: not fetched and a staged file is not extracted. These, and only these.
TIME_CRITICAL = {
    "georiva.ingestion.tasks.process_incoming_file",
    "georiva.ingestion.tasks.process_staging_file",
    "georiva.sources.tasks.run_data_feed_loader",
    "georiva.sources.tasks.retry_fetched_file",
}

#: Per-asset bookkeeping derived from an ingested COG. Nothing waits on it, and
#: both are re-dispatched by a sweep if they are dropped, so they are deferrable
#: by construction.
DEFERRABLE_FOLLOWUPS = {
    "georiva.analysis.zonal_stats.tasks.compute_boundary_zonal_stats",
    "georiva.virtual_zarr.tasks.build_virtual_zarr_manifest",
}

#: The package root, so the AST sweep is independent of the working directory.
PACKAGE_ROOT = Path(__file__).resolve().parents[2]


def core_tasks():
    """Every task this repo declares, keyed by name.

    Celery discovers tasks lazily, so a test process that has merely imported
    Django sees an empty registry. Plugin tasks are excluded: a source plugin
    is free to route its own work however it likes.
    """
    app.loader.import_default_modules()
    return {name: task for name, task in app.tasks.items() if name.startswith("georiva.")}


def dispatch_sites_naming_a_queue():
    """(path, line) for every ``apply_async`` call that passes ``queue=``."""
    found = []
    for path in PACKAGE_ROOT.rglob("*.py"):
        if "tests" in path.parts or path.name.startswith("test_"):
            continue
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr != "apply_async":
                continue
            if any(kw.arg == "queue" for kw in node.keywords):
                found.append((path.relative_to(PACKAGE_ROOT), node.lineno))
    return found


class IngestionQueueAdmissionTests(TestCase):
    """Importing every task module fires the ``on_after_finalize`` handlers that
    register periodic tasks, which touch the database — hence ``TestCase``."""

    def test_the_ingestion_queue_carries_fetch_and_extraction_and_nothing_else(self):
        on_ingestion = {name for name, task in core_tasks().items() if getattr(task, "queue", None) == INGESTION_QUEUE}

        self.assertEqual(
            on_ingestion,
            TIME_CRITICAL,
            "georiva-ingestion runs at concurrency 1 — anything here delays every "
            "file behind it. Route deferrable work to georiva-processing.",
        )

    def test_per_asset_followups_are_deferrable_and_run_on_the_processing_queue(self):
        tasks = core_tasks()

        for name in DEFERRABLE_FOLLOWUPS:
            with self.subTest(task=name):
                self.assertEqual(getattr(tasks[name], "queue", None), PROCESSING_QUEUE)


class QueueDeclarationIsAuthoritativeTests(SimpleTestCase):
    def test_no_dispatch_site_overrides_the_queue_its_task_declares(self):
        overrides = dispatch_sites_naming_a_queue()

        self.assertEqual(
            overrides,
            [],
            "apply_async(queue=...) silently overrides the task declaration, so "
            "routing read from the declaration would be a fiction. Let the "
            "declaration decide and drop the kwarg.",
        )
