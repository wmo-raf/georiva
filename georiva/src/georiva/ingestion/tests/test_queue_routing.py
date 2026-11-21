import ast
from pathlib import Path

from django.test import SimpleTestCase, TestCase

from georiva.config.celery import app

INGESTION_QUEUE = "georiva-ingestion"
PROCESSING_QUEUE = "georiva-processing"
BOOKKEEPING_QUEUE = "georiva-bookkeeping"

#: Tasks whose latency is data availability: until these run, a fetched file is
#: not fetched and a staged file is not extracted. These, and only these.
TIME_CRITICAL = {
    "georiva.ingestion.tasks.process_incoming_file",
    "georiva.ingestion.tasks.process_staging_file",
    "georiva.sources.tasks.run_data_feed_loader",
    "georiva.sources.tasks.retry_fetched_file",
    # A Harvest feed's materialization window: one Remote COG fetched and
    # ingested into our storage (ADR 0042 §8, #640). The harvest stays off it,
    # and a feed has one on the queue at a time, so a deep window delays a
    # fresh file by one materialization per feed, not by the window (#727).
    "georiva.harvest.tasks.materialize_harvested_asset",
}

#: Bookkeeping derived from an ingested COG. Nothing a reader can observe waits
#: on it — tiles, STAC and EDR all answer without it — so it is deferrable, and
#: it has a queue and a worker of its own (ADR 0037) so a burst of it cannot sit
#: in front of a derivation or a publish.
#: (Recovery differs: a dropped manifest build is re-dispatched by the
#: virtual-Zarr sweep; a dropped zonal-stats task is re-issued
#: by ``sweep_missing_boundary_stats`` — #402. Orthogonal to routing.)
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

    def test_per_asset_followups_are_deferrable_and_run_on_the_bookkeeping_queue(self):
        tasks = core_tasks()

        for name in DEFERRABLE_FOLLOWUPS:
            with self.subTest(task=name):
                self.assertEqual(getattr(tasks[name], "queue", None), BOOKKEEPING_QUEUE)

    def test_the_bookkeeping_queue_carries_bookkeeping_and_nothing_else(self):
        on_bookkeeping = {
            name for name, task in core_tasks().items() if getattr(task, "queue", None) == BOOKKEEPING_QUEUE
        }

        self.assertEqual(
            on_bookkeeping,
            DEFERRABLE_FOLLOWUPS,
            "georiva-bookkeeping exists so a fan-out of bookkeeping cannot delay anything "
            "else. A task that something waits on belongs on georiva-processing.",
        )

    def test_no_bookkeeping_sits_on_the_processing_queue(self):
        """The reason the bookkeeping queue exists. Derivation units and, in a
        plugin, publishes share georiva-processing, and a fan-out of per-asset
        bookkeeping in front of them is the delay ADR 0037 was written about."""
        on_processing = {
            name for name, task in core_tasks().items() if getattr(task, "queue", None) == PROCESSING_QUEUE
        }

        self.assertFalse(on_processing & DEFERRABLE_FOLLOWUPS)


class TaskFerryQueueTests(TestCase):
    def test_job_types_on_the_ingestion_queue_are_fetch_and_extraction(self):
        from django.conf import settings
        from task_ferry.registry import job_type_registry

        if settings.TASK_FERRY["CELERY_QUEUE"] != INGESTION_QUEUE:
            self.skipTest("task_ferry no longer dispatches to the ingestion queue")

        registered = {type(job_type).__name__ for job_type in job_type_registry.all()}

        self.assertEqual(
            registered,
            {"FileIngestionJobType", "LoaderJobType"},
            "every registered JobType rides TASK_FERRY['CELERY_QUEUE'] — a new one "
            "joins georiva-ingestion silently. Route it elsewhere or admit it here.",
        )


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
