"""Context across the task queue and onto log lines (ADR 0030, #422)."""

import json
import logging
import uuid

from django.test import SimpleTestCase, TestCase, override_settings

from georiva.config.celery import app
from georiva.events import metrics
from georiva.events.context import current_context, new_context, use_context
from georiva.events.logs import EventContextFilter, JsonFormatter
from georiva.events.tasks import (
    ChainedTask,
    EntryTask,
    MissingTaskContext,
    ReadSideTask,
    SweepTask,
    context_to_headers,
    inject_headers,
)

seen = []


@app.task(name="georiva.events.tests.chained", base=ChainedTask)
def chained_probe():
    seen.append(current_context())
    return current_context()


@app.task(name="georiva.events.tests.entry", base=EntryTask)
def entry_probe():
    return current_context()


@app.task(name="georiva.events.tests.sweep", base=SweepTask, sweep_name="probe")
def sweep_probe():
    return current_context()


@app.task(name="georiva.events.tests.read_side", base=ReadSideTask)
def read_side_probe():
    return current_context()


@app.task(name="georiva.events.tests.plain")
def plain_probe():
    return "ran"


def headers_for(**kwargs):
    context = new_context(actor="user:3", organisation_id=7, **kwargs)
    return context, context_to_headers(context)


class ChainedTaskTests(TestCase):
    def setUp(self):
        seen.clear()

    def test_a_chained_task_runs_with_the_context_its_dispatcher_carried(self):
        context, headers = headers_for()

        result = chained_probe.apply(headers=headers).get()

        self.assertEqual(result.correlation_id, context.correlation_id)
        self.assertEqual(result.actor, "user:3")
        self.assertEqual(result.organisation_id, 7)

    def test_caused_by_travels_too(self):
        cause = uuid.uuid4()
        context, headers = headers_for(caused_by=cause)

        self.assertEqual(chained_probe.apply(headers=headers).get().caused_by, cause)

    def test_whatever_it_publishes_carries_the_same_context(self):
        context, headers = headers_for()
        published = {}

        class Capture:
            def __enter__(self_inner):
                return self_inner

            def __exit__(self_inner, *exc):
                return False

        # Inside the task the active context is what the publish signal stamps.
        @app.task(name="georiva.events.tests.publisher", base=ChainedTask)
        def publisher():
            inject_headers(headers=published)

        publisher.apply(headers=headers).get()

        self.assertEqual(published["georiva_correlation_id"], str(context.correlation_id))
        self.assertEqual(published["georiva_actor"], "user:3")
        self.assertEqual(published["georiva_organisation_id"], "7")

    @override_settings(GEORIVA_TASK_CONTEXT_MODE="strict")
    def test_strict_mode_raises_without_headers(self):
        result = chained_probe.apply()
        with self.assertRaises(MissingTaskContext):
            result.get()

    @override_settings(GEORIVA_TASK_CONTEXT_MODE="warn")
    def test_warn_mode_logs_counts_and_continues_as_unpropagated(self):
        before = metrics.unpropagated_count()

        with self.assertLogs("georiva.events.tasks", level="ERROR"):
            result = chained_probe.apply().get()

        self.assertEqual(result.actor, "system:unpropagated")
        self.assertEqual(metrics.unpropagated_count(), before + 1)

    @override_settings(GEORIVA_TASK_CONTEXT_MODE="strict")
    def test_running_inline_inside_a_context_is_not_a_loss(self):
        with use_context(new_context(actor="user:1")) as context:
            self.assertEqual(chained_probe().correlation_id, context.correlation_id)

    @override_settings(GEORIVA_TASK_CONTEXT_MODE="warn")
    def test_two_tasks_for_different_organisations_share_no_context(self):
        _, first = headers_for()
        chained_probe.apply(headers=first).get()
        self.assertIsNone(current_context())

        with self.assertLogs("georiva.events.tasks", level="ERROR"):
            second = chained_probe.apply().get()

        self.assertIsNone(second.organisation_id)
        self.assertNotEqual(second.actor, "user:3")
        self.assertIsNone(current_context())

    def test_the_context_is_reset_when_a_task_raises(self):
        @app.task(name="georiva.events.tests.boom", base=ChainedTask)
        def boom():
            raise RuntimeError("x")

        _, headers = headers_for()
        boom.apply(headers=headers)

        self.assertIsNone(current_context())


class OtherBasesTests(TestCase):
    def test_an_entry_task_always_starts_a_fresh_chain(self):
        with use_context(new_context(actor="user:1")) as outer:
            result = entry_probe.apply().get()

        self.assertNotEqual(result.correlation_id, outer.correlation_id)
        self.assertEqual(result.actor, "celery")

    def test_a_sweep_task_starts_a_named_context(self):
        self.assertEqual(sweep_probe.apply().get().actor, "sweep:probe")

    @override_settings(GEORIVA_TASK_CONTEXT_MODE="strict")
    def test_a_read_side_task_runs_with_no_context(self):
        self.assertIsNone(read_side_probe.apply().get())

    def test_a_read_side_task_inherits_a_context_when_one_arrives(self):
        context, headers = headers_for()
        self.assertEqual(read_side_probe.apply(headers=headers).get().correlation_id, context.correlation_id)

    @override_settings(GEORIVA_TASK_CONTEXT_MODE="strict")
    def test_a_plainly_declared_task_keeps_working(self):
        self.assertEqual(plain_probe.apply().get(), "ran")


class EveryCoreTaskDeclaresABaseTests(TestCase):
    def test_every_core_task_is_on_one_of_the_context_bases(self):
        app.loader.import_default_modules()
        bases = (ChainedTask, EntryTask, SweepTask, ReadSideTask)
        undeclared = sorted(
            name
            for name, task in app.tasks.items()
            if name.startswith("georiva.")
            and not name.startswith("georiva.events.tests.")
            and not isinstance(task, bases)
        )
        self.assertEqual(undeclared, [], "declare base=ChainedTask/EntryTask/SweepTask/ReadSideTask (ADR 0030)")

    def test_a_plainly_declared_task_is_outside_the_closed_set(self):
        # Plugin tasks are out of scope: the assertion above is about core only.
        self.assertNotIsInstance(plain_probe, (ChainedTask, EntryTask, SweepTask, ReadSideTask))


class LoggingTests(SimpleTestCase):
    def _record(self, logger_name="georiva.processing.engine"):
        record = logging.LogRecord(logger_name, logging.INFO, __file__, 1, "hello %s", ("world",), None)
        EventContextFilter().filter(record)
        return record

    def test_a_record_carries_the_correlation_and_actor(self):
        with use_context(new_context(actor="celery")) as context:
            record = self._record()
        self.assertEqual(record.correlation_id, str(context.correlation_id))
        self.assertEqual(record.actor, "celery")

    def test_outside_a_context_it_is_a_dash(self):
        self.assertEqual(self._record().correlation_id, "-")

    def test_json_format_makes_the_id_a_field(self):
        with use_context(new_context(actor="celery")) as context:
            payload = json.loads(JsonFormatter().format(self._record()))
        self.assertEqual(payload["correlation_id"], str(context.correlation_id))
        self.assertEqual(payload["message"], "hello world")

    def test_the_non_propagating_processing_logger_is_covered(self):
        from django.conf import settings

        config = settings.LOGGING
        for name in ("georiva.processing", "georiva.sources.derivation_invocation"):
            with self.subTest(logger=name):
                self.assertFalse(config["loggers"][name]["propagate"])
                self.assertIn("event_context", config["handlers"]["console"]["filters"])

    def test_plain_text_is_the_default_format(self):
        from django.conf import settings

        self.assertEqual(settings.LOGGING["handlers"]["console"]["formatter"], "console")
        self.assertIn("[cid=", settings.LOGGING["formatters"]["console"]["format"])
