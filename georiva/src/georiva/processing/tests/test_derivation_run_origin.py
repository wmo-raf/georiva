"""
DerivationRun.origin — the opaque run-grouping key (ADR-0008, issue #144).

The engine stores and indexes `origin` but never interprets it; a later slice's
invocation layer stamps it with the product/trigger identity, and the tracking
UI joins product -> runs by it. These tests assert the field is nullable
(unstamped runs are fine, no backfill) and that adding it did not pull the feed
layer into the engine (ADR-0005 layering).
"""
import ast

from django.test import TestCase

from georiva.processing import engine as engine_module
from georiva.processing import models as models_module
from georiva.processing.models import DerivationRun


def _imported_modules(module) -> set:
    """Every module name imported (at any level) by a Python module's source."""
    import inspect

    tree = ast.parse(inspect.getsource(module))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def _run(**overrides):
    kwargs = dict(
        recipe_type="climatology",
        recipe_version="1",
        unit_key={"period": "1991-2020"},
        unit_hash="a" * 64,
    )
    kwargs.update(overrides)
    return DerivationRun.objects.create(**kwargs)


class DerivationRunOriginTests(TestCase):
    def test_origin_defaults_to_none_for_an_unstamped_run(self):
        run = _run()
        run.refresh_from_db()

        self.assertIsNone(run.origin)

    def test_a_stamped_origin_persists_and_reads_back(self):
        run = _run(origin="derived_product:42:scheduled")
        run.refresh_from_db()

        self.assertEqual(run.origin, "derived_product:42:scheduled")


class EngineLayeringTests(TestCase):
    """ADR-0005: the engine must not depend on the feed/sources layer. The
    opaque `origin` key exists precisely so the engine can be grouped by product
    without importing DerivedProduct — guard that invariant for future slices."""

    def test_engine_models_do_not_import_the_sources_layer(self):
        for module in (models_module, engine_module):
            with self.subTest(module=module.__name__):
                imported = _imported_modules(module)
                self.assertFalse(
                    any(name.startswith("georiva.sources") for name in imported),
                    f"{module.__name__} imports the sources layer: "
                    f"{sorted(n for n in imported if n.startswith('georiva.sources'))}",
                )
