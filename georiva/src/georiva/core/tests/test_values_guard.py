"""Values are read through ``core.values`` and nowhere else (ADR 0046 §19).

The one test that inspects source rather than behaviour, because the rule is
about source: a module that opens raster bytes itself reads values the read
module never sees, and once values are converted when read (ADR 0046 §11) its
numbers are in the wrong unit under the right label — and nothing errors.

So a call that opens raster bytes — ``rasterio.open``, an ``xarray.open_*``,
``open_rasterio``, a store's ``open_dataset``/``open_zarr``, an asset's
``open_raster`` or a bucket's ``read_bytes`` — is allowed only in the modules
listed in :data:`ALLOWED`: those that handle bytes, not values. A new entry is
a decision about that module, made in review; an entry whose module no longer
opens anything is taken out.

Test modules are not scanned: a test writes and reads its own fixtures.
"""

from __future__ import annotations

import ast
import tempfile
import textwrap
from pathlib import Path

from django.test import SimpleTestCase

import georiva

SOURCE_ROOT = Path(georiva.__file__).parent

#: Where opening raster bytes is allowed, by path under ``georiva/`` (a folder
#: ends in ``/``), and why. Each is a module that handles bytes, not values.
ALLOWED = {
    "core/values/": "the read module itself",
    "core/storage/asset_location.py": "opens an asset's bytes where they are (ADR 0043 §7), for the read module",
    "core/storage/manager.py": "the bucket: defines read_bytes and open_raster, and copies objects",
    "core/publishing/sink.py": "a publication's own JSON documents, not raster values",
    "ingestion/asset_writer.py": "ingestion's writer: writes the COG",
    "formats/": "the format plugins: decode a source file into the arrays ingestion writes",
    "harvest/materialize.py": "the materializer: copies a Remote COG's bytes into our bucket (#725)",
    "harvest/upstream.py": "the Upstream survey's probe: opens an upstream file to describe it",
    "harvest/testing.py": "FakeUpstream: writes the files a test's Upstream serves",
    "virtual_zarr/": "virtual Zarr's layout reader: reads a COG's layout and a store's coverage",
}

#: Methods that open raster bytes whatever they are called on.
BANNED_METHODS = {"open_rasterio", "open_dataset", "open_mfdataset", "open_zarr", "open_datatree", "open_raster"}


def _is_test_module(relpath: str) -> bool:
    parts = relpath.split("/")
    name = parts[-1]
    return "tests" in parts[:-1] or "migrations" in parts[:-1] or name == "tests.py" or name.startswith("test_")


def banned_calls(source: str) -> list[tuple[int, str]]:
    """``(line, what)`` of each call in ``source`` that opens raster bytes."""
    tree = ast.parse(source)
    rasterio_names, xarray_names = set(), set()
    banned_names: dict[str, str] = {}  # a bare name imported from rasterio/xarray → what it is

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "rasterio":
                    rasterio_names.add(alias.asname or "rasterio")
                elif alias.name == "xarray":
                    xarray_names.add(alias.asname or "xarray")
        elif isinstance(node, ast.ImportFrom) and node.module in ("rasterio", "xarray"):
            for alias in node.names:
                if (node.module == "rasterio" and alias.name == "open") or (
                    node.module == "xarray" and alias.name.startswith("open_")
                ):
                    banned_names[alias.asname or alias.name] = f"{node.module}.{alias.name}"

    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Name) and func.id in banned_names:
            found.append((node.lineno, banned_names[func.id]))
        elif isinstance(func, ast.Attribute):
            receiver = func.value.id if isinstance(func.value, ast.Name) else None
            if receiver in rasterio_names and func.attr == "open":
                found.append((node.lineno, "rasterio.open"))
            elif receiver in xarray_names and func.attr.startswith("open_"):
                found.append((node.lineno, f"xarray.{func.attr}"))
            elif func.attr in BANNED_METHODS:
                found.append((node.lineno, func.attr))
            elif func.attr == "read_bytes" and (node.args or node.keywords):
                # A bucket's read_bytes takes a key; a Path's takes nothing.
                found.append((node.lineno, "read_bytes"))
    return sorted(found)


def violations(root: Path, allowed: dict[str, str]) -> tuple[list[str], set[str]]:
    """Banned calls outside ``allowed`` as ``path:line what``, and the entries of
    ``allowed`` that cover no banned call."""
    found, used = [], set()
    for path in sorted(root.rglob("*.py")):
        relpath = path.relative_to(root).as_posix()
        if _is_test_module(relpath):
            continue
        calls = banned_calls(path.read_text())
        if not calls:
            continue
        entry = next((e for e in allowed if relpath == e or (e.endswith("/") and relpath.startswith(e))), None)
        if entry:
            used.add(entry)
            continue
        found.extend(f"{relpath}:{line} {what}" for line, what in calls)
    return found, set(allowed) - used


class ValuesAreReadThroughTheReadModuleTests(SimpleTestCase):
    def test_nothing_outside_the_allow_list_opens_raster_bytes(self):
        found, _ = violations(SOURCE_ROOT, ALLOWED)

        self.assertEqual(
            found,
            [],
            "These read raster bytes outside georiva.core.values. Read values through a reading session "
            "(`with values.session() as s:`), or, for a module that handles bytes and not values, add it to "
            "ALLOWED with the reason.",
        )

    def test_every_allow_list_entry_is_still_needed(self):
        _, unused = violations(SOURCE_ROOT, ALLOWED)

        self.assertEqual(unused, set(), "These no longer open raster bytes: take them out of ALLOWED.")


class GuardTests(SimpleTestCase):
    """The scan itself: it catches each banned call, and only in modules not allowed."""

    def scan(self, files: dict[str, str], allowed: dict[str, str] | None = None):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for relpath, source in files.items():
                path = root / relpath
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(textwrap.dedent(source))
            return violations(root, allowed or {})

    def test_each_banned_call_is_caught(self):
        sources = {
            "rasterio.open": "import rasterio\nrasterio.open(path)\n",
            "rasterio.open as an alias": "import rasterio as rio\nrio.open(path)\n",
            "rasterio.open imported bare": "from rasterio import open as ropen\nropen(path)\n",
            "xarray.open_dataset": "import xarray as xr\nxr.open_dataset(path)\n",
            "xarray.open_zarr imported bare": "from xarray import open_zarr\nopen_zarr(store)\n",
            "open_rasterio": "import rioxarray\nrioxarray.open_rasterio(path)\n",
            "a store's open_dataset": "manifest.open_dataset(chunks={})\n",
            "an asset's open_raster": "asset.location.open_raster()\n",
            "a bucket's read_bytes": "storage.assets.read_bytes(key)\n",
        }
        for what, source in sources.items():
            with self.subTest(what):
                found, _ = self.scan({"analysis/reader.py": source})
                self.assertEqual(len(found), 1, found)
                self.assertTrue(found[0].startswith("analysis/reader.py:"), found)

    def test_a_path_read_whole_is_not_a_bucket_read(self):
        found, _ = self.scan({"core/web.py": "Path(found).read_bytes()\n"})

        self.assertEqual(found, [])

    def test_an_allowed_module_or_folder_may_open_bytes(self):
        files = {
            "formats/grib.py": "import xarray as xr\nxr.open_dataset(path)\n",
            "ingestion/asset_writer.py": "import rasterio\nrasterio.open(path, 'w')\n",
        }
        found, unused = self.scan(files, {"formats/": "plugins", "ingestion/asset_writer.py": "writer"})

        self.assertEqual((found, unused), ([], set()))

    def test_tests_are_not_scanned(self):
        files = {
            "analysis/tests/test_reader.py": "import rasterio\nrasterio.open(path)\n",
            "analysis/test_reader.py": "import rasterio\nrasterio.open(path)\n",
        }
        self.assertEqual(self.scan(files)[0], [])

    def test_an_entry_that_opens_nothing_is_reported(self):
        _, unused = self.scan({"formats/grib.py": "x = 1\n"}, {"formats/": "plugins"})

        self.assertEqual(unused, {"formats/"})
