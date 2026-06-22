import os
import subprocess
import sys
import unittest
from pathlib import Path

# .../georiva/src  (so the subprocess can import the georiva namespace)
_SRC = Path(__file__).resolve().parents[3]
_GP = Path(__file__).resolve().parents[1]


class NoDjangoDependencyTests(unittest.TestCase):
    def test_imports_without_configured_settings(self):
        code = (
            "import os; os.environ.pop('DJANGO_SETTINGS_MODULE', None)\n"
            "import georiva.geoprocessing as g\n"
            "for n in ['zonal_stats_from_array','regrid_array','temporal_aggregate',"
            "'convert_calendar','raster_combine']:\n"
            "    assert hasattr(g, n), n\n"
            "from django.conf import settings\n"
            "assert not settings.configured, 'geoprocessing must not require configured settings'\n"
            "print('OK')\n"
        )
        env = dict(os.environ)
        env.pop("DJANGO_SETTINGS_MODULE", None)
        env["PYTHONPATH"] = str(_SRC)
        r = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, env=env
        )
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("OK", r.stdout)

    def test_modules_do_not_import_django(self):
        for f in _GP.glob("*.py"):
            src = f.read_text()
            self.assertNotIn("import django", src, f"{f.name} imports django")
            self.assertNotIn("from django", src, f"{f.name} imports from django")


if __name__ == "__main__":
    unittest.main()
