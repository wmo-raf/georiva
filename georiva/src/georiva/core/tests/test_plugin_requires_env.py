import json
import tempfile
from pathlib import Path

from django.test import SimpleTestCase, override_settings

from georiva.core.checks import check_plugin_requires_env
from georiva.core.plugin_manifest import load_manifest, missing_required_env, parse_requires_env

REQUIRED = {"name": "CDSAPI_KEY", "required": True, "description": "Get a key."}
OPTIONAL = {"name": "CDSAPI_URL", "required": False}


class ParseTests(SimpleTestCase):
    def test_absent_key_is_no_requirements(self):
        self.assertEqual(parse_requires_env({}), ([], []))

    def test_non_list_is_a_problem(self):
        entries, problems = parse_requires_env({"requires_env": {"a": 1}})
        self.assertEqual(entries, [])
        self.assertEqual(len(problems), 1)

    def test_malformed_entries_are_reported_and_skipped(self):
        raw = [REQUIRED, "nope", {"required": True}, {"name": "X", "required": "yes"}]
        entries, problems = parse_requires_env({"requires_env": raw})
        self.assertEqual([e["name"] for e in entries], ["CDSAPI_KEY"])
        self.assertEqual(len(problems), 3)

    def test_missing_required_only(self):
        entries, _ = parse_requires_env({"requires_env": [REQUIRED, OPTIONAL]})
        self.assertEqual([e["name"] for e in missing_required_env(entries, {})], ["CDSAPI_KEY"])

    def test_set_value_satisfies(self):
        entries, _ = parse_requires_env({"requires_env": [REQUIRED]})
        self.assertEqual(missing_required_env(entries, {"CDSAPI_KEY": "k"}), [])

    def test_empty_value_counts_as_unset(self):
        entries, _ = parse_requires_env({"requires_env": [REQUIRED]})
        self.assertEqual(len(missing_required_env(entries, {"CDSAPI_KEY": "  "})), 1)


class CheckTests(SimpleTestCase):
    def _plugin(self, payload=None, raw=None):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        if raw is not None:
            (Path(tmp.name) / "georiva_plugin_info.json").write_text(raw)
        elif payload is not None:
            (Path(tmp.name) / "georiva_plugin_info.json").write_text(json.dumps(payload))
        return tmp.name

    def _run(self, folder, env=None, strict=False):
        from unittest import mock

        with (
            override_settings(
                GEORIVA_PLUGIN_FOLDER_BY_APP={"georiva_source_cds": folder}, GEORIVA_STRICT_PLUGIN_ENV=strict
            ),
            mock.patch.dict("os.environ", env or {}, clear=True),
        ):
            return check_plugin_requires_env(None)

    def test_missing_required_warns_with_plugin_name(self):
        out = self._run(self._plugin({"requires_env": [REQUIRED, OPTIONAL]}))
        self.assertEqual([m.id for m in out], ["georiva.W001"])
        self.assertIn("Plugin 'georiva_source_cds' requires CDSAPI_KEY, but it is not set", out[0].msg)
        self.assertEqual(out[0].hint, "Get a key.")

    def test_optional_never_warns(self):
        self.assertEqual(self._run(self._plugin({"requires_env": [OPTIONAL]})), [])

    def test_set_required_is_silent(self):
        self.assertEqual(self._run(self._plugin({"requires_env": [REQUIRED]}), env={"CDSAPI_KEY": "k"}), [])

    def test_strict_escalates_to_error(self):
        out = self._run(self._plugin({"requires_env": [REQUIRED]}), strict=True)
        self.assertEqual([m.id for m in out], ["georiva.E001"])

    def test_malformed_requires_env_warns(self):
        out = self._run(self._plugin({"requires_env": "CDSAPI_KEY"}))
        self.assertEqual([m.id for m in out], ["georiva.W002"])

    def test_unparseable_manifest_warns(self):
        out = self._run(self._plugin(raw="{not json"))
        self.assertEqual([m.id for m in out], ["georiva.W002"])

    def test_missing_manifest_is_skipped(self):
        self.assertEqual(self._run(self._plugin()), [])
        self.assertIsNone(load_manifest("/nonexistent"))
