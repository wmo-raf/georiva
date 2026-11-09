import json
import tempfile
import textwrap
from pathlib import Path

from django.test import SimpleTestCase

from georiva.site_plugins.installer import (
    INSTALLED,
    INSTALLED_LINE,
    UP_TO_DATE_LINE,
    InstallError,
    install,
    report,
    stamp_for,
)
from georiva.site_plugins.manifest import PluginSpec, load_specs

MANIFEST = textwrap.dedent(
    """
    [[plugins]]
    name = "CDS"
    git  = "https://example.org/georiva-source-cds.git"
    tag  = "1.0.0"

    [[plugins]]
    name = "Off"
    git  = "https://example.org/georiva-source-off.git"
    enabled = false
    """
)

NOOP_HOOK = "#!/bin/bash\n# nothing to do\nset -euo pipefail\n"


class Site:
    """A scratch site-plugins directory plus fakes for the two things that touch the network."""

    def __init__(self, case, manifest=MANIFEST, hook=None, requires_env=None):
        tmp = tempfile.TemporaryDirectory()
        case.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.site = self.root / "site-plugins"
        self.manifest = self.root / "plugins.toml"
        self.manifest.write_text(manifest)
        self.constraints = self.root / "constraints.txt"
        self.constraints.write_text("numpy==2.0.0\n")
        self.hook = hook
        self.requires_env = requires_env
        self.fetched = []
        self.pip_calls = []
        self.pip_error = None
        #: What the next fetch reports, per package: the commit, and the versions its
        #: plugin info and pyproject declare (``None`` leaves that file without one).
        self.commit = None
        self.info_version = None
        self.pyproject_version = None
        self.packages = ["georiva_source_cds"]

    def fetch(self, spec: PluginSpec, dest: Path):
        self.fetched.append(spec.git or spec.url)
        package = self.packages[len(self.fetched) - 1] if len(self.fetched) <= len(self.packages) else self.packages[-1]
        (dest / "src" / package).mkdir(parents=True)
        (dest / "src" / package / "__init__.py").write_text("")
        version = f'version = "{self.pyproject_version}"\n' if self.pyproject_version else ""
        (dest / "pyproject.toml").write_text(
            f'[project]\nname = "x"\n{version}dependencies = ["cdsapi>=0.7", "numpy"]\n'
        )
        if self.hook is not None:
            (dest / "build.sh").write_text(self.hook)
        if self.requires_env is not None:
            (dest / "georiva_plugin_info.json").write_text(self.requires_env)
        elif self.info_version is not None:
            (dest / "georiva_plugin_info.json").write_text(json.dumps({"version": self.info_version}))
        return self.commit

    def pip(self, requirements, target, constraints):
        if self.pip_error:
            raise InstallError(self.pip_error)
        self.pip_calls.append((list(requirements), Path(target), Path(constraints)))
        Path(target).mkdir(parents=True, exist_ok=True)

    def run(self, tag="v1", environ=None, force=False):
        return install(
            self.manifest,
            self.site,
            tag=tag,
            constraints=self.constraints,
            fetch=self.fetch,
            pip=self.pip,
            environ=environ or {},
            force=force,
        )


class InstallTests(SimpleTestCase):
    def test_enabled_plugins_land_under_src_by_their_package_name(self):
        site = Site(self)
        site.run()
        plugin = site.site / "src" / "georiva_source_cds"
        self.assertTrue((plugin / "src" / "georiva_source_cds" / "__init__.py").exists())
        self.assertEqual(site.fetched, ["https://example.org/georiva-source-cds.git"])

    def test_dependencies_install_into_lib_against_the_core_constraints(self):
        site = Site(self)
        site.run()
        ((requirements, target, constraints),) = site.pip_calls
        self.assertEqual(requirements, ["cdsapi>=0.7", "numpy"])
        self.assertEqual(target.name, "lib")
        self.assertEqual(constraints, site.constraints)

    def test_a_matching_stamp_skips_everything(self):
        site = Site(self)
        site.run()
        site.fetched.clear()
        site.pip_calls.clear()
        result = site.run()
        self.assertFalse(result.installed)
        self.assertEqual((site.fetched, site.pip_calls), ([], []))

    def test_a_new_release_tag_reinstalls(self):
        site = Site(self)
        site.run(tag="v1")
        site.pip_calls.clear()
        self.assertTrue(site.run(tag="v2").installed)
        self.assertEqual(len(site.pip_calls), 1)

    def test_a_changed_manifest_reinstalls(self):
        site = Site(self)
        site.run()
        site.manifest.write_text(MANIFEST.replace("1.0.0", "1.1.0"))
        self.assertTrue(site.run().installed)

    def test_force_reinstalls_despite_the_stamp(self):
        site = Site(self)
        site.run()
        self.assertTrue(site.run(force=True).installed)

    def test_a_failed_pip_leaves_no_stamp_and_the_old_install_intact(self):
        site = Site(self)
        site.run(tag="v1")
        site.pip_error = "conflicts with numpy==2.0.0"
        with self.assertRaisesRegex(InstallError, "numpy"):
            site.run(tag="v2")
        self.assertTrue((site.site / "src" / "georiva_source_cds").exists())
        self.assertEqual((site.site / "stamp").read_text().strip(), stamp_for(site.manifest, "v1"))

    def test_removing_a_plugin_from_the_manifest_removes_it_from_the_site(self):
        site = Site(self)
        site.run()
        site.manifest.write_text("")
        site.run()
        self.assertFalse((site.site / "src" / "georiva_source_cds").exists())

    def test_a_hook_with_real_commands_fails_the_install(self):
        site = Site(self, hook="#!/bin/bash\npip install evil\n")
        with self.assertRaisesRegex(InstallError, "build.sh"):
            site.run()

    def test_a_manifest_that_is_a_directory_fails_instead_of_uninstalling_everything(self):
        site = Site(self)
        site.run()
        site.manifest.unlink()
        site.manifest.mkdir()
        with self.assertRaisesRegex(InstallError, "is a directory"):
            site.run()
        self.assertTrue((site.site / "src" / "georiva_source_cds").exists())

    def test_combined_set_flags_and_exit_zero_are_still_no_ops(self):
        Site(self, hook="#!/bin/bash\nset -e -u\nexit 0\n").run()

    def test_a_no_op_hook_is_ignored(self):
        Site(self, hook=NOOP_HOOK).run()

    def test_unset_required_env_is_reported_not_fatal(self):
        info = '{"requires_env": [{"name": "CDSAPI_KEY", "required": true}]}'
        result = Site(self, requires_env=info).run()
        self.assertEqual(result.warnings, ["georiva_source_cds needs CDSAPI_KEY, which is not set"])

    def test_set_required_env_is_not_reported(self):
        info = '{"requires_env": [{"name": "CDSAPI_KEY", "required": true}]}'
        result = Site(self, requires_env=info).run(environ={"CDSAPI_KEY": "k"})
        self.assertEqual(result.warnings, [])


TWO_PLUGINS = MANIFEST + textwrap.dedent(
    """
    [[plugins]]
    name = "Forti"
    git  = "https://example.org/georiva-publisher-forti.git"
    tag  = "0.2.0"
    """
)


class InstalledRecordTests(SimpleTestCase):
    """``installed.json``: what this server runs, and what a run changed."""

    def test_the_install_records_each_plugin_s_source_tag_version_and_commit(self):
        site = Site(self)
        site.info_version, site.commit = "1.0.0", "a1b2c3d4e5f6"
        site.run()

        record = json.loads((site.site / INSTALLED).read_text())
        self.assertEqual(
            record["georiva_source_cds"],
            {
                "name": "CDS",
                "source": "https://example.org/georiva-source-cds.git",
                "tag": "1.0.0",
                "version": "1.0.0",
                "commit": "a1b2c3d4e5f6",
            },
        )

    def test_the_version_falls_back_to_the_pyproject_then_the_tag(self):
        site = Site(self)
        site.pyproject_version = "0.9.0"
        site.run()
        self.assertEqual(json.loads((site.site / INSTALLED).read_text())["georiva_source_cds"]["version"], "0.9.0")

        site.pyproject_version = None
        site.run(force=True)
        self.assertEqual(json.loads((site.site / INSTALLED).read_text())["georiva_source_cds"]["version"], "1.0.0")

    def test_a_first_install_reports_every_plugin_as_added(self):
        site = Site(self)
        site.info_version = "1.0.0"
        result = site.run()
        self.assertEqual([(c.kind, c.package) for c in result.changes], [("+", "georiva_source_cds")])

    def test_a_tag_bump_reports_the_old_and_new_version(self):
        site = Site(self)
        site.info_version = "1.0.0"
        site.run()
        site.manifest.write_text(MANIFEST.replace("1.0.0", "1.1.0"))
        site.info_version = "1.1.0"

        (change,) = site.run().changes
        self.assertEqual((change.kind, change.old["version"], change.new["version"]), ("~", "1.0.0", "1.1.0"))

    def test_a_new_commit_at_the_same_version_is_a_change(self):
        """An untagged plugin follows its branch: --force picks up a new commit,
        and the version alone would call that unchanged."""
        site = Site(self)
        site.info_version, site.commit = "1.0.0", "a1b2c3d"
        site.run()
        site.commit = "e4f5a6b"

        (change,) = site.run(force=True).changes
        self.assertEqual(change.kind, "~")

    def test_an_identical_reinstall_reports_unchanged(self):
        site = Site(self)
        site.info_version = "1.0.0"
        site.run()
        (change,) = site.run(force=True).changes
        self.assertEqual(change.kind, "=")

    def test_a_plugin_dropped_from_the_manifest_is_reported_removed(self):
        site = Site(self, manifest=TWO_PLUGINS)
        site.packages = ["georiva_source_cds", "georiva_publisher_forti"]
        site.run()
        site.manifest.write_text(MANIFEST)
        site.fetched.clear()

        kinds = {c.package: c.kind for c in site.run().changes}
        self.assertEqual(kinds, {"georiva_source_cds": "=", "georiva_publisher_forti": "-"})

    def test_an_up_to_date_run_reports_what_is_running_and_leaves_the_record_alone(self):
        site = Site(self)
        site.info_version = "1.0.0"
        site.run()
        before = (site.site / INSTALLED).read_text()

        result = site.run()
        self.assertFalse(result.installed)
        self.assertEqual([(c.kind, c.package) for c in result.changes], [("=", "georiva_source_cds")])
        self.assertEqual((site.site / INSTALLED).read_text(), before)

    def test_a_failed_install_keeps_the_previous_record(self):
        site = Site(self)
        site.info_version = "1.0.0"
        site.run(tag="v1")
        before = (site.site / INSTALLED).read_text()
        site.pip_error = "conflict"
        with self.assertRaises(InstallError):
            site.run(tag="v2")
        self.assertEqual((site.site / INSTALLED).read_text(), before)

    def test_the_report_reads_as_one_line_per_plugin(self):
        site = Site(self)
        site.info_version, site.commit = "0.2.0", "c3d812a0000"
        site.run()
        site.manifest.write_text(MANIFEST.replace("1.0.0", "0.2.1"))
        site.info_version, site.commit = "0.2.1", "5703bd80000"

        (line,) = report(site.run().changes)
        self.assertEqual(
            line,
            "~ georiva_source_cds  0.2.0 → 0.2.1  (git 0.2.1 @ 5703bd8)",
        )


class ReportContractTests(SimpleTestCase):
    def test_the_lines_deploy_sh_reads_are_pinned(self):
        """``deploy/deploy.sh plugins`` restarts the backend only when it sees the
        first line. Changing either one silently stops plugin updates restarting
        anything — change deploy.sh in the same commit."""
        self.assertEqual(INSTALLED_LINE, "install-plugins: installed")
        self.assertEqual(UP_TO_DATE_LINE, "install-plugins: up to date")


class ManifestTests(SimpleTestCase):
    def test_disabled_entries_are_skipped(self):
        with tempfile.NamedTemporaryFile("w", suffix=".toml") as f:
            f.write(MANIFEST)
            f.flush()
            self.assertEqual([s.name for s in load_specs(Path(f.name))], ["CDS"])

    def test_a_git_tag_is_carried(self):
        with tempfile.NamedTemporaryFile("w", suffix=".toml") as f:
            f.write(MANIFEST)
            f.flush()
            self.assertEqual(load_specs(Path(f.name))[0].tag, "1.0.0")

    def test_a_folder_entry_is_refused(self):
        with tempfile.NamedTemporaryFile("w", suffix=".toml") as f:
            f.write('[[plugins]]\nname="x"\nfolder="/a"\n')
            f.flush()
            with self.assertRaisesRegex(ValueError, "git.*url"):
                load_specs(Path(f.name))

    def test_a_missing_manifest_is_no_plugins(self):
        self.assertEqual(load_specs(Path("/nonexistent/plugins.toml")), [])
