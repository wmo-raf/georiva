import tempfile
import textwrap
from pathlib import Path

from django.test import SimpleTestCase

from georiva.site_plugins.installer import InstallError, install, stamp_for
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

    def fetch(self, spec: PluginSpec, dest: Path):
        self.fetched.append(spec.git or spec.url)
        (dest / "src" / "georiva_source_cds").mkdir(parents=True)
        (dest / "src" / "georiva_source_cds" / "__init__.py").write_text("")
        (dest / "pyproject.toml").write_text('[project]\nname = "x"\ndependencies = ["cdsapi>=0.7", "numpy"]\n')
        if self.hook is not None:
            (dest / "build.sh").write_text(self.hook)
        if self.requires_env is not None:
            (dest / "georiva_plugin_info.json").write_text(self.requires_env)

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
