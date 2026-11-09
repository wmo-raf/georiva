"""Plugin-contributed page routes (ADR 0039).

A plugin that ships a built bundle declares a ``web_app`` module and core mounts
it under the reserved page prefix, serving the bundle's ``index.html`` and
falling back to it for unknown sub-paths. Four things are load-bearing:

- **the mount label is the operator's**, with the plugin's own only a default, so
  the address an operator gives their users is not an artefact of how the bundle
  got there;
- **the prefix is reserved**, so a page route cannot take a path core already
  answers — the same rule ADR 0028 gave ``/api/``, enforced here by the prefix as
  well as by ordering;
- **an unknown sub-path serves the entry document**, which is the whole of what
  client-side routing needs, while an asset-shaped sub-path must 404 rather than
  answer a script request with HTML;
- **a plugin that will not import must not be fatal**, because the URL conf is
  built once for the whole instance.

Fixtures are real packages on a real ``sys.path`` entry, as in
``test_plugin_api_urls``, because what is under test is partly the import
system's own behaviour. The bundle lives in a directory on ``STATICFILES_DIRS`` —
the staticfiles finders are how a mount resolves its entry document, so a test
that bypassed them would not be testing the mount.
"""

import importlib
import shutil
import sys
import tempfile
import textwrap
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType

from django.contrib.staticfiles import finders
from django.http import Http404
from django.test import RequestFactory, SimpleTestCase, override_settings
from django.urls import resolve, reverse

from georiva.config import urls as config_urls
from georiva.core import web_apps

WORKING = """
from georiva.core.web_apps import WebApp

app = WebApp(slug="demo-viewer", index="demo_viewer/index.html", name="Demo Viewer")
"""

BROKEN = """
import a_dependency_the_operator_did_not_install  # noqa: F401

from georiva.core.web_apps import WebApp

app = WebApp(slug="broken", index="broken/index.html")
"""

NOT_AN_APP = """
app = "a string is not a WebApp"
"""


class PluginWebAppTests(SimpleTestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.static = Path(tempfile.mkdtemp())
        self.made = []
        sys.path.insert(0, str(self.root))
        self.addCleanup(self.cleanup)

    def cleanup(self):
        sys.path.remove(str(self.root))
        for name in self.made:
            for key in [k for k in sys.modules if k == name or k.startswith(f"{name}.")]:
                del sys.modules[key]
        shutil.rmtree(self.root)
        shutil.rmtree(self.static)
        importlib.invalidate_caches()
        finders.get_finder.cache_clear()

    def make_plugin(self, name, web_app=None):
        """A plugin package on the path, with or without a ``web_app``."""
        package = self.root / name
        package.mkdir()
        (package / "__init__.py").write_text("")
        if web_app is not None:
            (package / "web_app.py").write_text(textwrap.dedent(web_app))
        self.made.append(name)
        importlib.invalidate_caches()
        return name

    def make_bundle(self, index="demo_viewer/index.html", body="<title>the app</title>"):
        """A built bundle's entry document, where the finders will see it."""
        entry = self.static / index
        entry.parent.mkdir(parents=True, exist_ok=True)
        entry.write_text(body)
        return index

    @contextmanager
    def instance(self, *names, **overrides):
        """Core's settings with a given set of plugins discovered and the fixture
        bundle directory on the static path. The finder cache is cleared on the
        way in and out: the finders read ``STATICFILES_DIRS`` once, at
        construction, and are then memoised for the process."""
        with override_settings(GEORIVA_PLUGIN_NAMES=list(names), STATICFILES_DIRS=[str(self.static)], **overrides):
            finders.get_finder.cache_clear()
            try:
                yield
            finally:
                finders.get_finder.cache_clear()

    def urlconf(self, patterns):
        """A throwaway URL conf. A real module and not a namespace: Django caches
        resolvers on the urlconf itself, so it has to be hashable."""
        module = ModuleType(f"urlconf_{len(self.made)}_{id(patterns)}")
        module.urlpatterns = patterns
        return module

    def get(self, urlconf, url):
        """The mounted view, called the way the resolver would call it. Not the
        test client: that would run the instance's middleware, which answers
        questions of its own that this module is not asking."""
        match = resolve(url, urlconf=urlconf)
        return match.func(RequestFactory().get(url), *match.args, **match.kwargs)

    # -- what gets mounted -------------------------------------------------

    def test_a_plugin_with_a_web_app_is_mounted_under_the_reserved_prefix(self):
        name = self.make_plugin("plugin_with_a_web_app", WORKING)

        with self.instance(name):
            mounts = web_apps.web_app_mounts()

        self.assertEqual([m.path for m in mounts], ["/app/demo-viewer/"])
        self.assertEqual(mounts[0].plugin, name)
        self.assertEqual(mounts[0].name, "Demo Viewer")

    def test_a_plugin_without_a_web_app_contributes_nothing_and_says_nothing(self):
        """The ordinary case: most plugins ship no bundle at all."""
        name = self.make_plugin("plugin_without_a_web_app")

        with self.instance(name), self.assertNoLogs("georiva.core.web_apps", level="WARNING"):
            self.assertEqual(web_apps.web_app_mounts(), [])

    def test_a_broken_web_app_is_logged_and_skipped(self):
        name = self.make_plugin("plugin_with_a_broken_web_app", BROKEN)

        with self.instance(name), self.assertLogs("georiva.core.web_apps", level="ERROR") as logged:
            self.assertEqual(web_apps.web_app_mounts(), [])

        self.assertIn("will not import", logged.output[0])
        self.assertIn(name, logged.output[0])

    def test_a_broken_plugin_does_not_cost_a_working_one_its_page(self):
        """The blast radius. One plugin's fault is one plugin's page."""
        broken = self.make_plugin("plugin_broken_web_neighbour", BROKEN)
        working = self.make_plugin("plugin_working_web_neighbour", WORKING)

        with self.instance(broken, working), self.assertLogs("georiva.core.web_apps", level="ERROR"):
            mounts = web_apps.web_app_mounts()

        self.assertEqual([m.plugin for m in mounts], [working])

    def test_a_web_app_module_without_a_web_app_is_logged_and_skipped(self):
        name = self.make_plugin("plugin_with_a_non_app", NOT_AN_APP)

        with self.instance(name), self.assertLogs("georiva.core.web_apps", level="WARNING") as logged:
            self.assertEqual(web_apps.web_app_mounts(), [])

        self.assertIn(name, logged.output[0])

    def test_a_slug_that_is_not_one_path_segment_is_refused(self):
        """A slug is one segment of a URL this instance will answer on. Anything
        else — a slash, a dot-dot, an empty string — is a plugin claiming more
        than one mount, so it gets none."""
        name = self.make_plugin(
            "plugin_with_a_bad_slug",
            """
            from georiva.core.web_apps import WebApp

            app = WebApp(slug="../escape", index="x/index.html")
            """,
        )

        with self.instance(name), self.assertLogs("georiva.core.web_apps", level="ERROR") as logged:
            self.assertEqual(web_apps.web_app_mounts(), [])

        self.assertIn("slug", logged.output[0])

    def test_an_index_outside_the_static_tree_is_refused(self):
        """The index is a staticfiles path, resolved by the finders. A plugin
        that writes an absolute or climbing path is asking for a file the
        finders would never have given it."""
        name = self.make_plugin(
            "plugin_with_a_climbing_index",
            """
            from georiva.core.web_apps import WebApp

            app = WebApp(slug="climber", index="../../etc/passwd")
            """,
        )

        with self.instance(name), self.assertLogs("georiva.core.web_apps", level="ERROR") as logged:
            self.assertEqual(web_apps.web_app_mounts(), [])

        self.assertIn("index", logged.output[0])

    def test_two_plugins_cannot_take_the_same_mount(self):
        """Unlike ``/api/``, where arbitrating a collision would have meant a
        package name in every public route (ADR 0028), the mount label is ours to
        police: the first in discovery order keeps it and the second is told."""
        first = self.make_plugin("plugin_first_at_the_mount", WORKING)
        second = self.make_plugin("plugin_second_at_the_mount", WORKING)

        with self.instance(first, second), self.assertLogs("georiva.core.web_apps", level="ERROR") as logged:
            mounts = web_apps.web_app_mounts()

        self.assertEqual([m.plugin for m in mounts], [first])
        self.assertIn(second, logged.output[0])
        self.assertIn("demo-viewer", logged.output[0])

    # -- the operator's choice of label ------------------------------------

    def test_an_operator_chooses_the_label(self):
        """The plugin's slug is a default. The address users are given is the
        operator's to pick, which is the whole point of the mechanism."""
        name = self.make_plugin("plugin_with_a_relabelled_app", WORKING)

        with self.instance(name, GEORIVA_WEB_APP_MOUNTS={"plugin_with_a_relabelled_app": "viewer"}):
            mounts = web_apps.web_app_mounts()

        self.assertEqual([m.path for m in mounts], ["/app/viewer/"])

    def test_an_operator_chooses_the_prefix_too(self):
        name = self.make_plugin("plugin_under_another_prefix", WORKING)

        with self.instance(name, GEORIVA_WEB_APP_PATH_PREFIX="/apps/"):
            mounts = web_apps.web_app_mounts()

        self.assertEqual([m.path for m in mounts], ["/apps/demo-viewer/"])

    def test_an_empty_prefix_mounts_at_the_site_root(self):
        """Allowed, and the operator's own risk: the label then shares a space
        with the page tree's own slugs."""
        name = self.make_plugin("plugin_at_the_root", WORKING)

        with self.instance(name, GEORIVA_WEB_APP_PATH_PREFIX=""):
            mounts = web_apps.web_app_mounts()

        self.assertEqual([m.path for m in mounts], ["/demo-viewer/"])

    def test_an_operator_choice_displaces_another_plugins_default(self):
        """The round the labels are claimed in. A rename the operator wrote down
        must not be refused by a default label nobody chose — the plugin holding
        it by default is the one that loses."""
        holder = self.make_plugin("plugin_holding_the_label_by_default", WORKING)
        renamed = self.make_plugin(
            "plugin_sent_to_the_same_label",
            """
            from georiva.core.web_apps import WebApp

            app = WebApp(slug="other", index="demo_viewer/index.html")
            """,
        )

        with (
            self.instance(holder, renamed, GEORIVA_WEB_APP_MOUNTS={"plugin_sent_to_the_same_label": "demo-viewer"}),
            self.assertLogs("georiva.core.web_apps", level="ERROR") as logged,
        ):
            mounts = web_apps.web_app_mounts()

        self.assertEqual([m.plugin for m in mounts], [renamed])
        self.assertIn(holder, logged.output[0])

    def test_two_operator_choices_at_one_label_leave_the_first_standing(self):
        first = self.make_plugin("plugin_relabelled_first", WORKING)
        second = self.make_plugin("plugin_relabelled_second", WORKING)

        with (
            self.instance(
                first,
                second,
                GEORIVA_WEB_APP_MOUNTS={"plugin_relabelled_first": "viewer", "plugin_relabelled_second": "viewer"},
            ),
            self.assertLogs("georiva.core.web_apps", level="ERROR"),
        ):
            mounts = web_apps.web_app_mounts()

        self.assertEqual([m.plugin for m in mounts], [first])

    def test_a_label_for_a_plugin_that_is_not_installed_is_reported(self):
        """A mistyped plugin name would otherwise leave the app at its default
        address with nothing said, which is the question this mechanism exists to
        answer."""
        name = self.make_plugin("plugin_named_correctly", WORKING)

        with (
            self.instance(name, GEORIVA_WEB_APP_MOUNTS={"plugin_naemd_wrongly": "viewer"}),
            self.assertLogs("georiva.core.web_apps", level="WARNING") as logged,
        ):
            mounts = web_apps.web_app_mounts()

        self.assertEqual([m.path for m in mounts], ["/app/demo-viewer/"])
        self.assertIn("plugin_naemd_wrongly", logged.output[0])

    def test_an_operator_label_that_is_not_one_path_segment_is_refused(self):
        name = self.make_plugin("plugin_relabelled_badly", WORKING)

        with (
            self.instance(name, GEORIVA_WEB_APP_MOUNTS={"plugin_relabelled_badly": "a/b"}),
            self.assertLogs("georiva.core.web_apps", level="ERROR"),
        ):
            self.assertEqual(web_apps.web_app_mounts(), [])

    # -- what the mount serves ---------------------------------------------

    def test_the_mount_serves_the_bundles_entry_document(self):
        name = self.make_plugin("plugin_serving_its_bundle", WORKING)
        self.make_bundle(body="<title>the app</title>")

        with self.instance(name):
            response = self.get(self.urlconf(web_apps.web_app_urlpatterns()), "/app/demo-viewer/")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "text/html; charset=utf-8")
        self.assertIn("<title>the app</title>", response.content.decode())

    def test_an_unknown_sub_path_serves_the_entry_document(self):
        """What a shareable URL needs: the server knows nothing about the app's
        own routes, so every path under the mount is the same document."""
        name = self.make_plugin("plugin_with_client_routing", WORKING)
        self.make_bundle(body="<title>the app</title>")

        with self.instance(name):
            response = self.get(
                self.urlconf(web_apps.web_app_urlpatterns()),
                "/app/demo-viewer/run/2026-01-01T00:00Z/wind",
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("<title>the app</title>", response.content.decode())

    def test_an_asset_shaped_sub_path_is_not_answered_with_the_document(self):
        """A bundle's assets are served from ``/static/``. A request under the
        mount for something with a file extension is a mistaken asset reference,
        and HTML in its place turns a 404 into a parse error somewhere else."""
        name = self.make_plugin("plugin_asked_for_an_asset", WORKING)
        self.make_bundle()

        with self.instance(name), self.assertRaises(Http404):
            self.get(self.urlconf(web_apps.web_app_urlpatterns()), "/app/demo-viewer/assets/index-abc123.js")

    def test_the_entry_document_is_not_cached_by_the_client(self):
        """It is a document, not a fingerprinted asset: an operator who replaces
        the bundle must not have to wait out a cache they did not choose."""
        name = self.make_plugin("plugin_with_fresh_html", WORKING)
        self.make_bundle()

        with self.instance(name):
            response = self.get(self.urlconf(web_apps.web_app_urlpatterns()), "/app/demo-viewer/")

        self.assertIn("no-cache", response["Cache-Control"])

    def test_a_missing_entry_document_is_a_404_and_is_logged(self):
        """The bundle is resolved per request, not at startup, so a plugin whose
        bundle was never built costs its own page and nothing else."""
        name = self.make_plugin("plugin_without_a_bundle", WORKING)

        with self.instance(name):
            urlconf = self.urlconf(web_apps.web_app_urlpatterns())
            with self.assertLogs("georiva.core.web_apps", level="ERROR") as logged, self.assertRaises(Http404):
                self.get(urlconf, "/app/demo-viewer/")

        self.assertIn("demo_viewer/index.html", logged.output[0])

    def test_the_mount_is_reversible_by_plugin(self):
        """So a plugin's admin surface can link to its own app without writing
        down a label the operator chose."""
        name = self.make_plugin("plugin_linking_to_itself", WORKING)

        with self.instance(name):
            urlconf = self.urlconf(web_apps.web_app_urlpatterns())

        self.assertEqual(reverse(f"web_app_{name}", urlconf=urlconf), "/app/demo-viewer/")

    # -- the reserved prefix -----------------------------------------------

    def test_a_page_route_cannot_take_a_segment_core_answers_on(self):
        """Not just the paths core answers: the paths core *refuses*.

        Appending the mounts after core's patterns is not enough on its own,
        because Django keeps looking past an ``include`` whose inner patterns did
        not match. A plugin labelled ``api`` at the root would leave
        ``/api/stac/`` alone and still answer every unmatched path under
        ``/api/`` with its own HTML, so core's 404s would become one plugin's
        front page. The whole segment is refused.
        """
        name = self.make_plugin(
            "plugin_that_wants_api",
            """
            from georiva.core.web_apps import WebApp

            app = WebApp(slug="api", index="demo_viewer/index.html")
            """,
        )
        self.make_bundle()

        with (
            self.instance(name, GEORIVA_WEB_APP_PATH_PREFIX=""),
            self.assertLogs("georiva.core.web_apps", level="ERROR") as logged,
        ):
            urlconf = self.urlconf(config_urls.build_urlpatterns())

            self.assertEqual(resolve("/api/stac/", urlconf=urlconf).func.view_class.__name__, "STACLandingPageView")
            # Nothing raises here: the page tree's catch-all answers for every
            # path and 404s in its view. What matters is who it is *not*.
            refused = resolve("/api/not-a-route-core-has/", urlconf=urlconf)
            self.assertIsNot(getattr(refused.func, "view_class", None), web_apps.WebAppIndexView)

        self.assertIn("plugin_that_wants_api", logged.output[0])

    def test_a_prefix_pointed_at_cores_space_serves_no_page_at_all(self):
        """The operator's own version of the same mistake. Every mount would be
        under it, so none is served and the prefix is named."""
        name = self.make_plugin("plugin_under_a_bad_prefix", WORKING)

        with (
            self.instance(name, GEORIVA_WEB_APP_PATH_PREFIX="api/"),
            self.assertLogs("georiva.core.web_apps", level="ERROR") as logged,
        ):
            self.assertEqual(web_apps.web_app_mounts(config_urls.reserved_segments()), [])

        self.assertIn("GEORIVA_WEB_APP_PATH_PREFIX", logged.output[0])

    def test_the_default_prefix_takes_nothing_of_cores(self):
        """Why the default prefix exists at all: with it, no label can collide
        with core, whatever a plugin asks for."""
        name = self.make_plugin(
            "plugin_that_wants_api_under_the_prefix",
            """
            from georiva.core.web_apps import WebApp

            app = WebApp(slug="api", index="demo_viewer/index.html")
            """,
        )

        with self.instance(name):
            mounts = web_apps.web_app_mounts(config_urls.reserved_segments())

        self.assertEqual([m.path for m in mounts], ["/app/api/"])

    def test_a_page_route_is_reached_before_the_page_tree(self):
        """The other half of the ordering: the mounts are matched before
        Wagtail's catch-all, or an app at the root would never be served."""
        name = self.make_plugin("plugin_before_the_page_tree", WORKING)
        self.make_bundle()

        with self.instance(name, GEORIVA_WEB_APP_PATH_PREFIX=""):
            urlconf = self.urlconf(config_urls.build_urlpatterns())

            response = self.get(urlconf, "/demo-viewer/")

        self.assertEqual(response.status_code, 200)

    # -- how the portal presents it ----------------------------------------

    def test_an_app_may_say_how_the_portal_presents_it(self):
        name = self.make_plugin(
            "plugin_that_describes_itself",
            """
            from georiva.core.web_apps import WebApp

            app = WebApp(
                slug="demo-viewer",
                index="demo_viewer/index.html",
                name="Demo Viewer",
                icon="cloud-sun",
                description="Look at the demo.",
            )
            """,
        )

        with self.instance(name):
            (mount,) = web_apps.web_app_mounts()

        self.assertEqual(mount.icon, "cloud-sun")
        self.assertEqual(mount.description, "Look at the demo.")

    def test_an_app_that_says_nothing_gets_a_generic_icon_and_no_description(self):
        """Presentation is optional: a plugin declared before it existed keeps its page."""
        name = self.make_plugin("plugin_that_describes_nothing", WORKING)

        with self.instance(name):
            (mount,) = web_apps.web_app_mounts()

        self.assertEqual(mount.icon, web_apps.DEFAULT_ICON)
        self.assertEqual(mount.description, "")

    def test_an_icon_may_be_written_with_its_class_prefix(self):
        name = self.make_plugin(
            "plugin_with_a_prefixed_icon",
            """
            from georiva.core.web_apps import WebApp

            app = WebApp(slug="demo-viewer", index="demo_viewer/index.html", icon="bi-cloud-sun")
            """,
        )

        with self.instance(name):
            (mount,) = web_apps.web_app_mounts()

        self.assertEqual(mount.icon, "cloud-sun")

    def test_the_served_apps_are_read_back_from_the_url_conf(self):
        """Once per app, not once per pattern, and only what the URL conf routes:
        core's own patterns contribute nothing."""
        name = self.make_plugin("plugin_read_back", WORKING)

        with self.instance(name):
            urlconf = self.urlconf(config_urls.build_urlpatterns())

            served = web_apps.served_web_apps(urlconf)

        self.assertEqual([(m.plugin, m.path) for m in served], [(name, "/app/demo-viewer/")])

    def test_the_served_address_follows_an_operators_rename(self):
        """A link is to wherever the app is actually served, not its default slug."""
        name = self.make_plugin("plugin_read_back_renamed", WORKING)

        with self.instance(name, GEORIVA_WEB_APP_MOUNTS={name: "viewer"}, GEORIVA_WEB_APP_PATH_PREFIX="tools/"):
            urlconf = self.urlconf(config_urls.build_urlpatterns())

            served = web_apps.served_web_apps(urlconf)

        self.assertEqual([m.path for m in served], ["/tools/viewer/"])
