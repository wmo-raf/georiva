"""Plugin-contributed routes under ``/api/``.

The convention is the registration: a plugin publishes an ``api_urls`` module
and it is included. That makes two things load-bearing, and both fail in ways
nothing reports:

- **core is matched first.** The resolver stops at the first match, so a plugin
  declaring a path core already answers must lose. Were it to win, installing a
  plugin would silently move ``/api/stac/``.
- **a plugin that will not import must not be fatal.** The URL conf is built once
  for the whole instance, so an exception raised while assembling it reaches
  every request, not just the routes of the plugin that caused it.

The fixtures are real packages on a real ``sys.path`` entry rather than modules
pushed into ``sys.modules``, because the behaviour under test *is* the import
system's: telling "this plugin publishes no routes" from "this plugin publishes
routes and is broken" is a question about the finder, and a hand-built module
would answer it without ever asking one.
"""

import importlib
import shutil
import sys
import tempfile
import textwrap
from pathlib import Path
from types import ModuleType

from django.test import SimpleTestCase, override_settings
from django.urls import resolve, reverse

from georiva.api import urls as api_urls
from georiva.core import plugins

WORKING = """
from django.http import HttpResponse
from django.urls import path


def forecast(request):
    return HttpResponse("from the plugin")


urlpatterns = [path("forecast/", forecast, name="forecast")]
"""

#: The trap `_import_api_urls` exists for: an ``api_urls`` that is present and
#: raises ``ModuleNotFoundError`` on import, which is the same exception the
#: import system raises for a plugin that has no ``api_urls`` at all.
BROKEN = """
import a_dependency_the_operator_did_not_install  # noqa: F401

urlpatterns = []
"""

SHADOWING = """
from django.http import HttpResponse
from django.urls import path


def usurper(request):
    return HttpResponse("not core's STAC")


urlpatterns = [path("stac/", usurper, name="stac-landing")]
"""

NAMESPACED = """
from django.http import HttpResponse
from django.urls import path


def forecast(request):
    return HttpResponse("from the plugin")


app_name = "demo"

urlpatterns = [path("demo/forecast/", forecast, name="forecast")]
"""


class PluginApiUrlTests(SimpleTestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.made = []
        sys.path.insert(0, str(self.root))
        self.addCleanup(self.cleanup)

    def cleanup(self):
        sys.path.remove(str(self.root))
        for name in self.made:
            for key in [k for k in sys.modules if k == name or k.startswith(f"{name}.")]:
                del sys.modules[key]
        shutil.rmtree(self.root)
        importlib.invalidate_caches()

    def make_plugin(self, name, api_urls=None):
        """A plugin package on the path, with or without an ``api_urls``."""
        package = self.root / name
        package.mkdir()
        (package / "__init__.py").write_text("")
        if api_urls is not None:
            (package / "api_urls.py").write_text(textwrap.dedent(api_urls))
        self.made.append(name)
        importlib.invalidate_caches()
        return name

    def patterns_for(self, *names):
        with override_settings(GEORIVA_PLUGIN_NAMES=list(names)):
            return plugins.api_urlpatterns()

    def urlconf(self, patterns):
        """A throwaway URL conf. A real module and not a namespace: Django caches
        resolvers on the urlconf itself, so it has to be hashable."""
        module = ModuleType(f"urlconf_{len(self.made)}_{id(patterns)}")
        module.urlpatterns = patterns
        return module

    def test_a_plugin_with_api_urls_contributes_them(self):
        name = self.make_plugin("plugin_with_routes", WORKING)

        urlconf = self.urlconf(self.patterns_for(name))

        self.assertEqual(resolve("/forecast/", urlconf=urlconf).func.__name__, "forecast")

    def test_a_plugin_without_api_urls_contributes_nothing_and_says_nothing(self):
        """The ordinary case. Most plugins publish no routes, so it must not be
        reported as anything."""
        name = self.make_plugin("plugin_without_routes")

        with self.assertNoLogs("georiva.core.plugins", level="WARNING"):
            self.assertEqual(self.patterns_for(name), [])

    def test_a_broken_api_urls_is_logged_and_skipped(self):
        """The case the finder check exists to separate from the one above."""
        name = self.make_plugin("plugin_with_broken_routes", BROKEN)

        with self.assertLogs("georiva.core.plugins", level="ERROR") as logged:
            self.assertEqual(self.patterns_for(name), [])

        self.assertIn("will not import", logged.output[0])
        self.assertIn(name, logged.output[0])

    def test_a_broken_plugin_does_not_cost_a_working_one_its_routes(self):
        """The blast radius. One plugin's fault is one plugin's routes."""
        broken = self.make_plugin("plugin_broken_neighbour", BROKEN)
        working = self.make_plugin("plugin_working_neighbour", WORKING)

        with self.assertLogs("georiva.core.plugins", level="ERROR"):
            urlconf = self.urlconf(self.patterns_for(broken, working))

        self.assertEqual(resolve("/forecast/", urlconf=urlconf).func.__name__, "forecast")

    def test_an_api_urls_with_no_urlpatterns_is_logged_and_skipped(self):
        name = self.make_plugin("plugin_with_empty_routes", "urlpatterns = []\n")

        with self.assertLogs("georiva.core.plugins", level="WARNING") as logged:
            self.assertEqual(self.patterns_for(name), [])

        self.assertIn("no urlpatterns", logged.output[0])

    def test_a_plugin_cannot_shadow_a_core_route(self):
        """Asserted against core's real ``/api/`` table, not a stand-in, because
        the property is about where the plugin patterns are appended."""
        name = self.make_plugin("plugin_that_wants_stac", SHADOWING)

        urlconf = self.urlconf(list(api_urls.urlpatterns) + self.patterns_for(name))

        match = resolve("/stac/", urlconf=urlconf)
        self.assertEqual(match.func.view_class.__name__, "STACLandingPageView")

    def test_a_plugin_may_namespace_its_routes(self):
        """``include`` is handed the module, so a plugin declaring ``app_name``
        gets the namespace it asked for and can be reversed by it."""
        name = self.make_plugin("plugin_with_a_namespace", NAMESPACED)

        urlconf = self.urlconf(self.patterns_for(name))

        self.assertEqual(reverse("demo:forecast", urlconf=urlconf), "/demo/forecast/")
