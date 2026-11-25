"""The style editor's generator endpoint and page script (#801, #806).

Two seams:

* the generator endpoint — the stops a ramp style's editor fills itself with
  whenever its ramp, mode, classes or reverse change, reversed when asked;
* the rendered page — its one disclosure (the shared range) reads as one, and
  the page script reaches the unsaved guard only through a fallback.

What each origin's editor shows is in ``test_style_origins``. Live
regeneration itself is JavaScript and is verified by hand, like the map.
"""

import re

from georiva.core.models import VariableStyle
from georiva.core.models.visualization import served_ramp_stops
from georiva.core.tests.test_styling_surface import StylingSurfaceTestCase


class ReversedGenerationTests(StylingSurfaceTestCase):
    def test_reverse_flips_the_colours_against_fixed_values(self):
        response = self.client.get(
            self.stops_url, {"ramp": str(self.org_ramp.pk), "mode": "continuous", "reverse": "1"}
        )

        self.assertEqual(
            response.json()["stops"],
            [{"value": 0.0, "color": "#ffffff"}, {"value": 50.0, "color": "#000000"}],
        )

    def test_reverse_reads_like_any_ticked_checkbox(self):
        forward = self.client.get(self.stops_url, {"ramp": str(self.org_ramp.pk), "mode": "continuous"}).json()
        for ticked in ("1", "true", "on"):
            with self.subTest(reverse=ticked):
                stops = self.client.get(
                    self.stops_url, {"ramp": str(self.org_ramp.pk), "mode": "continuous", "reverse": ticked}
                ).json()["stops"]
                self.assertEqual(stops[0]["color"], forward["stops"][-1]["color"])

    def test_a_reversed_stepped_ramp_keeps_its_class_pairs(self):
        forward = served_ramp_stops(self.org_ramp, self.variable, mode="stepped", steps=3)
        response = self.client.get(
            self.stops_url, {"ramp": str(self.org_ramp.pk), "mode": "stepped", "steps": "3", "reverse": "1"}
        )

        stops = response.json()["stops"]
        self.assertEqual([s["value"] for s in stops], [s["value"] for s in forward])
        self.assertEqual([s["color"] for s in stops], [s["color"] for s in reversed(forward)])


STOPS = [{"value": 0.0, "color": "#000000"}, {"value": 50.0, "color": "#ffffff"}]


class DisclosureMarkerTests(StylingSurfaceTestCase):
    """The shared range is a disclosure (#798): its summary carries the toggle
    marker, turned when open, the way Wagtail's collapsible panels do."""

    def test_the_range_summary_carries_the_disclosure_marker(self):
        VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official", is_default=True, stops=STOPS
        )
        html = self.client.get(self.form_url).content.decode()

        summary = re.search(r'<details[^>]*id="vs-range"[^>]*>\s*(<summary[^>]*>.*?</summary>)', html, re.S).group(1)
        self.assertRegex(summary, r'<summary[^>]*class="[^"]*\bvs-disclosure\b')
        self.assertIn("vs-disclosure-icon", summary)


class GuardFallbackTests(StylingSurfaceTestCase):
    """Live regeneration never depends on unsaved_guard.js loading (#798): the
    page script reaches the guard's functions only through a fallback."""

    def test_the_page_script_never_calls_the_guard_unguarded(self):
        VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official", is_default=True, stops=STOPS
        )
        html = self.client.get(self.form_url).content.decode()
        page_script = html[html.index("getElementById('vs-style-form')") :]

        self.assertNotRegex(page_script, r"window\.mark(Style|Range)Dirty\(\)")
