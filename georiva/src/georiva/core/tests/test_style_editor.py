"""The style editor's basic and advanced views, and live ramp regeneration (#801).

Three seams:

* the hand-tuning check on ``VariableStyle`` — no field records it: the style's
  saved ramp, mode and steps are regenerated over the Variable's current range
  and compared to the stored stops (ADR 0022 amendment);
* the generator endpoint — the stops a ramp change fills the form with,
  reversed when asked;
* the rendered page — basic controls in view, the stops behind "Advanced:
  fine-tune stops", open by itself for a hand-tuned or template style.

Live regeneration itself is JavaScript and is verified by hand, like the map;
what is tested here is the state the page opens in and what it is told.
"""

import re

from georiva.core.models import StyleTemplate, VariableStyle
from georiva.core.models.visualization import served_ramp_stops
from georiva.core.tests.test_styling_surface import StylingSurfaceTestCase


class HandTuningCheckTests(StylingSurfaceTestCase):
    """Stops are "as generated" only while they are exactly what the style's own
    ramp, mode and steps produce over the Variable's current range."""

    def style(self, **fields):
        fields.setdefault("ramp", self.org_ramp)
        style = VariableStyle(variable=self.variable, name="Official", slug="official", is_default=True, **fields)
        if "stops" not in fields:
            style.apply_ramp()
        style.save()
        return style

    def test_stops_the_ramp_generated_are_not_hand_tuned(self):
        style = self.style()

        self.assertFalse(style.is_hand_tuned)
        self.assertFalse(style.as_generated().reverse)

    def test_a_stepped_style_as_generated_is_not_hand_tuned(self):
        style = self.style(mode=VariableStyle.Mode.STEPPED, steps=5)

        self.assertFalse(style.is_hand_tuned)

    def test_a_reversed_ramp_still_reads_as_generated_and_says_so(self):
        # make_org_tree's variable spans 0–50; the org ramp runs black→white.
        style = self.style(stops=[{"value": 0.0, "color": "#ffffff"}, {"value": 50.0, "color": "#000000"}])

        self.assertFalse(style.is_hand_tuned)
        self.assertTrue(style.as_generated().reverse)

    def test_a_hand_edited_stop_is_hand_tuned(self):
        style = self.style(stops=[{"value": 0.0, "color": "#000000"}, {"value": 42.0, "color": "#ffffff"}])

        self.assertTrue(style.is_hand_tuned)
        self.assertIsNone(style.as_generated())

    def test_a_hand_edited_colour_is_hand_tuned(self):
        style = self.style(stops=[{"value": 0.0, "color": "#000000"}, {"value": 50.0, "color": "#fffffe"}])

        self.assertTrue(style.is_hand_tuned)

    def test_a_range_change_makes_a_ramp_style_read_as_tuned(self):
        style = self.style()
        self.variable.value_max = 60
        self.variable.save(update_fields=["value_max"])
        style.refresh_from_db()

        self.assertTrue(style.is_hand_tuned)

    def test_a_style_with_no_ramp_is_hand_tuned(self):
        style = self.style(ramp=None, stops=[{"value": 0.0, "color": "#000000"}, {"value": 50.0, "color": "#ffffff"}])

        self.assertTrue(style.is_hand_tuned)

    def test_a_template_style_is_always_hand_tuned(self):
        # Even with stops a ramp would have produced, a template's levels are
        # deliberate classes, never a ramp's to regenerate.
        template = StyleTemplate.objects.filter(organisation__isnull=True).first()
        style = self.style(template=template)

        self.assertTrue(style.is_hand_tuned)


class ReversedGenerationTests(StylingSurfaceTestCase):
    def test_reverse_flips_the_colours_against_fixed_values(self):
        response = self.client.get(
            self.stops_url, {"ramp": str(self.org_ramp.pk), "mode": "continuous", "reverse": "1"}
        )

        self.assertEqual(
            response.json()["stops"],
            [{"value": 0.0, "color": "#ffffff"}, {"value": 50.0, "color": "#000000"}],
        )

    def test_a_reversed_stepped_ramp_keeps_its_class_pairs(self):
        forward = served_ramp_stops(self.org_ramp, self.variable, mode="stepped", steps=3)
        response = self.client.get(
            self.stops_url, {"ramp": str(self.org_ramp.pk), "mode": "stepped", "steps": "3", "reverse": "1"}
        )

        stops = response.json()["stops"]
        self.assertEqual([s["value"] for s in stops], [s["value"] for s in forward])
        self.assertEqual([s["color"] for s in stops], [s["color"] for s in reversed(forward)])


BASIC_CONTROLS = ('name="name"', 'name="ramp"', 'name="reverse"', 'name="mode"', 'name="steps"', 'id="vs-preview"')
ADVANCED_CONTROLS = (
    'id="vs-stops-table"',
    'id="vs-classes"',
    'name="under"',
    'name="over"',
    'id="vs-add-stop"',
    'id="vs-json-toggle"',
)


class EditorViewsTests(StylingSurfaceTestCase):
    def advanced(self, response):
        html = response.content.decode()
        match = re.search(r'<details[^>]*id="vs-advanced"[^>]*>', html)
        self.assertIsNotNone(match, "no advanced disclosure on the page")
        return match.group(0), html

    def make_style(self, **fields):
        style = VariableStyle(
            variable=self.variable, name="Official", slug="official", is_default=True, ramp=self.org_ramp, **fields
        )
        if "stops" not in fields:
            style.apply_ramp()
        style.save()
        return style

    def test_basic_controls_sit_before_the_disclosure_and_the_stops_inside_it(self):
        self.make_style()
        tag, html = self.advanced(self.client.get(self.form_url))
        start = html.index(tag)
        end = html.index("</details>", start)
        advanced = html[start:end]

        before = html[:start]
        for control in BASIC_CONTROLS:
            self.assertIn(control, before)
        self.assertIn("Advanced: fine-tune stops", advanced)
        for control in ADVANCED_CONTROLS:
            self.assertIn(control, advanced)

    def test_the_disclosure_is_closed_for_stops_as_generated(self):
        self.make_style()
        tag, _ = self.advanced(self.client.get(self.form_url))

        self.assertNotIn(" open", tag)

    def test_the_disclosure_opens_for_hand_tuned_stops(self):
        self.make_style(stops=[{"value": 0.0, "color": "#000000"}, {"value": 42.0, "color": "#ffffff"}])
        tag, _ = self.advanced(self.client.get(self.form_url))

        self.assertIn(" open", tag)

    def test_the_disclosure_opens_for_a_template_style(self):
        self.make_style(template=StyleTemplate.objects.filter(organisation__isnull=True).first())
        tag, _ = self.advanced(self.client.get(self.form_url))

        self.assertIn(" open", tag)

    def test_reverse_is_ticked_for_a_reversed_ramp(self):
        self.make_style(stops=[{"value": 0.0, "color": "#ffffff"}, {"value": 50.0, "color": "#000000"}])
        html = self.client.get(self.form_url).content.decode()

        self.assertRegex(html, r'<input[^>]*name="reverse"[^>]*checked')

    def test_the_page_tells_its_script_whether_the_stops_are_tuned(self):
        self.make_style(stops=[{"value": 0.0, "color": "#000000"}, {"value": 42.0, "color": "#ffffff"}])
        response = self.client.get(self.form_url)

        self.assertContains(response, 'data-stops-tuned="true"')

    def test_no_apply_ramp_button_remains(self):
        self.make_style()
        response = self.client.get(self.form_url)

        self.assertNotContains(response, "Apply ramp")
        self.assertNotContains(response, "vs-apply-ramp")
        self.assertContains(response, "Regenerate from ramp (replaces your stops)")
