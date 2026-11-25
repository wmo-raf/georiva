"""The Variable Styling page's layout (#799): which style is being edited, and what the range applies to.

Read through the rendered page, as an operator meets it:

* the styles are tabs above the editor, the selected one marked
  ``aria-current``, the default badged, "+ New style" last;
* "Make default" and "Delete" act from the selected style's header;
* the map preview is alone in the right column;
* the range and curve sit below the editor, labelled as shared by every style,
  collapsed unless the selected style's stops call for attention.

The warnings that follow unsaved edits are JavaScript, verified by hand like
the map; what is tested here is the state the page opens in.
"""

import re

from django.template.loader import render_to_string

from georiva.core.models import VariableStyle
from georiva.core.tests.test_styling_surface import StylingSurfaceTestCase
from georiva.core.views.styling import styling_summary


def stops(*values):
    return [{"value": value, "color": "#123456"} for value in values]


class StyleTabsTests(StylingSurfaceTestCase):
    def setUp(self):
        super().setUp()
        self.official = VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official", is_default=True, stops=stops(0, 50)
        )
        self.analyst = VariableStyle.objects.create(
            variable=self.variable, name="Analyst", slug="analyst", stops=stops(0, 50)
        )

    def tabs(self, response):
        html = response.content.decode()
        match = re.search(r'<nav class="vs-style-tabs".*?</nav>', html, re.S)
        self.assertIsNotNone(match, "no style tabs on the page")
        return match.group(0)

    def test_every_style_is_a_tab_and_new_style_comes_last(self):
        tabs = self.tabs(self.client.get(self.form_url))

        official = tabs.index(f'href="{self.form_url}?style=official"')
        analyst = tabs.index(f'href="{self.form_url}?style=analyst"')
        new = tabs.index(f'href="{self.form_url}?style=__new__"')
        self.assertLess(max(official, analyst), new)
        self.assertIn("New style", tabs[new:])

    def test_the_selected_tab_is_marked_current(self):
        tabs = self.tabs(self.client.get(self.form_url, {"style": "analyst"}))

        self.assertEqual(tabs.count('aria-current="page"'), 1)
        self.assertRegex(tabs, rf'href="{re.escape(self.form_url)}\?style=analyst"[^>]*aria-current="page"')

    def test_new_style_is_the_current_tab_while_creating(self):
        tabs = self.tabs(self.client.get(self.form_url, {"style": "__new__"}))

        self.assertRegex(tabs, r'style=__new__"[^>]*aria-current="page"')

    def test_each_tab_carries_its_swatch_and_the_default_its_badge(self):
        tabs = self.tabs(self.client.get(self.form_url))

        self.assertEqual(tabs.count("vs-style-tab-swatch"), 2)
        self.assertIn("#123456", tabs)
        self.assertEqual(tabs.count("Default"), 1)
        self.assertLess(tabs.index("Official"), tabs.index("Default"))
        self.assertLess(tabs.index("Default"), tabs.index("Analyst"))

    def test_each_tab_carries_its_full_name_as_its_title(self):
        # A long name is cut short on one line of tabs (#804); the title keeps it whole.
        self.analyst.name = "Analyst view of two-metre temperature anomalies"
        self.analyst.save()

        tabs = self.tabs(self.client.get(self.form_url))

        self.assertRegex(
            tabs,
            rf'href="{re.escape(self.form_url)}\?style=analyst"[^>]*'
            r'title="Analyst view of two-metre temperature anomalies"',
        )
        self.assertIn('title="Official"', tabs)


class SelectedStyleHeaderTests(StylingSurfaceTestCase):
    def setUp(self):
        super().setUp()
        for slug, is_default in (("official", True), ("analyst", False), ("draft", False)):
            VariableStyle.objects.create(
                variable=self.variable, name=slug.title(), slug=slug, is_default=is_default, stops=stops(0, 50)
            )

    def action_forms(self, response, action):
        html = response.content.decode()
        return re.findall(rf'<form[^>]*>(?:(?!</form>).)*value="{action}"(?:(?!</form>).)*</form>', html, re.S)

    def test_make_default_acts_on_the_selected_style_only(self):
        response = self.client.get(self.form_url, {"style": "analyst"})

        forms = self.action_forms(response, "promote")
        self.assertEqual(len(forms), 1)
        self.assertIn('name="style_slug" value="analyst"', forms[0])
        self.assertIn("Make default", forms[0])

    def test_the_default_style_offers_no_make_default(self):
        response = self.client.get(self.form_url, {"style": "official"})

        self.assertEqual(self.action_forms(response, "promote"), [])

    def test_delete_acts_on_the_selected_style_only(self):
        response = self.client.get(self.form_url, {"style": "draft"})

        forms = self.action_forms(response, "delete-style")
        self.assertEqual(len(forms), 1)
        self.assertIn('name="style_slug" value="draft"', forms[0])

    def test_delete_keeps_its_own_confirm_and_the_unsaved_guard_lets_it_through(self):
        response = self.client.get(self.form_url, {"style": "draft"})

        form = self.action_forms(response, "delete-style")[0]
        tag = re.match(r"<form[^>]*>", form).group(0)
        self.assertIn("data-confirm=", tag)
        self.assertIn('data-unsaved-discards="vs-style-form"', tag)

    def test_a_new_style_has_nothing_to_promote_or_delete(self):
        response = self.client.get(self.form_url, {"style": "__new__"})

        self.assertEqual(self.action_forms(response, "promote"), [])
        self.assertEqual(self.action_forms(response, "delete-style"), [])


class MapColumnTests(StylingSurfaceTestCase):
    def test_the_map_preview_is_alone_in_the_right_column(self):
        VariableStyle.objects.create(variable=self.variable, name="Official", slug="official", is_default=True)
        html = self.client.get(self.form_url).content.decode()

        aside = re.search(r"<div class=\"vs-layout\">.*?(<aside>.*?</aside>)", html, re.S).group(1)
        self.assertEqual(aside.count("<section"), 1)
        self.assertIn("vs-map-panel", aside)


class SharedRangeSectionTests(StylingSurfaceTestCase):
    def style(self, *values):
        return VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official", is_default=True, stops=stops(*values)
        )

    def range_section(self, response):
        html = response.content.decode()
        match = re.search(r'<details[^>]*id="vs-range".*?</details>', html, re.S)
        self.assertIsNotNone(match, "no range section on the page")
        return match.group(0)

    def is_open(self, section):
        return re.match(r"<details[^>]*\bopen\b", section) is not None

    def test_it_sits_below_the_style_editor(self):
        self.style(0, 50)
        html = self.client.get(self.form_url).content.decode()

        self.assertLess(html.index('id="vs-style-form"'), html.index('id="vs-range"'))

    def test_it_says_it_is_shared_by_every_style_and_saves_for_all(self):
        self.style(0, 50)
        section = self.range_section(self.client.get(self.form_url))

        self.assertIn("shared by every style", section)
        self.assertIn("Save range for all styles", section)
        self.assertIn('value="save-range"', section)

    def test_it_carries_the_fit_range_hint(self):
        self.style(0, 50)
        section = self.range_section(self.client.get(self.form_url))

        self.assertIn('id="vs-range-hint"', section)
        self.assertIn("Fit range to stops", section)

    def test_it_is_collapsed_when_the_stops_fill_the_range(self):
        self.style(0, 50)

        self.assertFalse(self.is_open(self.range_section(self.client.get(self.form_url))))

    def test_it_is_collapsed_for_a_variable_with_no_styles(self):
        self.assertFalse(self.is_open(self.range_section(self.client.get(self.form_url))))

    def test_it_opens_when_stops_fall_outside_the_range(self):
        self.style(-10, 50)

        self.assertTrue(self.is_open(self.range_section(self.client.get(self.form_url))))

    def test_it_opens_when_the_range_is_much_wider_than_the_stops(self):
        self.style(10, 20)

        self.assertTrue(self.is_open(self.range_section(self.client.get(self.form_url))))

    def test_it_opens_on_a_rejected_range(self):
        self.style(0, 50)
        response = self.client.post(
            self.form_url,
            {"action": "save-range", "style_slug": "official", "value_min": 60, "value_max": 10},
        )

        self.assertTrue(self.is_open(self.range_section(response)))


class SharedEncodingWordingTests(StylingSurfaceTestCase):
    """The overview and the Variable edit panel say what the Styling page does
    (#798): the range and curve are shared by every style, and a variable with
    none is pointed at the guided start."""

    def summary_panel(self):
        return render_to_string(
            "core/panels/styling_summary.html", {"variable": self.variable, **styling_summary(self.variable)}
        )

    def test_the_overview_says_the_range_and_curve_are_shared_and_points_to_set_up(self):
        response = self.client.get(self.page_url)

        self.assertContains(response, "shared by every style")
        self.assertRegex(response.content.decode(), rf'<a href="{re.escape(self.form_url)}"[^>]*>\s*Set up styling')

    def test_the_overview_offers_no_set_up_for_a_styled_variable(self):
        VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official", is_default=True, stops=stops(0, 50)
        )

        self.assertNotContains(self.client.get(self.page_url), "Set up styling")

    def test_the_panel_says_the_range_and_curve_are_shared_and_points_to_set_up(self):
        html = self.summary_panel()

        self.assertIn("shared by every style", html)
        self.assertIn("Set up styling", html)

    def test_the_panel_edits_styling_once_a_style_exists(self):
        VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official", is_default=True, stops=stops(0, 50)
        )
        html = self.summary_panel()

        self.assertIn("shared by every style", html)
        self.assertIn("Edit styling", html)
        self.assertNotIn("Set up styling", html)
