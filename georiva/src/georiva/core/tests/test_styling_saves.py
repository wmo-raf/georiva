"""Saving on the Variable Styling page (#802): one sticky "Save style" bar, and a guard on unsaved work.

Read through the rendered page:

* "Save style" lives in a bar at the foot of the style form, pinned in view,
  with the style's "Unsaved" note beside it — and nowhere else in the form;
* the range keeps its own "Save range for all styles", beside its own note;
* both forms name the note that says they are dirty and the question to ask
  before that work is lost, and the style tabs ask to be guarded — that markup
  is all the guard script reads.

What the guard does with it (the prompts on leaving, switching tabs and
submitting the other form) is JavaScript, verified by hand like the map.
"""

import re

from georiva.core.models import VariableStyle
from georiva.core.tests.test_styling_surface import StylingSurfaceTestCase


def opening_tag(html, element_id):
    match = re.search(rf'<\w+[^>]*\bid="{element_id}"[^>]*>', html)
    return match.group(0) if match else None


class StyledVariableTestCase(StylingSurfaceTestCase):
    def setUp(self):
        super().setUp()
        VariableStyle.objects.create(
            variable=self.variable,
            name="Official",
            slug="official",
            is_default=True,
            stops=[{"value": 0, "color": "#123456"}, {"value": 50, "color": "#654321"}],
        )

    def page(self, **params):
        return self.client.get(self.form_url, params).content.decode()

    def style_form(self, html):
        match = re.search(r'<form[^>]*id="vs-style-form".*?</form>', html, re.S)
        self.assertIsNotNone(match, "no style form on the page")
        return match.group(0)

    def save_bar(self, html):
        match = re.search(r'<div[^>]*id="vs-save-bar".*?</div>', html, re.S)
        self.assertIsNotNone(match, "no save bar on the page")
        return match.group(0)


class SaveBarTests(StyledVariableTestCase):
    def test_save_style_is_only_in_the_bar_at_the_foot_of_the_style_form(self):
        form = self.style_form(self.page())
        bar = self.save_bar(form)

        self.assertEqual(form.count('value="save-style"'), 1)
        self.assertIn('value="save-style"', bar)
        self.assertIn("Save style", bar)
        self.assertNotRegex(form[form.index(bar) + len(bar) :], r"<(input|button|select|textarea)\b")

    def test_the_bar_is_there_while_creating_a_style(self):
        self.save_bar(self.style_form(self.page(style="__new__")))

    def test_the_unsaved_note_sits_in_the_bar(self):
        bar = self.save_bar(self.page())

        self.assertIn('id="vs-unsaved"', bar)
        self.assertRegex(opening_tag(bar, "vs-unsaved"), r"\bhidden\b")

    def test_the_range_keeps_its_own_save_and_note(self):
        html = self.page()
        range_form = re.search(r'<details[^>]*id="vs-range".*?</details>', html, re.S).group(0)

        self.assertIn("Save range for all styles", range_form)
        self.assertIn('id="vs-range-unsaved"', range_form)
        self.assertNotIn('value="save-style"', range_form)


class UnsavedGuardMarkupTests(StyledVariableTestCase):
    def form_tag(self, html, marker):
        for tag in re.findall(r"<form\b[^>]*>", html):
            if marker in tag:
                return tag
        self.fail(f"no form carrying {marker}")

    def test_both_forms_name_their_unsaved_note_and_question(self):
        html = self.page()
        style = opening_tag(html, "vs-style-form")
        range_form = self.form_tag(html, 'data-unsaved-note="vs-range-unsaved"')

        self.assertIn('data-unsaved-note="vs-unsaved"', style)
        self.assertRegex(style, r'data-unsaved-message="[^"]*style[^"]*"')
        self.assertRegex(range_form, r'data-unsaved-message="[^"]*range[^"]*"')

    def test_the_style_tabs_are_guarded(self):
        html = self.page()
        nav = re.search(r'<nav class="vs-style-tabs"[^>]*>', html).group(0)

        self.assertIn("data-unsaved-guard", nav)

    def test_the_guard_script_is_loaded_before_the_page_script(self):
        html = self.page()

        self.assertIn("core/js/unsaved_guard.js", html)
        self.assertLess(html.index("core/js/unsaved_guard.js"), html.index("getElementById('vs-style-form')"))
