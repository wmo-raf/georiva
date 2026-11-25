"""Switching a style's path (#807): the in-place template picker and "Start over".

One seam: the per-variable Styling page, driven through the admin test client —
what the page offers for a selected style, and what its POSTs do to that same
style. A switch never makes a new style: name, slug and default status stay.

The live preview of a picked template, and the unsaved-changes prompts, are
JavaScript, verified by hand like the map; the markup they hang on is asserted.
"""

import re

from georiva.core.models import OutOfRange, StyleTemplate, VariableStyle
from georiva.core.tests.test_style_origins import OriginTestCase


class SwitchTestCase(OriginTestCase):
    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        # Laid out for 0–100, wider than the variable's 0–50 range.
        cls.other_template = StyleTemplate.objects.create(
            organisation=cls.org,
            name="Wide bands",
            slug="wide-bands",
            levels=[0, 40, 100],
            colors=["#00ff00", "#ffff00"],
            unit=cls.variable.source_unit,
            under=OutOfRange.TRANSPARENT,
            over=OutOfRange.TRANSPARENT,
        )

    def encoding(self):
        self.variable.refresh_from_db()
        return self.variable.value_min, self.variable.value_max

    def second_style(self):
        """A style beside the default, which is not itself the default."""
        return VariableStyle.objects.create(
            variable=self.variable, name="Second", slug="second", origin="custom", stops=[{"value": 0, "color": "#000"}]
        )


class StartOverTestCase(SwitchTestCase):
    def on_every_origin(self, check):
        """``check`` a fresh style of each origin, each the variable's only (and default) style."""
        for make in (self.template_style, self.ramp_style, self.custom_style):
            self.variable.styles.all().delete()
            style = make()
            with self.subTest(origin=style.origin):
                check(style)

    def chooser(self, response):
        match = re.search(r'<section[^>]*id="vs-chooser".*?</section>', response.content.decode(), re.S)
        return match.group(0) if match else None

    def start_over(self, style, action, **fields):
        return self.client.post(
            self.form_url, {"action": action, "style_slug": style.slug, "start_over": "over", **fields}
        )

    def assertSameStyle(self, before, after):
        self.assertEqual(self.variable.styles.count(), 1)
        self.assertEqual(
            (after.pk, after.slug, after.name, after.is_default), (before.pk, before.slug, before.name, True)
        )


class StartOverEntryTests(StartOverTestCase):
    def test_every_path_offers_start_over_in_its_header_behind_the_unsaved_guard(self):
        def check(style):
            header = re.search(r'<header class="vs-style-header".*?</header>', self.editor(self.page(style)), re.S)[0]
            link = re.search(r"<[^>]*data-unsaved-guard[^>]*>\s*<a [^>]*>\s*Start over", header)
            self.assertIsNotNone(link, header)
            self.assertIn(f"?style={style.slug}&amp;start=over", link.group(0))

        self.on_every_origin(check)

    def test_start_over_reopens_the_chooser_for_this_style(self):
        style = self.ramp_style()

        response = self.page(style, start="over")

        chooser = self.chooser(response)
        self.assertIsNotNone(chooser)
        self.assertEqual(response.context["selected"], style)
        for start in ('value="apply-template"', 'value="start-from-ramp"', 'name="start" value="scratch"'):
            self.assertIn(start, chooser)
        # Template and ramp remake this style; scratch opens this style's editor.
        self.assertEqual(chooser.count(f'name="style_slug" value="{style.slug}"'), 2)
        self.assertEqual(chooser.count('name="start_over" value="over"'), 2)
        self.assertIn(f'name="style" value="{style.slug}"', chooser)

    def test_each_start_asks_first(self):
        chooser = self.chooser(self.page(self.ramp_style(), start="over"))

        self.assertEqual(len(re.findall(r"<form[^>]*data-confirm=", chooser)), 3)

    def test_the_default_style_is_offered_the_range_checkbox(self):
        self.assertIn('name="accept_encoding"', self.chooser(self.page(self.ramp_style(), start="over")))

    def test_a_style_that_is_not_the_default_is_not(self):
        self.custom_style()

        chooser = self.chooser(self.page(self.second_style(), start="over"))

        self.assertNotIn("accept_encoding", chooser)


class StartOverApplyTests(StartOverTestCase):
    def test_a_template_remakes_the_same_style_on_every_path(self):
        def check(style):
            self.start_over(style, "apply-template", template=self.other_template.pk)

            after = self.variable.styles.get()
            self.assertSameStyle(style, after)
            self.assertEqual((after.origin, after.template, after.ramp), ("template", self.other_template, None))
            self.assertEqual(after.stops[-1]["value"], 100.0)

        self.on_every_origin(check)

    def test_a_ramp_remakes_the_same_style_on_every_path(self):
        def check(style):
            self.start_over(style, "start-from-ramp", ramp=self.org_ramp.pk, mode="stepped", steps="3")

            after = self.variable.styles.get()
            self.assertSameStyle(style, after)
            self.assertEqual((after.origin, after.ramp, after.template), ("ramp", self.org_ramp, None))
            self.assertEqual((after.mode, after.steps, len(after.stops)), ("stepped", 3, 6))

        self.on_every_origin(check)

    def test_scratch_remakes_the_same_style_on_every_path(self):
        def check(style):
            self.client.post(
                self.form_url,
                {
                    "action": "save-style",
                    "style_slug": style.slug,
                    "start_over": "scratch",
                    "name": style.name,
                    "stop_value": ["0", "50"],
                    "stop_color": ["#123456", "#654321"],
                },
            )

            after = self.variable.styles.get()
            self.assertSameStyle(style, after)
            self.assertEqual((after.origin, after.ramp, after.template), ("custom", None, None))
            self.assertEqual([stop["color"] for stop in after.stops], ["#123456", "#654321"])

        self.on_every_origin(check)

    def test_saving_a_scratch_start_over_asks_first(self):
        editor = self.editor(self.page(self.template_style(), start="scratch"))

        self.assertRegex(editor, r'<form[^>]*id="vs-style-form"[^>]*data-confirm=')

    def test_saving_a_new_scratch_style_does_not_ask(self):
        editor = self.editor(self.client.get(self.form_url, {"style": "__new__", "start": "scratch"}))

        self.assertNotRegex(editor, r'<form[^>]*id="vs-style-form"[^>]*data-confirm=')

    def test_scratch_opens_this_styles_editor_empty_and_custom(self):
        style = self.template_style()

        editor = self.editor(self.page(style, start="scratch"))

        self.assertRegex(editor, r'<form[^>]*id="vs-style-form"[^>]*data-origin="custom"')
        self.assertIn('name="start_over" value="scratch"', editor)
        self.assertIn(f'name="style_slug"\n                           value="{style.slug}"', editor)
        self.assertNotIn('name="stop_value" value="0', editor)
        # Its template link goes on save, so it is not shown as where it came from.
        self.assertNotIn("Customised from", editor)

    def test_the_default_takes_the_templates_range_when_ticked(self):
        self.start_over(self.ramp_style(), "apply-template", template=self.other_template.pk, accept_encoding="on")

        self.assertEqual(self.encoding(), (0, 100))

    def test_a_style_that_is_not_the_default_never_takes_a_range(self):
        self.custom_style()
        second = self.second_style()

        self.start_over(second, "apply-template", template=self.other_template.pk, accept_encoding="on")

        second.refresh_from_db()
        self.assertEqual((second.template, second.is_default), (self.other_template, False))
        self.assertEqual(self.encoding(), (0, 50))
        self.assertEqual(self.variable.styles.count(), 2)

    def test_a_refused_start_goes_back_to_this_styles_chooser(self):
        style = self.ramp_style()

        response = self.start_over(style, "apply-template", template="")

        self.assertRedirects(response, f"{self.form_url}?style={style.slug}&start=over", fetch_redirect_response=False)


class InPlacePickerTests(SwitchTestCase):
    def picker(self, style):
        editor = self.editor(self.page(style))
        match = re.search(r'<fieldset[^>]*id="vs-template-source".*?class="vs-customise"', editor, re.S)
        self.assertIsNotNone(match, "no template picker in the editor")
        return match.group(0)

    def test_a_template_style_picks_from_the_catalogue_with_its_own_template_chosen(self):
        picker = self.picker(self.template_style())

        self.assertRegex(picker, rf'<input type="radio" name="template" value="{self.template.pk}"[^>]* checked')
        self.assertRegex(picker, rf'<input type="radio" name="template" value="{self.other_template.pk}"')
        self.assertNotRegex(picker, rf'value="{self.other_template.pk}"[^>]* checked')

    def test_each_template_carries_its_stops_for_the_live_preview(self):
        picker = self.picker(self.template_style())

        radio = re.search(rf'<input type="radio" name="template" value="{self.other_template.pk}"[^>]*>', picker)[0]
        self.assertIn("data-stops=", radio)
        self.assertIn('data-under="transparent"', radio)
        self.assertIn("#ffff00", radio)

    def test_the_default_style_is_offered_the_range_checkbox(self):
        picker = self.picker(self.template_style())

        self.assertIn('name="accept_encoding"', picker)
        self.assertIn("Also use its range and curve", picker)

    def test_a_style_that_is_not_the_default_is_not_offered_the_range_checkbox(self):
        self.custom_style()

        picker = self.picker(self.template_style())

        self.assertNotIn("accept_encoding", picker)
        self.assertNotIn("data-when", picker)

    def test_the_default_style_is_shown_the_new_templates_warnings(self):
        # 40 and 100 fall outside 0–50, so the kept range clips them.
        picker = self.picker(self.template_style())

        self.assertIn("will be clipped: 100", picker)

    def test_the_picker_is_inside_the_guarded_style_form(self):
        editor = self.editor(self.page(self.template_style()))

        form = re.search(r'<form[^>]*id="vs-style-form".*?</form>', editor, re.S)[0]
        self.assertIn('name="template"', form)


class InPlaceTemplateSwitchTests(SwitchTestCase):
    def test_saving_another_template_replaces_the_stops_and_link_and_keeps_the_style(self):
        style = self.template_style()
        style.name = "Official"
        style.save()

        self.save(style, template=self.other_template.pk, under="transparent", over="transparent")

        style.refresh_from_db()
        self.assertEqual(
            [(stop["value"], stop["color"]) for stop in style.stops],
            [(0.0, "#00ff00"), (40.0, "#00ff00"), (40.0, "#ffff00"), (100.0, "#ffff00")],
        )
        self.assertEqual(style.template, self.other_template)
        self.assertEqual((style.name, style.slug, style.is_default), ("Official", "house-bands", True))
        self.assertEqual(self.variable.styles.count(), 1)

    def test_the_default_takes_the_new_templates_range_when_ticked(self):
        style = self.template_style()

        self.save(style, template=self.other_template.pk, accept_encoding="on")

        self.assertEqual(self.encoding(), (0, 100))

    def test_the_default_keeps_its_range_when_unticked(self):
        style = self.template_style()

        self.save(style, template=self.other_template.pk)

        self.assertEqual(self.encoding(), (0, 50))

    def test_a_style_that_is_not_the_default_never_takes_a_range(self):
        self.custom_style()
        style = self.template_style()
        self.assertFalse(style.is_default)

        self.save(style, template=self.other_template.pk, accept_encoding="on")

        style.refresh_from_db()
        self.assertEqual(style.template, self.other_template)
        self.assertEqual(self.encoding(), (0, 50))

    def test_keeping_the_same_template_never_takes_a_range(self):
        style = self.template_style()

        self.save(style, template=self.template.pk, accept_encoding="on")

        self.assertEqual(self.encoding(), (0, 50))


class ChangeTemplateDisclosureTests(SwitchTestCase):
    """A template style shows only its template until "Change template" is opened (#810)."""

    def disclosure(self, response):
        editor = self.editor(response)
        match = re.search(r'<details[^>]*id="vs-template-change".*?</details>\s*</fieldset>', editor, re.S)
        self.assertIsNotNone(match, "no Change template disclosure in the editor")
        return match.group(0)

    def summary(self, disclosure):
        return re.search(r"<summary.*?</summary>", disclosure, re.S).group(0)

    def is_open(self, disclosure):
        return re.match(r"<details[^>]*\sopen[\s>]", disclosure) is not None

    def rejected_save(self, style, template):
        # No name: the form refuses it and the editor comes back.
        response = self.client.post(
            self.form_url, {"action": "save-style", "style_slug": style.slug, "name": "", "template": template.pk}
        )
        self.assertEqual(response.status_code, 200)
        return response

    def test_the_current_template_shows_on_one_closed_line(self):
        disclosure = self.disclosure(self.page(self.template_style()))

        self.assertFalse(self.is_open(disclosure))
        summary = self.summary(disclosure)
        self.assertIn("House bands", summary)
        self.assertIn("vs-template-swatch", summary)
        self.assertIn("0, 10, 20", summary)
        self.assertIn("Change template", summary)
        self.assertNotIn('name="template"', summary)

    def test_the_catalogue_and_range_checkbox_are_behind_the_toggle(self):
        disclosure = self.disclosure(self.page(self.template_style()))

        self.assertRegex(disclosure, rf'name="template" value="{self.other_template.pk}"')
        self.assertIn('name="accept_encoding"', disclosure)

    def test_it_opens_on_a_rejected_save_with_another_template_picked(self):
        response = self.rejected_save(self.template_style(), self.other_template)

        disclosure = self.disclosure(response)
        self.assertTrue(self.is_open(disclosure))
        self.assertRegex(disclosure, rf'value="{self.other_template.pk}"[^>]* checked')
        self.assertNotRegex(disclosure, rf'value="{self.template.pk}"[^>]* checked')

    def test_it_stays_closed_on_a_rejected_save_that_kept_the_template(self):
        response = self.rejected_save(self.template_style(), self.template)

        self.assertFalse(self.is_open(self.disclosure(response)))
