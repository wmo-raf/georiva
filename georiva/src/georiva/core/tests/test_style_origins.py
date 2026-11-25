"""Style origins (#806): each style records how it was made, and the editor shows only that path's controls.

Three seams:

* making a style — applying a template, starting from a ramp and starting from
  scratch each record their origin;
* the rendered editor — what each origin shows and what it leaves out, read
  through the page as an operator meets it;
* the page's POSTs — "Save style" per origin (a ramp style's stops always come
  from its ramp, a template style's never change, a custom style's are its
  rows), and "Customise stops".

Live regeneration on a ramp style is JavaScript, verified by hand like the map.
"""

import re

from georiva.core import style_application
from georiva.core.models import OutOfRange, StyleTemplate, VariableStyle
from georiva.core.tests.test_styling_surface import StylingSurfaceTestCase

# make_org_tree's variable spans 0–50; the org ramp runs black → white.
FORWARD = [{"value": 0.0, "color": "#000000"}, {"value": 50.0, "color": "#ffffff"}]
REVERSED = [{"value": 0.0, "color": "#ffffff"}, {"value": 50.0, "color": "#000000"}]
TUNED = [{"value": 0.0, "color": "#123456"}, {"value": 42.0, "color": "#654321"}]


class OriginTestCase(StylingSurfaceTestCase):
    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        cls.template = StyleTemplate.objects.create(
            organisation=cls.org,
            name="House bands",
            slug="house-bands",
            levels=[0, 10, 20],
            colors=["#0000ff", "#ff0000"],
            unit=cls.variable.source_unit,
        )

    def ramp_style(self, **fields):
        return style_application.start_from_ramp(self.org_ramp, self.variable, **fields)

    def template_style(self):
        return style_application.apply_template(self.template, self.variable)

    def custom_style(self, **fields):
        fields.setdefault("stops", TUNED)
        return VariableStyle.objects.create(
            variable=self.variable, name="Mine", slug="mine", is_default=True, origin="custom", **fields
        )

    def page(self, style=None, **params):
        if style is not None:
            params["style"] = style.slug
        return self.client.get(self.form_url, params)

    def editor(self, response):
        html = response.content.decode()
        match = re.search(r'<section[^>]*id="vs-style-editor".*?</section>', html, re.S)
        self.assertIsNotNone(match, "no style editor on the page")
        return match.group(0)

    def save(self, style, **fields):
        return self.client.post(
            self.form_url, {"action": "save-style", "style_slug": style.slug, "name": style.name, **fields}
        )


class OriginOnCreateTests(OriginTestCase):
    def test_applying_a_template_makes_a_template_style(self):
        self.assertEqual(self.template_style().origin, VariableStyle.Origin.TEMPLATE)

    def test_starting_from_a_ramp_makes_a_ramp_style(self):
        self.assertEqual(self.ramp_style().origin, VariableStyle.Origin.RAMP)

    def test_a_new_style_saved_with_a_ramp_posted_is_still_custom(self):
        self.client.post(
            self.form_url,
            {
                "action": "save-style",
                "style_slug": "",
                "name": "Pasted",
                "ramp": self.org_ramp.pk,
                "mode": "continuous",
                "stop_value": ["0", "42"],
                "stop_color": ["#123456", "#654321"],
            },
        )

        style = self.variable.styles.get()
        self.assertEqual((style.origin, style.stops), (VariableStyle.Origin.CUSTOM, TUNED))

    def test_starting_from_scratch_makes_a_custom_style(self):
        self.client.post(
            self.form_url,
            {
                "action": "save-style",
                "style_slug": "",
                "name": "Pasted",
                "stop_value": ["0", "50"],
                "stop_color": ["#000000", "#ffffff"],
            },
        )

        self.assertEqual(self.variable.styles.get().origin, VariableStyle.Origin.CUSTOM)


RAMP_CONTROLS = ('name="ramp"', 'name="mode"', 'name="steps"', 'name="reverse"')
STOPS_CONTROLS = ('id="vs-stops-table"', 'id="vs-classes"', 'id="vs-add-stop"', 'id="vs-json-toggle"')
GONE = (
    'id="vs-advanced"',
    'id="vs-restyle"',
    'id="vs-regenerate"',
    "data-stops-tuned",
    "Regenerate from ramp",
    "Restyle from a color ramp",
    "Advanced: fine-tune stops",
)
OUTSIDE_THE_STOPS = ('name="under"', 'name="over"')


class RampEditorTests(OriginTestCase):
    def test_a_ramp_style_shows_its_ramp_controls_and_no_stops_table(self):
        editor = self.editor(self.page(self.ramp_style()))

        for control in ('name="name"', *RAMP_CONTROLS, *OUTSIDE_THE_STOPS, 'id="vs-preview"'):
            self.assertIn(control, editor)
        for control in (*STOPS_CONTROLS, *GONE, "vs-template-source"):
            self.assertNotIn(control, editor)

    def test_a_ramp_style_offers_customise_stops(self):
        self.assertIn("Customise stops", self.editor(self.page(self.ramp_style())))

    def test_reverse_opens_ticked_for_a_reversed_ramp_style_after_a_range_change(self):
        style = self.ramp_style()
        style.reverse = True
        style.apply_ramp()
        style.save()
        self.variable.value_max = 60
        self.variable.save(update_fields=["value_max"])

        self.assertRegex(self.editor(self.page(style)), r'<input[^>]*name="reverse"[^>]*checked')

    def stop_values(self, editor):
        return [float(v) for v in re.findall(r'name="stop_value" value="([^"]+)"', editor)]

    def unsaved_note(self, editor):
        return re.search(r'<p[^>]*id="vs-unsaved"[^>]*>', editor).group(0)

    def test_a_ramp_style_laid_over_an_old_range_shows_the_stops_a_save_will_store(self):
        style = self.ramp_style()
        self.client.post(
            self.form_url,
            {
                "action": "save-range",
                "style_slug": style.slug,
                "value_min": "0",
                "value_max": "60",
                "scale_type": "linear",
            },
        )
        style.refresh_from_db()
        self.assertEqual(style.stops, FORWARD, "saving the range rewrote the stops")

        editor = self.editor(self.page(style))

        self.assertEqual(self.stop_values(editor), [0.0, 60.0])
        self.assertNotIn("hidden", self.unsaved_note(editor))
        self.assertIn("Laid out over the old range", editor)

    def test_a_ramp_style_laid_over_the_current_range_is_saved_as_shown(self):
        editor = self.editor(self.page(self.ramp_style()))

        self.assertEqual(self.stop_values(editor), [0.0, 50.0])
        self.assertIn("hidden", self.unsaved_note(editor))
        self.assertNotIn("Laid out over the old range", editor)

    def test_the_editor_tells_its_script_the_origin(self):
        editor = self.editor(self.page(self.ramp_style()))

        self.assertRegex(editor, r'<form[^>]*id="vs-style-form"[^>]*data-origin="ramp"')


class TemplateEditorTests(OriginTestCase):
    def test_a_template_style_shows_where_it_came_from_and_no_ramp_or_stops(self):
        editor = self.editor(self.page(self.template_style()))

        for control in ('name="name"', *OUTSIDE_THE_STOPS, 'id="vs-preview"', 'id="vs-template-source"'):
            self.assertIn(control, editor)
        self.assertIn("House bands", re.search(r'id="vs-template-source".*?</div>', editor, re.S).group(0))
        for control in (*RAMP_CONTROLS, *STOPS_CONTROLS, *GONE):
            self.assertNotIn(control, editor)

    def test_a_template_style_offers_customise_stops(self):
        self.assertIn("Customise stops", self.editor(self.page(self.template_style())))


class CustomEditorTests(OriginTestCase):
    def test_a_custom_style_is_the_whole_stops_editor_and_nothing_else(self):
        editor = self.editor(self.page(self.custom_style()))

        for control in ('name="name"', *STOPS_CONTROLS, *OUTSIDE_THE_STOPS, 'id="vs-preview"', 'id="vs-reverse"'):
            self.assertIn(control, editor)
        for control in (*RAMP_CONTROLS, *GONE, "vs-template-source", "Customise stops", "<details"):
            self.assertNotIn(control, editor)

    def test_scratch_is_the_custom_editor(self):
        editor = self.editor(self.client.get(self.form_url, {"style": "__new__", "start": "scratch"}))

        self.assertRegex(editor, r'<form[^>]*id="vs-style-form"[^>]*data-origin="custom"')
        self.assertIn('id="vs-stops-table"', editor)
        self.assertNotIn('name="ramp"', editor)

    def test_a_style_customised_from_a_template_says_so(self):
        editor = self.editor(self.page(self.custom_style(template=self.template)))

        self.assertIn("Customised from the template", editor)
        self.assertIn("House bands", editor)

    def test_a_style_customised_from_a_ramp_says_so(self):
        editor = self.editor(self.page(self.custom_style(ramp=self.org_ramp)))

        self.assertIn("Customised from the ramp", editor)
        self.assertIn("House Rainfall", editor)

    def test_a_custom_style_with_no_lineage_says_nothing_of_one(self):
        self.assertNotIn("Customised from", self.editor(self.page(self.custom_style())))


class PathLabelTests(OriginTestCase):
    def header(self, response):
        return re.search(r'<header class="vs-style-header".*?</header>', self.editor(response), re.S).group(0)

    def test_the_header_names_the_styles_path(self):
        for make, label in (
            (self.ramp_style, "From a ramp"),
            (self.template_style, "From a template"),
            (self.custom_style, "Custom"),
        ):
            with self.subTest(label=label):
                self.variable.styles.all().delete()
                self.assertIn(label, self.header(self.page(make())))


class SaveByOriginTests(OriginTestCase):
    def test_a_ramp_style_saves_what_its_ramp_produces_whatever_rows_are_posted(self):
        style = self.ramp_style()

        self.save(
            style,
            ramp=self.org_ramp.pk,
            mode="continuous",
            reverse="on",
            stop_value=["0", "42"],
            stop_color=["#123456", "#654321"],
        )

        style.refresh_from_db()
        self.assertEqual(style.stops, REVERSED)
        self.assertTrue(style.reverse)
        self.assertEqual(style.origin, VariableStyle.Origin.RAMP)

    def test_a_ramp_style_saves_its_ramp_mode_and_classes(self):
        style = self.ramp_style()

        self.save(style, ramp=self.org_ramp.pk, mode="stepped", steps="3")

        style.refresh_from_db()
        self.assertEqual((style.mode, style.steps, len(style.stops)), ("stepped", 3, 6))

    def test_a_ramp_style_is_regenerated_over_the_current_range(self):
        style = self.ramp_style()
        self.variable.value_max = 60
        self.variable.save(update_fields=["value_max"])

        self.save(style, ramp=self.org_ramp.pk, mode="continuous")

        style.refresh_from_db()
        self.assertEqual(style.stops[-1]["value"], 60.0)

    def test_a_ramp_style_needs_a_ramp(self):
        style = self.ramp_style()

        response = self.save(style, ramp="", mode="continuous")

        self.assertEqual(response.status_code, 200)
        style.refresh_from_db()
        self.assertEqual(style.ramp, self.org_ramp)

    def test_a_template_styles_stops_never_change_on_save(self):
        style = self.template_style()
        before = style.stops

        self.save(
            style,
            name="Renamed",
            over=OutOfRange.TRANSPARENT,
            ramp=self.org_ramp.pk,
            mode="continuous",
            stop_value=["0", "42"],
            stop_color=["#123456", "#654321"],
        )

        style.refresh_from_db()
        self.assertEqual(style.stops, before)
        self.assertEqual((style.name, style.over), ("Renamed", OutOfRange.TRANSPARENT))
        self.assertEqual((style.ramp, style.mode, style.origin), (None, "stepped", VariableStyle.Origin.TEMPLATE))

    def test_a_custom_style_saves_its_posted_rows(self):
        style = self.custom_style(ramp=self.org_ramp)

        self.save(style, stop_value=["0", "10", "50"], stop_color=["#000000", "#ff0000", "#ffffff"])

        style.refresh_from_db()
        self.assertEqual([stop["value"] for stop in style.stops], [0.0, 10.0, 50.0])
        self.assertEqual((style.ramp, style.origin), (self.org_ramp, VariableStyle.Origin.CUSTOM))


class CustomiseStopsTests(OriginTestCase):
    def customise(self, style):
        return self.client.post(self.form_url, {"action": "customise-stops", "style_slug": style.slug})

    def test_customising_a_ramp_style_keeps_its_stops_and_its_ramp_as_history(self):
        style = self.ramp_style()
        stops = style.stops

        response = self.customise(style)

        self.assertRedirects(response, f"{self.form_url}?style={style.slug}", fetch_redirect_response=False)
        style.refresh_from_db()
        self.assertEqual((style.origin, style.stops, style.ramp), (VariableStyle.Origin.CUSTOM, stops, self.org_ramp))
        self.assertIn("Customised from the ramp", self.editor(self.page(style)))

    def test_customising_a_template_style_keeps_its_stops(self):
        style = self.template_style()
        stops = style.stops

        self.customise(style)

        style.refresh_from_db()
        self.assertEqual((style.origin, style.stops), (VariableStyle.Origin.CUSTOM, stops))
        self.assertIn("Customised from the template", self.editor(self.page(style)))

    def test_a_customised_ramp_style_saves_its_rows_not_its_ramp(self):
        style = self.ramp_style()
        self.customise(style)

        self.save(
            style, ramp=self.org_ramp.pk, mode="continuous", stop_value=["0", "42"], stop_color=["#123456", "#654321"]
        )

        style.refresh_from_db()
        self.assertEqual(style.stops, TUNED)

    def test_customise_asks_first(self):
        editor = self.editor(self.page(self.ramp_style()))

        form = re.search(r'<form[^>]*id="vs-customise-form"[^>]*>', editor).group(0)
        self.assertIn("data-confirm=", form)
        self.assertIn('data-unsaved-discards="vs-style-form"', form)
