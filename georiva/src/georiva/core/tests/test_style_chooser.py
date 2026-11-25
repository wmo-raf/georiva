"""Starting a style (#800): the chooser a new style opens on.

One seam: the per-variable Styling page, driven through the admin test client.
A variable with no styles, and the "+ New style" tab, open a chooser with three
starting points — a Style template, a color ramp, or scratch — instead of the
editor. Each start is read through what it stores and the page it lands on.
"""

import re

from georiva.core import style_application
from georiva.core.models import StyleTemplate, Variable, VariableStyle
from georiva.core.tests.test_apply_style_template import _template, _unit
from georiva.core.tests.test_styling_surface import StylingSurfaceTestCase


def stops(*values):
    return [{"value": value, "color": "#123456"} for value in values]


class ChooserPageTestCase(StylingSurfaceTestCase):
    @classmethod
    def mm_variable(cls, value_max=50):
        """A Variable in mm, which the rain templates convert into."""
        return Variable.objects.create(
            collection=cls.collection,
            name="tp",
            slug="tp",
            source_unit=_unit("mm"),
            unit=_unit("mm"),
            value_min=0,
            value_max=value_max,
        )

    def chooser(self, response):
        html = response.content.decode()
        match = re.search(r'<section[^>]*id="vs-chooser".*?</section>', html, re.S)
        return match.group(0) if match else None

    def editor_hidden(self, response):
        html = response.content.decode()
        tag = re.search(r'<section[^>]*id="vs-style-editor"[^>]*>', html).group(0)
        return re.search(r"\bhidden\b", tag) is not None


class ChooserShownTests(ChooserPageTestCase):
    def test_a_variable_with_no_styles_shows_the_chooser_not_the_editor(self):
        response = self.client.get(self.form_url)

        self.assertIsNotNone(self.chooser(response))
        self.assertTrue(self.editor_hidden(response))

    def test_new_style_shows_the_same_chooser(self):
        VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official", is_default=True, stops=stops(0, 50)
        )

        response = self.client.get(self.form_url, {"style": "__new__"})

        self.assertIsNotNone(self.chooser(response))
        self.assertTrue(self.editor_hidden(response))

    def test_a_saved_style_shows_the_editor_not_the_chooser(self):
        VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official", is_default=True, stops=stops(0, 50)
        )

        response = self.client.get(self.form_url)

        self.assertIsNone(self.chooser(response))
        self.assertFalse(self.editor_hidden(response))

    def test_the_chooser_offers_a_template_a_ramp_and_scratch(self):
        chooser = self.chooser(self.client.get(self.form_url))

        self.assertIn('value="apply-template"', chooser)
        self.assertIn('value="start-from-ramp"', chooser)
        self.assertIn("start=scratch", chooser)

    def test_the_chooser_has_no_make_default_control(self):
        chooser = self.chooser(self.client.get(self.form_url))

        self.assertNotIn("make_default", chooser)
        self.assertNotIn("Make it the default", chooser)


class TemplateStartTests(ChooserPageTestCase):
    """A template applied with or without its range and curve, decided before
    it is applied, in the same request."""

    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        # 0–50 mm linear before; the template is laid out for 0.5–200 mm, sqrt.
        cls.variable = cls.mm_variable()
        cls.template = _template(levels=[0.5, 1, 2, 200], scale_type=Variable.ScaleType.SQRT)

    def encoding(self):
        self.variable.refresh_from_db()
        return self.variable.value_min, self.variable.value_max, self.variable.scale_type

    def apply(self, **fields):
        return self.client.post(
            self.form_url, {"action": "apply-template", "template": self.template.pk, **fields}, follow=True
        )

    def test_ticked_sets_the_range_and_curve_with_the_style(self):
        self.apply(accept_encoding="on")

        self.assertEqual(self.encoding(), (0.5, 200, Variable.ScaleType.SQRT))
        self.assertTrue(self.variable.styles.get().is_default)

    def test_unticked_leaves_the_range_and_curve_alone(self):
        self.apply()

        self.assertEqual(self.encoding(), (0, 50, Variable.ScaleType.LINEAR))
        self.assertTrue(self.variable.styles.get().is_default)

    def test_the_checkbox_is_offered_ticked_when_the_variable_has_no_styles(self):
        chooser = self.chooser(self.client.get(self.form_url))

        self.assertIn('<input type="checkbox" name="accept_encoding" checked>', chooser)
        self.assertIn("Also use its range and curve", chooser)

    def test_the_checkbox_is_not_offered_when_the_new_style_will_not_be_the_default(self):
        VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official", is_default=True, stops=stops(0, 50)
        )

        chooser = self.chooser(self.client.get(self.form_url, {"style": "__new__"}))

        self.assertNotIn("accept_encoding", chooser)

    def test_the_checkbox_is_ignored_when_the_new_style_will_not_be_the_default(self):
        VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official", is_default=True, stops=stops(0, 50)
        )

        self.apply(accept_encoding="on")

        self.assertEqual(self.encoding(), (0, 50, Variable.ScaleType.LINEAR))
        self.assertEqual(self.variable.default_style.slug, "official")

    def test_the_applied_style_opens_in_the_editor(self):
        response = self.apply()

        self.assertEqual(response.context["selected"].template, self.template)
        self.assertFalse(response.context["show_chooser"])

    def test_the_template_shows_its_range_and_curve_beside_the_variables(self):
        chooser = self.chooser(self.client.get(self.form_url))

        self.assertIn("0.0 – 50.0 mm, linear", chooser)
        self.assertIn("Laid out for 0.5 – 200.0 mm, square root.", chooser)


class TemplateSourceTests(ChooserPageTestCase):
    """Shipped templates come from two catalogues (#811): each row says which,
    and the upstream style it was converted from."""

    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        cls.variable = cls.mm_variable()

    def row(self, chooser, name):
        rows = re.findall(r'<label class="vs-template[^"]*">.*?</label>', chooser, re.S)
        return next(row for row in rows if f'<span class="vs-template-name">{name}</span>' in row)

    def test_magics_and_earthkit_plots_templates_are_both_offered_each_with_its_source(self):
        chooser = self.chooser(self.client.get(self.form_url))

        magics = self.row(chooser, "Precipitation, 0.5 to 250 mm, 7 bands")
        self.assertIn('<span class="vs-template-source">Magics · sh_blured_f05t300lst</span>', magics)
        earthkit = self.row(chooser, "Precipitation, 0.5 to 200 mm, 17 bands")
        self.assertIn('<span class="vs-template-source">earthkit-plots · ', earthkit)

    def test_an_organisations_own_template_has_no_source(self):
        own = _template(name="Our rain")

        row = self.row(self.chooser(self.client.get(self.form_url)), own.name)

        self.assertNotIn("vs-template-source", row)


class TemplateWarningsBeforeApplyingTests(ChooserPageTestCase):
    """The warnings for the encoding that would be in force show with the
    template, before it is applied."""

    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        StyleTemplate.objects.all().delete()
        # 0–200 mm linear: the 0.5–1 and 1–2 bands get one code each and 300
        # is clipped; under the template's own 0.5–300 mm sqrt, nothing is.
        cls.variable = cls.mm_variable(value_max=200)
        cls.template = _template(levels=[0.5, 1, 2, 50, 300], colors=["#111111", "#222222", "#333333", "#444444"])

    def encoding_block(self, response):
        return re.search(r'<div class="vs-template-encoding">.*?</li>', response.content.decode(), re.S)[0]

    def warnings(self, block, when):
        return re.findall(rf'<div class="help-block help-warning"{when}>(.*?)</div>', block, re.S)

    def test_kept_and_accepted_encodings_each_carry_their_warnings(self):
        block = self.encoding_block(self.client.get(self.form_url))

        starved, clipped = self.warnings(block, ' data-when="keep"')
        self.assertIn("0.5–1 mm, 1–2 mm.", starved)
        self.assertIn("300 mm", clipped)
        self.assertEqual(self.warnings(block, ' data-when="accept"'), [])

    def test_without_the_checkbox_the_current_encodings_warnings_show(self):
        VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official", is_default=True, stops=stops(0, 50)
        )

        block = self.encoding_block(self.client.get(self.form_url, {"style": "__new__"}))

        self.assertEqual(len(self.warnings(block, "")), 2)
        self.assertNotIn("data-when", block)


class RampStartTests(ChooserPageTestCase):
    def start(self, **fields):
        return self.client.post(self.form_url, {"action": "start-from-ramp", **fields})

    def test_a_ramp_start_stores_the_ramp_over_the_current_range_and_opens_the_editor(self):
        response = self.start(ramp=self.org_ramp.pk, mode="continuous")

        style = self.variable.styles.get()
        self.assertRedirects(response, f"{self.form_url}?style={style.slug}", fetch_redirect_response=False)
        self.assertEqual(style.name, "House Rainfall")
        self.assertEqual(style.ramp, self.org_ramp)
        self.assertEqual(style.stops, [{"value": 0.0, "color": "#000000"}, {"value": 50.0, "color": "#ffffff"}])

    def test_a_stepped_ramp_start_has_its_classes(self):
        self.start(ramp=self.org_ramp.pk, mode="stepped", steps=4)

        style = self.variable.styles.get()
        self.assertEqual((style.mode, style.steps), (VariableStyle.Mode.STEPPED, 4))
        self.assertEqual(len(style.stops), 8)

    def test_another_organisations_ramp_is_refused(self):
        self.start(ramp=self.foreign_ramp.pk, mode="continuous")

        self.assertFalse(self.variable.styles.exists())

    def test_a_ramp_start_opens_the_ramp_editor(self):
        for mode, steps in (("continuous", None), ("stepped", 4)):
            with self.subTest(mode=mode):
                self.variable.styles.all().delete()
                self.start(ramp=self.org_ramp.pk, mode=mode, **({"steps": steps} if steps else {}))
                style = self.variable.styles.get()

                self.assertEqual(style.origin, VariableStyle.Origin.RAMP)
                response = self.client.get(self.form_url, {"style": style.slug})
                self.assertEqual(response.context["origin"], VariableStyle.Origin.RAMP)
                self.assertContains(response, 'data-origin="ramp"')

    def test_the_ramp_start_shows_the_current_range_with_a_link_to_change_it(self):
        chooser = self.chooser(self.client.get(self.form_url))

        self.assertIn("Spread over the current range, 0.0 – 50.0", chooser)
        self.assertIn('href="#vs-range" class="vs-open-range"', chooser)


class ScratchStartTests(ChooserPageTestCase):
    def test_scratch_opens_the_editor_with_an_empty_stops_table(self):
        response = self.client.get(self.form_url, {"style": "__new__", "start": "scratch"})

        self.assertIsNone(self.chooser(response))
        self.assertFalse(self.editor_hidden(response))
        self.assertEqual(response.context["stop_rows"], [])
        self.assertFalse(self.variable.styles.exists())

    def test_scratch_opens_the_custom_editor_with_its_json_panel(self):
        response = self.client.get(self.form_url, {"style": "__new__", "start": "scratch"})
        html = response.content.decode()

        self.assertEqual(response.context["origin"], VariableStyle.Origin.CUSTOM)
        self.assertIn('id="vs-json-toggle"', html)
        self.assertIn("getElementById('vs-json-toggle')?.click()", html)

    def test_start_from_scratch_is_guarded_like_the_tabs(self):
        chooser = self.chooser(self.client.get(self.form_url))

        guarded = re.findall(r"<[a-z]+[^>]*data-unsaved-guard.*?start=scratch", chooser, re.S)
        self.assertTrue(guarded, "the scratch link is not inside a [data-unsaved-guard]")

    def test_the_rendering_choices_are_the_styles_modes(self):
        chooser = self.chooser(self.client.get(self.form_url))

        for mode in VariableStyle.Mode.values:
            self.assertIn(f'name="mode" value="{mode}"', chooser)

    def test_the_chooser_does_not_open_the_json_panel(self):
        html = self.client.get(self.form_url).content.decode()

        self.assertNotIn("getElementById('vs-json-toggle')?.click()", html)

    def test_saving_with_no_stops_stores_nothing(self):
        response = self.client.post(
            self.form_url, {"action": "save-style", "style_slug": "", "name": "Blank", "mode": "continuous"}
        )

        self.assertEqual(response.status_code, 200)
        self.assertFalse(self.variable.styles.exists())
        self.assertFalse(self.editor_hidden(response))
        self.assertIn("at least one stop", response.content.decode())

    def test_an_existing_style_cannot_be_emptied(self):
        VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official", is_default=True, stops=stops(0, 50)
        )

        self.client.post(
            self.form_url, {"action": "save-style", "style_slug": "official", "name": "Official", "mode": "continuous"}
        )

        self.assertEqual(self.variable.styles.get().stops, stops(0, 50))


class FirstStyleIsTheDefaultTests(ChooserPageTestCase):
    def test_the_first_style_is_the_default_and_a_later_one_is_not(self):
        self.client.post(self.form_url, {"action": "start-from-ramp", "ramp": self.org_ramp.pk, "mode": "continuous"})
        self.client.post(
            self.form_url, {"action": "start-from-ramp", "ramp": self.global_ramp.pk, "mode": "continuous"}
        )

        self.assertEqual(self.variable.default_style.ramp, self.org_ramp)
        self.assertEqual(self.variable.styles.filter(is_default=True).count(), 1)

    def test_a_later_template_is_not_made_default_even_if_asked(self):
        self.variable = self.mm_variable()
        VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official", is_default=True, stops=stops(0, 50)
        )

        self.client.post(self.form_url, {"action": "apply-template", "template": _template().pk, "make_default": "on"})

        self.assertEqual(self.variable.default_style.slug, "official")


class StartFromRampTests(ChooserPageTestCase):
    """``style_application.start_from_ramp``: a style generated from a ramp,
    named and slugged after it, default only when it is the first."""

    def test_the_first_is_the_default_and_a_second_with_the_same_ramp_gets_its_own_slug(self):
        first = style_application.start_from_ramp(self.org_ramp, self.variable)
        second = style_application.start_from_ramp(
            self.org_ramp, self.variable, mode=VariableStyle.Mode.STEPPED, steps=3
        )

        self.assertTrue(first.is_default)
        self.assertFalse(second.is_default)
        self.assertNotEqual(first.slug, second.slug)
        self.assertEqual(len(second.stops), 6)
        self.assertEqual(second.origin, VariableStyle.Origin.RAMP)


class NewStyleIsDefaultTests(ChooserPageTestCase):
    def test_a_new_style_is_the_default_only_while_the_variable_has_none(self):
        self.assertTrue(self.variable.new_style_is_default)

        VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official", is_default=True, stops=stops(0, 50)
        )

        self.assertFalse(Variable.objects.get(pk=self.variable.pk).new_style_is_default)
