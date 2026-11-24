"""Applying a Style template on a Variable's Styling surface (#791).

One seam: the per-variable Styling page, driven through the admin test client.
The picker it renders, the style an Apply posts into being, and the range and
curve a default style proposes — accepted only when the operator says so.
"""

from django.test import TestCase
from django.urls import reverse

from georiva.core.models import OutOfRange, StyleTemplate, Unit, Variable, VariableStyle
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.testing import dial_org, make_org_tree, make_organisation
from georiva.organisations.tests.factories import PASSWORD, grant_everything, make_user


def _unit(symbol):
    found, _ = Unit.objects.get_or_create(symbol=symbol, defaults={"name": f"test {symbol}"})
    return found


def _template(organisation=None, **overrides):
    fields = {
        "organisation": organisation,
        "name": "Rain bands",
        "slug": "rain-bands",
        "quantity": StyleTemplate.Quantity.PRECIPITATION,
        "levels": [0.5, 1, 2, 5],
        "colors": ["#ffffff", "#a0c8f0", "#0000ff"],
        "unit": _unit("mm"),
        "under": OutOfRange.TRANSPARENT,
        "over": OutOfRange.CLAMP,
        "scale_type": Variable.ScaleType.SQRT,
    }
    fields.update(overrides)
    template = StyleTemplate(**fields)
    template.full_clean()
    template.save()
    return template


class ApplyStyleTemplateTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.org = make_organisation()
        cls.other_org = make_organisation("other-org")
        cls.tree = make_org_tree(cls.org)
        cls.collection = cls.tree["collection"]
        cls.variable = cls.tree["variable"]

    def setUp(self):
        self.user = grant_everything(make_user("amina"))
        OrganisationMembership.objects.create(
            user=self.user, organisation=self.org, role=OrganisationMembership.Role.ADMIN
        )
        dial_org(self.client)
        self.client.login(username="amina", password=PASSWORD)

    def form_url(self, variable=None):
        variable = variable or self.variable
        return reverse("variable_styling", args=[self.collection.pk, variable.pk])

    def make_variable(self, slug, source_unit, unit=None, *, water=False, value_min=0, value_max=50, **extra):
        return Variable.objects.create(
            collection=self.collection,
            name=slug,
            slug=slug,
            source_unit=_unit(source_unit),
            unit=_unit(unit or source_unit),
            water=water,
            value_min=value_min,
            value_max=value_max,
            **extra,
        )

    def apply(self, template, variable=None, *, follow=False, **fields):
        return self.client.post(
            self.form_url(variable), {"action": "apply-template", "template": template.pk, **fields}, follow=follow
        )


class TemplatePickerTests(ApplyStyleTemplateTestCase):
    """The picker offers the templates whose unit converts into the Variable's
    source unit, grouped by quantity; the rest are there on request, with why."""

    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        # The shipped catalogue is in every test database (#789); this picker is
        # judged on templates the test controls.
        StyleTemplate.objects.all().delete()
        cls.celsius = _template(
            name="Warm",
            slug="warm",
            quantity=StyleTemplate.Quantity.TEMPERATURE,
            unit=_unit("°C"),
            levels=[0, 1],
            colors=["#ff0000"],
        )
        cls.fahrenheit = _template(
            name="Warm °F",
            slug="warm-f",
            quantity=StyleTemplate.Quantity.TEMPERATURE,
            unit=_unit("°F"),
            levels=[32, 50],
            colors=["#ff0000"],
        )
        cls.rain = _template(organisation=cls.org)
        cls.foreign = _template(organisation=cls.other_org, name="Foreign rain", slug="foreign-rain")

    def picker(self, variable):
        return self.client.get(self.form_url(variable)).context["template_picker"]

    @staticmethod
    def names(groups):
        return {group["label"]: [row["name"] for row in group["templates"]] for group in groups}

    def test_a_kelvin_variable_is_offered_celsius_and_fahrenheit_templates_only(self):
        picker = self.picker(self.make_variable("t2m", "K", value_min=230, value_max=320))

        self.assertEqual(self.names(picker["offered"]), {"Temperature": ["Warm", "Warm °F"]})
        self.assertEqual(self.names(picker["excluded"]), {"Precipitation": ["Rain bands"]})

    def test_an_mm_template_is_offered_for_a_water_variable_in_kg_per_square_metre(self):
        picker = self.picker(self.make_variable("tp", "kg/m²", water=True))

        self.assertEqual(self.names(picker["offered"]), {"Precipitation": ["Rain bands"]})

    def test_an_excluded_template_says_why(self):
        picker = self.picker(self.make_variable("dust", "kg/m²"))

        self.assertEqual(picker["offered"], [])
        rain = next(row for group in picker["excluded"] for row in group["templates"] if row["name"] == "Rain bands")
        self.assertIn("water", rain["reason"])

    def test_groups_follow_the_catalogue_order_of_quantities(self):
        _template(name="Aa cloud", slug="aa-cloud", quantity=StyleTemplate.Quantity.CLOUD)
        _template(name="Zz snow depth", slug="zz", quantity=StyleTemplate.Quantity.TEMPERATURE)

        picker = self.picker(self.make_variable("tp", "mm"))

        self.assertEqual([group["label"] for group in picker["offered"]], ["Temperature", "Precipitation", "Cloud"])

    def test_another_organisations_templates_are_not_listed(self):
        picker = self.picker(self.make_variable("tp", "mm"))

        everything = picker["offered"] + picker["excluded"]
        self.assertNotIn("Foreign rain", str(self.names(everything)))

    def test_the_page_shows_the_offered_templates_and_hides_the_rest_behind_show_all(self):
        page = self.client.get(self.form_url(self.make_variable("dust", "kg/m²")))

        self.assertContains(page, 'value="apply-template"')
        self.assertContains(page, "Show all templates")
        self.assertContains(page, "holds only for water")


class ApplyWritesASteppedStyleTests(ApplyStyleTemplateTestCase):
    def test_a_kelvin_variable_takes_a_celsius_template_as_kelvin_stops(self):
        kelvin = self.make_variable("t2m", "K", "°C", value_min=233.15, value_max=323.15)
        template = _template(
            name="Warm bands",
            slug="warm-bands",
            quantity=StyleTemplate.Quantity.TEMPERATURE,
            levels=[-10, 0, 10],
            colors=["#0000ff", "#ff000080"],
            unit=_unit("°C"),
            under=OutOfRange.CLAMP,
            over=OutOfRange.TRANSPARENT,
        )

        response = self.apply(template, kelvin)

        self.assertEqual(response.status_code, 302)
        style = kelvin.styles.get()
        self.assertEqual(style.mode, VariableStyle.Mode.STEPPED)
        self.assertEqual(style.steps, 2)
        self.assertEqual(
            style.stops,
            [
                {"value": 263.15, "color": "#0000ff"},
                {"value": 273.15, "color": "#0000ff"},
                {"value": 273.15, "color": "#ff000080"},
                {"value": 283.15, "color": "#ff000080"},
            ],
        )
        self.assertEqual((style.under, style.over), (OutOfRange.CLAMP, OutOfRange.TRANSPARENT))
        self.assertEqual(style.template, template)
        self.assertEqual(style.name, "Warm bands")

    def test_an_mm_template_applies_to_a_water_variable_in_kg_per_square_metre(self):
        rain = self.make_variable("tp", "kg/m²", water=True)

        self.apply(_template(), rain)

        style = rain.styles.get()
        self.assertEqual([stop["value"] for stop in style.stops], [0.5, 1, 1, 2, 2, 5])

    def test_an_mm_template_is_refused_for_a_kg_per_square_metre_variable_that_is_not_water(self):
        dust = self.make_variable("dust", "kg/m²")

        response = self.apply(_template(), dust, make_default="on")

        self.assertEqual(response.status_code, 302)
        self.assertFalse(dust.styles.exists())

    def test_another_organisations_template_cannot_be_applied(self):
        foreign = _template(organisation=self.other_org, slug="foreign")

        self.apply(foreign)

        self.assertFalse(self.variable.styles.exists())

    def test_the_applied_style_is_a_snapshot(self):
        template = _template()
        self.apply(template, self.make_variable("tp", "mm"))
        style = VariableStyle.objects.get(template=template)
        applied = list(style.stops)

        template.levels = [1, 2, 3, 4]
        template.colors = ["#000000", "#111111", "#222222"]
        template.save()
        style.refresh_from_db()
        self.assertEqual(style.stops, applied)

        template.delete()
        style.refresh_from_db()
        self.assertIsNone(style.template)
        self.assertEqual(style.stops, applied)

    def test_a_second_style_is_not_the_default_unless_asked(self):
        rain = self.make_variable("tp", "mm")
        self.apply(_template(), rain)
        self.apply(_template(slug="other", name="Other"), rain)
        self.assertEqual(rain.default_style.name, "Rain bands")

        self.apply(_template(slug="third", name="Third"), rain, make_default="on")
        self.assertEqual(VariableStyle.objects.get(variable=rain, is_default=True).name, "Third")


class ProposedEncodingTests(ApplyStyleTemplateTestCase):
    """A template's range and curve change the Variable only on a default style,
    and only once the operator confirms them."""

    def setUp(self):
        super().setUp()
        # Stored in kg/m², served in mm: the proposal reads in the served unit.
        self.rain = self.make_variable("tp", "kg/m²", "mm", water=True, value_min=0, value_max=50)
        self.template = _template(levels=[0.5, 1, 2, 200], scale_type=Variable.ScaleType.SQRT)

    def encoding(self):
        self.rain.refresh_from_db()
        return self.rain.value_min, self.rain.value_max, self.rain.scale_type

    def accept(self, slug):
        return self.client.post(self.form_url(self.rain), {"action": "accept-template-encoding", "style_slug": slug})

    def test_applying_a_default_style_proposes_but_does_not_change_the_encoding(self):
        response = self.apply(self.template, self.rain)

        self.assertEqual(self.encoding(), (0, 50, Variable.ScaleType.LINEAR))
        page = self.client.get(response.url)
        proposal = page.context["encoding_proposal"]
        self.assertEqual((proposal["value_min"], proposal["value_max"]), (0.5, 200))
        self.assertEqual(proposal["scale_type"], Variable.ScaleType.SQRT)
        self.assertContains(page, 'value="accept-template-encoding"')

    def test_confirming_the_proposal_sets_the_range_and_curve(self):
        self.apply(self.template, self.rain)

        response = self.accept("rain-bands")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(self.encoding(), (0.5, 200, Variable.ScaleType.SQRT))

    def test_a_non_default_style_proposes_nothing_and_cannot_be_confirmed(self):
        VariableStyle.objects.create(variable=self.rain, name="House", slug="house", is_default=True)

        response = self.apply(self.template, self.rain)
        page = self.client.get(f"{response.url}&propose=encoding")
        self.assertIsNone(page.context["encoding_proposal"])

        self.accept("rain-bands")
        self.assertEqual(self.encoding(), (0, 50, Variable.ScaleType.LINEAR))

    def test_a_style_made_by_hand_has_nothing_to_confirm(self):
        VariableStyle.objects.create(variable=self.rain, name="House", slug="house", is_default=True)

        self.accept("house")

        self.assertEqual(self.encoding(), (0, 50, Variable.ScaleType.LINEAR))

    def test_a_log_template_whose_range_starts_at_zero_proposes_a_linear_curve(self):
        template = _template(slug="log-from-zero", levels=[0, 1, 10, 100], scale_type=Variable.ScaleType.LOG)
        self.apply(template, self.rain)

        self.accept("rain-bands")

        self.assertEqual(self.encoding(), (0, 100, Variable.ScaleType.LINEAR))


class WarningTests(ApplyStyleTemplateTestCase):
    """Whatever encoding is in force, the operator is told which bands it
    starves of codes and which levels it clips."""

    def setUp(self):
        super().setUp()
        # 0–200 mm linear: a code is about 0.8 mm, so the 0.5–1 and 1–2 bands
        # get one code each; 300 mm is beyond the range.
        self.rain = self.make_variable("tp", "mm", value_min=0, value_max=200)
        self.template = _template(levels=[0.5, 1, 2, 50, 300], colors=["#111111", "#222222", "#333333", "#444444"])

    def warnings(self, response):
        return [str(message) for message in response.context["messages"] if message.level_tag == "warning"]

    def test_applying_names_starved_bands_and_clipped_levels(self):
        response = self.apply(self.template, self.rain, follow=True)

        starved, clipped = self.warnings(response)
        self.assertIn("0.5–1 mm, 1–2 mm.", starved)
        self.assertNotIn("2–50", starved)
        self.assertIn("300 mm", clipped)
        self.assertNotIn("50,", clipped)

    def test_a_fitting_encoding_warns_of_nothing(self):
        response = self.apply(
            _template(slug="fits", levels=[0, 100, 200], colors=["#111111", "#222222"]), self.rain, follow=True
        )

        self.assertEqual(self.warnings(response), [])

    def test_the_proposal_warns_under_the_proposed_encoding(self):
        response = self.apply(self.template, self.rain, follow=True)

        # 0.5–300 mm on a sqrt curve: every band has codes, no level is clipped.
        self.assertEqual(response.context["encoding_proposal"]["warnings"], [])

    def test_accepting_the_proposal_warns_under_the_new_encoding(self):
        narrow = _template(
            slug="narrow", levels=[0, 0.1, 100], colors=["#111111", "#222222"], scale_type=Variable.ScaleType.LINEAR
        )
        self.apply(narrow, self.rain, follow=True)

        response = self.client.post(
            self.form_url(self.rain),
            {"action": "accept-template-encoding", "style_slug": "rain-bands"},
            follow=True,
        )

        (starved,) = self.warnings(response)
        self.assertIn("0–0.1 mm", starved)
