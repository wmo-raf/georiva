"""The Magics Style template source (#811): the generator's translation.

The generator (``georiva/scripts/magics_styles/generate.py``) is a dev-only
script, loaded here by path. Its seam is ``convert``: a source read from Magics
(named styles, the rule files that use them, the colour names) and the curated
config go in; templates and the styles it skipped, each with why, come out.
Reading Magics itself needs the network and is not tested here; the vendored
catalogue it wrote is (``test_style_catalogue``).
"""

import importlib.util
from pathlib import Path
from unittest import SkipTest

from django.test import SimpleTestCase

GENERATOR = Path(__file__).resolve().parents[4] / "scripts" / "magics_styles" / "generate.py"


def _generator():
    if not GENERATOR.exists():
        raise SkipTest(f"the generator is not in this image ({GENERATOR})")
    spec = importlib.util.spec_from_file_location("magics_generate", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# Magics' own definition of the style, as styles.json has it at the pinned commit.
SH_BLURED_F05T300LST = {
    "contour_description": "Method : contour shade, with contours Level list : (0.5/2/4/10/25/50/100/250) "
    "Colour : from blue to red Used for precipitation",
    "contour_level_list": "0.5/2/4/10/25/50/100/250",
    "contour_level_selection_type": "level_list",
    "contour_shade": "on",
    "contour_shade_colour_list": "cyan/greenish_blue/blue/bluish_purple/magenta/orange/red/charcoal",
    "contour_shade_colour_method": "list",
    "contour_shade_method": "area_fill",
    "contour_shade_min_level": "0.5",
}

# The named colours those styles use, as Magics' Colour.cc defines them.
COLOURS = {
    "cyan": (0.0, 1.0, 1.0, 1.0),
    "greenish_blue": (0.0, 0.5, 1.0, 1.0),
    "blue": (0.0, 0.0, 1.0, 1.0),
    "bluish_purple": (0.8536, 0.0, 1.0, 1.0),
    "magenta": (1.0, 0.0, 1.0, 1.0),
    "orange": (1.0, 0.5, 0.0, 1.0),
    "red": (1.0, 0.0, 0.0, 1.0),
    "charcoal": (0.2, 0.2, 0.2, 1.0),
    "none": None,
}

CONFIG = {
    "unit_strings": {"mm": "mm", "C": "°C"},
    "units": [
        {"symbol": "mm", "slug": "mm", "name": "Millimetre"},
        {"symbol": "°C", "slug": "degc", "name": "Degree Celsius"},
        {"symbol": "m/s", "slug": "ms", "name": "Metre per second"},
    ],
    "parameters": {
        "tp": {"label": "Precipitation", "quantity": "precipitation"},
        "sf": {"label": "Snowfall", "quantity": "precipitation"},
        "2t": {"label": "2 m temperature", "quantity": "temperature"},
        "ws": {"label": "Wind speed", "quantity": "wind", "unit": "m/s"},
    },
    "rules": {},
}


def _rule(file, styles, short_names, prefered_units=None):
    return {"file": file, "styles": styles, "short_names": short_names, "prefered_units": prefered_units}


class TranslationTests(SimpleTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.gen = _generator()

    def _convert(self, styles, rules, config=CONFIG, corrections=None):
        source = self.gen.MagicsSource(styles=styles, rules=rules, colours=COLOURS)
        return self.gen.convert(source, config, corrections or {})

    def _one(self, style, rules=None, **kwargs):
        rules = rules or [_rule("tp_interval.json", ["sh_x"], ["tp"], "mm")]
        templates, skipped = self._convert({"sh_x": style}, rules, **kwargs)
        self.assertEqual(skipped, [])
        (template,) = templates
        return template

    def test_sh_blured_f05t300lst_is_its_listed_levels_with_the_colours_magics_draws(self):
        templates, skipped = self._convert(
            {"sh_blured_f05t300lst": SH_BLURED_F05T300LST},
            [_rule("tp_interval.json", ["sh_blured_f05t300lst", "ct_blumag_lst"], ["tp", "cp"], "mm")],
        )
        self.assertEqual(skipped, [])
        (template,) = templates
        self.assertEqual(template["levels"], [0.5, 2, 4, 10, 25, 50, 100, 250])
        # Magics colours n levels with the first n - 1 colours of the list
        # (ColourTableDefinitionCompute::set); charcoal, the eighth, is never drawn.
        self.assertEqual(
            template["colors"], ["#00ffff", "#0080ff", "#0000ff", "#da00ff", "#ff00ff", "#ff8000", "#ff0000"]
        )
        # Nothing is drawn below 0.5 mm or above 250 mm: PolyShadingMethod fills no polygon outside the levels.
        self.assertEqual((template["under"], template["over"]), ("transparent", "transparent"))
        self.assertEqual(template["unit"], "mm")
        self.assertEqual(template["quantity"], "precipitation")
        self.assertEqual(template["source_name"], "sh_blured_f05t300lst")
        self.assertEqual(template["name"], "Precipitation, 0.5 to 250 mm, 7 bands")
        self.assertEqual(template["slug"], "magics-sh-blured-f05t300lst-mm")
        self.assertEqual(template["scale_type"], "log")

    def test_an_interval_style_is_the_levels_magics_computes(self):
        template = self._one(
            {
                "contour_level_selection_type": "interval",
                "contour_shade": "on",
                "contour_shade_min_level": -4,
                "contour_shade_max_level": 8,
                "contour_interval": 4,
                "contour_shade_colour_method": "list",
                "contour_shade_colour_list": "blue/cyan/orange",
            },
            rules=[_rule("2t.json", ["sh_x"], ["2t"], "C")],
        )
        self.assertEqual(template["levels"], [-4, 0, 4, 8])
        self.assertEqual(template["colors"], ["#0000ff", "#00ffff", "#ff8000"])
        self.assertEqual(template["scale_type"], "linear")
        self.assertEqual(template["name"], "2 m temperature, −4 to 8 °C, 3 bands")

    def test_levels_below_the_shading_minimum_are_dropped(self):
        style = {**SH_BLURED_F05T300LST, "contour_shade_min_level": 4}
        self.assertEqual(self._one(style)["levels"], [4, 10, 25, 50, 100, 250])

    def test_a_short_colour_list_repeats_its_last_colour(self):
        style = {**SH_BLURED_F05T300LST, "contour_shade_colour_list": "cyan/red"}
        self.assertEqual(self._one(style)["colors"], ["#00ffff"] + ["#ff0000"] * 6)

    def test_rgb_hsl_hex_and_none_colours_are_resolved(self):
        style = {
            **SH_BLURED_F05T300LST,
            "contour_level_list": "1/2/3/4/5/6",
            "contour_shade_colour_list": "rgb(0.5,0,1)/RGB(255,0,0)/rgba(0,0,1,0.5)/hsl(120,1,0.5)/#A0B0C0/none",
        }
        self.assertEqual(self._one(style)["colors"], ["#8000ff", "#ff0000", "#0000ff80", "#00ff00", "#a0b0c0"])
        style["contour_level_list"] = "1/2/3/4/5/6/7"
        self.assertEqual(self._one(style)["colors"][-1], "#00000000")

    def test_one_template_per_unit_among_the_rules_that_use_a_style(self):
        templates, _ = self._convert(
            {"sh_x": SH_BLURED_F05T300LST},
            [
                _rule("tp_interval.json", ["sh_x"], ["tp"], "mm"),
                _rule("sf_interval.json", ["sh_x"], ["sf"], "mm"),
                _rule("ws.json", ["sh_x"], ["ws"]),
            ],
        )
        self.assertEqual(sorted(t["unit"] for t in templates), ["m/s", "mm"])
        by_unit = {t["unit"]: t for t in templates}
        # The first rule in file order names it.
        self.assertEqual(by_unit["mm"]["name"], "Precipitation, 0.5 to 250 mm, 7 bands")
        self.assertEqual(by_unit["m/s"]["quantity"], "wind")
        self.assertEqual(by_unit["m/s"]["slug"], "magics-sh-x-ms")

    def test_a_preferred_rule_names_the_template(self):
        rules = [
            _rule("sf_interval.json", ["sh_x"], ["sf"], "mm"),
            _rule("tp_interval.json", ["sh_x"], ["tp"], "mm"),
        ]
        (template,) = self._convert({"sh_x": SH_BLURED_F05T300LST}, rules)[0]
        self.assertTrue(template["name"].startswith("Snowfall"))
        config = {**CONFIG, "prefer": ["tp_interval.json"]}
        (template,) = self._convert({"sh_x": SH_BLURED_F05T300LST}, rules, config=config)[0]
        self.assertTrue(template["name"].startswith("Precipitation"))

    def test_a_style_drawn_as_an_earlier_one_is_a_duplicate(self):
        cells = {**SH_BLURED_F05T300LST, "contour_shade_technique": "cell_shading"}
        templates, skipped = self._convert(
            {"sh_a": SH_BLURED_F05T300LST, "sh_a_cells": cells},
            [_rule("tp_interval.json", ["sh_a", "sh_a_cells"], ["tp"], "mm")],
        )
        self.assertEqual([t["source_name"] for t in templates], ["sh_a"])
        self.assertEqual([(s["source_name"], s["kind"]) for s in skipped], [("sh_a_cells", "duplicate")])

    def test_a_style_no_rule_uses_is_skipped(self):
        templates, skipped = self._convert({"sh_lonely": SH_BLURED_F05T300LST, "ct_lines": {}}, [])
        self.assertEqual(templates, [])
        self.assertEqual([(s["source_name"], s["kind"]) for s in skipped], [("sh_lonely", "unused")])

    def test_a_style_whose_unit_does_not_resolve_is_skipped(self):
        rules = [
            _rule("vorticity.json", ["sh_x"], ["vo"], "10**5/sec"),  # no parameter GeoRiva knows
            _rule("pv.json", ["sh_x"], ["tp"], "pv units"),  # a parameter, but a Magics unit GeoRiva lacks
            _rule("nc_tp_interval.json", ["sh_x"], ["tp"]),  # no unit named, and none for tp in the config
        ]
        templates, skipped = self._convert({"sh_x": SH_BLURED_F05T300LST}, rules)
        self.assertEqual(templates, [])
        (skip,) = skipped
        self.assertEqual(skip["kind"], "unit")
        for why in ("vorticity.json", "pv units", "nc_tp_interval.json"):
            self.assertIn(why, skip["reason"])

    def test_a_style_magics_colours_by_calculation_is_skipped(self):
        style = {**SH_BLURED_F05T300LST, "contour_shade_colour_method": "calculate"}
        _, skipped = self._convert({"sh_x": style}, [_rule("tp_interval.json", ["sh_x"], ["tp"], "mm")])
        self.assertEqual([s["kind"] for s in skipped], ["untranslatable"])

    def test_a_rule_override_names_a_spread_and_its_unit(self):
        override = {"label": "Temperature spread", "quantity": "other", "unit": "m/s"}
        config = {**CONFIG, "rules": {"t850_spread.json": override}}
        rules = [_rule("t850_spread.json", ["sh_x"], ["t"], "C")]
        template = self._one(SH_BLURED_F05T300LST, rules=rules, config=config)
        self.assertEqual(template["name"], "Temperature spread, 0.5 to 250 m/s, 7 bands")
        self.assertEqual((template["quantity"], template["unit"]), ("other", "m/s"))

    def test_spacing_gives_the_curve(self):
        cases = {
            "1/2/3/4/5": "linear",
            "0.1/0.2/0.5/1/2/5/10": "log",
            "0/1/2/4/7/11/16": "sqrt",
        }
        for levels, curve in cases.items():
            with self.subTest(levels=levels):
                style = {**SH_BLURED_F05T300LST, "contour_level_list": levels, "contour_shade_min_level": 0}
                self.assertEqual(self._one(style)["scale_type"], curve)

    def test_corrections_override_the_curve_and_the_group(self):
        template = self._one(SH_BLURED_F05T300LST, corrections={"sh_x": {"scale_type": "sqrt", "quantity": "other"}})
        self.assertEqual((template["scale_type"], template["quantity"]), ("sqrt", "other"))

    def test_two_styles_of_one_name_are_told_apart_by_their_colours(self):
        dark = {
            **SH_BLURED_F05T300LST,
            "contour_description": "Colour : dark blue to red Used for precipitation",
            "contour_shade_colour_list": "blue/cyan/red",
        }
        templates, _ = self._convert(
            {"sh_a": SH_BLURED_F05T300LST, "sh_b": dark}, [_rule("tp_interval.json", ["sh_a", "sh_b"], ["tp"], "mm")]
        )
        self.assertEqual(
            sorted(t["name"] for t in templates),
            [
                "Precipitation, 0.5 to 250 mm, 7 bands, dark blue to red",
                "Precipitation, 0.5 to 250 mm, 7 bands, from blue to red",
            ],
        )


class ColourNamesTests(SimpleTestCase):
    def test_the_colour_table_is_read_up_to_the_disabled_css_names(self):
        source = """
    colours_["none"]             = Rgb(-1., -1., -1.);
    colours_["charcoal"]         = Rgb(0.2000, 0.2000, 0.2000);
#if 0
    colours_["aqua"]                 = Colour("#00FFFF");
    colours_["transparent"] = Rgb(0, 0, 0, 0);
#endif
"""
        self.assertEqual(_generator().read_colour_names(source), {"none": None, "charcoal": (0.2, 0.2, 0.2, 1.0)})
