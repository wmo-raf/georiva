"""Admin pages take their colours from theme-aware tokens (#577).

``.claude/docs/template_conventions.md`` → *Theme-aware colours*: every admin
page must read in both the light and the dark theme, so colour comes from the
``--w-color-*`` semantic tokens that flip with the theme
(``--w-color-surface-header``, ``--w-color-text-meta``,
``--w-color-border-furniture``, …). Three shapes don't flip, and all fail
quietly — the page renders, just as a near-white block under light text:

- the ``--w-color-grey-*`` scale. It is the same in both themes, so a
  ``grey-50`` panel header stays near-white in dark mode;
- a fixed colour (hex, ``rgb()``/``hsl()``, ``white``/``black``) on a colour
  property;
- ``--w-color-white`` / ``--w-color-black`` as a surface (a background or a
  border). They are the same in both themes; a white panel body is
  ``--w-color-surface-page``. White *text* is fine, on a filled colour that
  doesn't flip either (a status chip, an active step).

Shadows (``box-shadow``) are not scanned: a shadow is black in either theme.

Scanned: admin templates, the apps' own stylesheets and the Vue sources of the
admin dashboard. Not scanned: the public site (``pages/``,
``config/templates/georiva/``, ``config/static/css/``, the datetime selector
the dataset pages use) — a separate design system with no theme switch —
vendored and built files, and collectstatic's output.
"""

import re
from pathlib import Path

from django.test import SimpleTestCase

PACKAGE_ROOT = Path(__file__).resolve().parents[2]

PUBLIC_ROOTS = (
    PACKAGE_ROOT / "pages",
    PACKAGE_ROOT / "config" / "templates" / "georiva",
    PACKAGE_ROOT / "config" / "templates" / "500.html",
    PACKAGE_ROOT / "config" / "static" / "css",
    PACKAGE_ROOT / "config" / "datetime-selector",
)

NOT_SOURCE = (
    PACKAGE_ROOT / "static",  # collectstatic's output
    PACKAGE_ROOT / "ingestion" / "static" / "vue",  # the dashboard's build
)
NOT_SOURCE_PARTS = {"node_modules", "vendor", "dist"}

# The token, and the utility classes built on it (``w-text-grey-400``).
GREY_SCALE = re.compile(r"--w-color-grey-\d+|\bw-(?:text|bg|border)-grey-\d+")

COLOUR_PROPERTY = (
    r"(?<![-\w])(?:color|background(?:-color)?|border(?:-(?:top|right|bottom|left))?(?:-color)?"
    r"|outline(?:-color)?|fill|stroke|stop-color|caret-color|accent-color"
    # A page-local variable is fine, defined *from* the tokens — not from a hex.
    # Bar a shadow, which is black in either theme.
    r"|--(?![\w-]*shadow)[\w-]+)"
)
FIXED_COLOUR = r"(?:#[0-9a-fA-F]{3,8}\b|\b(?:rgba?|hsla?)\(|(?<![-\w])(?:white|black)(?![-\w]))"
FIXED = re.compile(COLOUR_PROPERTY + r"\s*[:=]\s*[\"']?[^;\"'{}]*?" + FIXED_COLOUR)

SURFACE_PROPERTY = r"(?<![-\w])(?:background(?:-color)?|border(?:-(?:top|right|bottom|left))?(?:-color)?)"
FIXED_TOKEN = re.compile(SURFACE_PROPERTY + r"\s*:[^;{}]*--w-color-(?:white|black)(?![-\w])")

RULES = {
    "grey": (GREY_SCALE, "the grey scale doesn't flip with the theme — use a semantic --w-color-* token"),
    "fixed-token": (FIXED_TOKEN, "white/black don't flip with the theme — a surface is --w-color-surface-page"),
    "fixed": (FIXED, "a fixed colour doesn't flip with the theme — use a semantic --w-color-* token"),
}

# Colours that are right in both themes, each with its reason in a comment
# beside it. Pinned as a count, so a new one is a decision rather than an
# accident.
ALLOWED = {
    # The live-preview overlays sit on the map, dark by design.
    ("core/templates/core/variable_styling.html", "fixed"): 7,
    # The org hopper sits in the always-dark sidebar; its avatars are a
    # categorical palette.
    ("organisations/static/organisations/org_hopper.css", "fixed"): 9,
    # The footer's text sits on the always-dark sidebar.
    ("config/templates/wagtailadmin/base.html", "fixed"): 4,
}


def _excluded(path):
    if NOT_SOURCE_PARTS.intersection(path.parts):
        return True
    return any(path.is_relative_to(root) for root in PUBLIC_ROOTS + NOT_SOURCE)


def scanned_files():
    patterns = ("templates/**/*.html", "static/**/*.css", "**/src/**/*.vue")
    paths = {path for pattern in patterns for path in PACKAGE_ROOT.rglob(pattern)}
    return sorted(path for path in paths if not _excluded(path))


class AdminColoursAreThemeAwareTests(SimpleTestCase):
    def test_the_scan_finds_admin_sources(self):
        names = {str(path.relative_to(PACKAGE_ROOT)) for path in scanned_files()}

        self.assertIn("events/templates/georivaevents/timeline.html", names)
        self.assertIn("core/static/core/css/feed_rows.css", names)
        self.assertIn("ingestion/ingestion-dashboard/src/IngestionDashboard.vue", names)
        self.assertNotIn("pages/datasets/static/css/georiva-map.css", names)  # public
        self.assertNotIn("config/static/css/georiva.css", names)  # public
        self.assertFalse(any(name.startswith("static/") for name in names))  # collectstatic

    def test_the_fixed_colour_rule(self):
        def hit(line):
            return bool(FIXED.search(line))

        self.assertTrue(hit("color: #fff;"))
        self.assertTrue(hit("background: var(--w-color-grey-100, #f5f5f5);"))
        self.assertTrue(hit("border-bottom: 1px solid rgba(0, 0, 0, .1);"))
        self.assertTrue(hit('<div style="color:#ababab">'))
        self.assertTrue(hit('<path fill="#334155"/>'))
        self.assertTrue(hit("background: white;"))
        self.assertTrue(hit("--geo-bg: #fff;"))
        self.assertFalse(hit("--geo-bg: var(--w-color-surface-page);"))
        self.assertFalse(hit("color: white-space;"))
        self.assertFalse(hit("--geo-shadow-sm: 0 1px 2px 0 rgb(0 0 0 / 0.05);"))
        self.assertFalse(hit("color: var(--w-color-white);"))
        self.assertFalse(hit("box-shadow: 0 1px 4px rgba(0, 0, 0, .08);"))
        self.assertFalse(hit("{# Failure scope (#477) #}"))
        self.assertFalse(hit('<a href="#top">'))

    def test_the_fixed_token_rule(self):
        def hit(line):
            return bool(FIXED_TOKEN.search(line))

        self.assertTrue(hit("background: var(--w-color-white);"))
        self.assertTrue(hit("border: 1px solid var(--w-color-black);"))
        self.assertFalse(hit("color: var(--w-color-white);"))
        self.assertFalse(hit("background: var(--w-color-white-80);"))
        self.assertFalse(hit("background: var(--w-color-surface-page);"))

    def test_no_colours_that_ignore_the_theme(self):
        offences = []
        for path in scanned_files():
            name = str(path.relative_to(PACKAGE_ROOT))
            lines = path.read_text().splitlines()
            for rule, (pattern, remedy) in RULES.items():
                hits = [lineno for lineno, line in enumerate(lines, start=1) if pattern.search(line)]
                if len(hits) != ALLOWED.get((name, rule), 0):
                    offences += [f"{name}:{lineno}: {remedy}" for lineno in hits]

        self.assertEqual(offences, [], f"\n{len(offences)} offences\n" + "\n".join(offences))
