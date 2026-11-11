"""Admin templates take their UI markup from Wagtail's styleguide (#575).

``.claude/docs/template_conventions.md`` says to copy buttons, icons and status
tags from ``/admin/styleguide/`` rather than hand-roll them. Two hand-rolled
shapes kept coming back, and both fail quietly — the page renders, just wrong:

- ``w-status-tag`` has no styles in Wagtail 7, so a tag built from it renders
  as bare text. ``{% status %}`` is the styleguide's tag.
- An inline ``<svg>`` either duplicates what ``{% icon %}`` renders and drifts
  from it (one had ``<use ref=…>`` and never drew), or draws an icon Wagtail
  already ships.

The public site (``pages/`` and ``config/templates/georiva/``) has its own
design system and is not scanned.
"""

from pathlib import Path

from django.test import SimpleTestCase

PACKAGE_ROOT = Path(__file__).resolve().parents[2]

PUBLIC_TEMPLATE_ROOTS = (
    PACKAGE_ROOT / "pages",
    PACKAGE_ROOT / "config" / "templates" / "georiva",
)

FORBIDDEN = {
    "w-status-tag": 'a status tag is {% status label="…" classname="w-status--primary" %}',
    "<svg": 'an icon is {% icon name="…" %} (wrapped=1 inside a button)',
}

# Inline SVG the styleguide has no equivalent for: the item preview's map
# overlay (basemap dropdown and its checkmarks, the layer legend's close).
# Pinned as a count, so a new one is a decision rather than an accident.
ALLOWED = {
    ("visualization/templates/visualization/item_preview.html", "<svg"): 5,
}


def admin_templates():
    for path in sorted(PACKAGE_ROOT.rglob("templates/**/*.html")):
        if any(path.is_relative_to(root) for root in PUBLIC_TEMPLATE_ROOTS):
            continue
        yield path


class AdminTemplatesUseStyleguideMarkupTests(SimpleTestCase):
    def test_the_scan_finds_admin_templates(self):
        names = {path.name for path in admin_templates()}

        self.assertIn("collection_items.html", names)
        self.assertNotIn("item_detail.html", names)  # pages/datasets — public

    def test_no_hand_rolled_status_tags_or_icons(self):
        offences = []
        for path in admin_templates():
            name = str(path.relative_to(PACKAGE_ROOT))
            lines = path.read_text().splitlines()
            for needle, remedy in FORBIDDEN.items():
                hits = [lineno for lineno, line in enumerate(lines, start=1) if needle in line]
                if len(hits) != ALLOWED.get((name, needle), 0):
                    offences += [f"{name}:{lineno}: {remedy}" for lineno in hits]

        self.assertEqual(offences, [], "\n" + "\n".join(offences))
