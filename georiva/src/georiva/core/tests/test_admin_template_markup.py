"""Admin templates take their UI markup from Wagtail's styleguide (#575).

``.claude/docs/template_conventions.md`` says to copy buttons, icons and status
tags from ``/admin/styleguide/`` rather than hand-roll them. Two hand-rolled
shapes kept coming back, and both fail quietly — the page renders, just wrong:

- ``w-status-tag`` has no styles in Wagtail 7, so a tag built from it renders
  as bare text. ``{% status %}`` is the styleguide's tag.
- A hand-expanded ``<svg><use href="#icon-…">`` duplicates what
  ``{% icon %}`` renders, and drifts from it (the ``wrapped=1`` spacing, the
  ``icon-<name>`` class).

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
    '<use href="#icon-': 'an icon is {% icon name="…" %} (wrapped=1 inside a button)',
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
            for lineno, line in enumerate(path.read_text().splitlines(), start=1):
                for needle, remedy in FORBIDDEN.items():
                    if needle in line:
                        offences.append(f"{path.relative_to(PACKAGE_ROOT)}:{lineno}: {remedy}")

        self.assertEqual(offences, [], "\n" + "\n".join(offences))
