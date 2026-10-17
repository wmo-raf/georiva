"""No admin surface offers a Copy action (#285).

Wagtail's generic copy view binds the submitted form to the *source* row, so
"copying" renames and overwrites the original instead of creating a second row.
Until a working copy exists the action is switched off everywhere. The sweep walks
the URLs the project actually registers, so a viewset added next month is covered
the day it lands.
"""

from django.test import SimpleTestCase
from django.urls import get_resolver

from georiva.organisations.tests.urlsweep import flatten_url_patterns


class AdminCopyDisabledTests(SimpleTestCase):
    def test_no_copy_route_is_registered(self):
        copy_routes = sorted(
            f"{prefix}{pattern.pattern}"
            for pattern, prefix in flatten_url_patterns(get_resolver().url_patterns)
            if (pattern.name or "").endswith("copy")
            and "admin" in prefix
            and "/pages/" not in f"/{prefix}"  # Wagtail's purpose-built page copy
        )
        self.assertEqual(copy_routes, [])
