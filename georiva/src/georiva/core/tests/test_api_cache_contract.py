"""What core promises the `/api/` proxy cache.

Nginx caches a response under `/api/` only when the upstream marks it
`Cache-Control: public` — there is no `proxy_cache_valid` in that block, so
nginx invents a lifetime for nothing. The safety of that default is therefore
not a property of the nginx config at all. It is a property of **core's views**:
the cache stays empty because nothing core serves says it may be shared.

This is the test of that half. The other half — that nginx really does store
nothing without the marking — lives in `deploy/nginx/nginx.conf` and cannot be
asserted from here, in the same way and for the same reason the tile gateway's
`error_page` line cannot be.

It walks core's real pattern table rather than a list written out by hand, so an
endpoint added tomorrow is covered without anybody remembering this file. Plugin
routes are deliberately excluded: a plugin marking a response public is the
mechanism working, not a violation of it, and `core_urlpatterns` is where that
line is drawn.
"""

from django.test import TestCase
from django.urls import URLPattern, URLResolver

from georiva.api.urls import core_urlpatterns
from georiva.organisations.testing import dial_org


def parameterless_paths(patterns, prefix=""):
    """Every core `/api/` route that can be fetched without knowing any ids.

    Routes taking a slug or a pk are skipped: they need fixtures to answer at
    all, and what is under test is the response *header*, which no view varies
    by the identity of the thing it is describing. A view that marked itself
    public for one catalog and not another would be a stranger bug than this
    test is for.
    """
    for entry in patterns:
        route = str(entry.pattern)
        if isinstance(entry, URLResolver):
            yield from parameterless_paths(entry.url_patterns, prefix + route)
        elif isinstance(entry, URLPattern):
            full = prefix + route
            if "<" not in full and "(" not in full:
                yield "/api/" + full


class ApiCacheContractTests(TestCase):
    def setUp(self):
        dial_org(self.client)

    def test_there_are_routes_to_check(self):
        """The walk is only worth anything if it finds something. Without this a
        rename that made `parameterless_paths` yield nothing would leave every
        assertion below passing vacuously."""
        self.assertGreater(len(list(parameterless_paths(core_urlpatterns))), 3)

    def test_no_core_api_response_declares_itself_publicly_cacheable(self):
        """The premise the whole `/api/` cache rests on.

        The day a view returning tenant-scoped data gains
        `Cache-Control: public`, nginx will begin storing it under a key that
        carries no credential and serving it to anonymous callers. Nothing
        downstream reports that; this does.
        """
        for path in parameterless_paths(core_urlpatterns):
            with self.subTest(path=path):
                response = self.client.get(path)
                cache_control = response.headers.get("Cache-Control", "")

                self.assertNotIn("public", cache_control)
