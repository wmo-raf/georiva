"""Proof that the tile servers are told which organisation, and told correctly.

Titiler and Martin are the two services on this instance that answer without a
resolvable Host: the browser reaches them through nginx on the portal's own
hostname, and Titiler dials Django back on an internal container name that
belongs to no organisation at all. Neither can therefore do what
``OrganisationMiddleware`` does, so the organisation has to travel in the
address instead — and everything below exists to hold that arrangement to two
promises.

*Django decides, the tile servers concatenate.* Every machine-plane URL is built
from a row Django already fetched under a host-scoped query, so an org segment
can never disagree with the catalog beside it. The tests read the org out of the
generated URL rather than checking a format, because the failure that matters is
not a malformed path — it is a well-formed path naming the wrong tenant.

*Colliding slugs stay apart.* Two institutions may each run a ``forecast``
catalog (#267), so every fixture here is duplicated across two organisations
under the same slug. A key, a path or a lookup that drops the org segment stops
distinguishing them, and each test below is written so that dropping it fails.
"""
from datetime import datetime, timezone

from django.test import TestCase, override_settings
from django.urls import reverse

from georiva.core.machine_plane import (
    MARTIN_PREFIX,
    martin_boundary_stats_url,
    org_slug_of,
    titiler_preview_url,
    titiler_variable_root,
)
from georiva.core.models import Item, Variable
from georiva.core.palette_cache import (
    get_palette_cache_key,
    variable_cache_key,
    warm_all,
)
from georiva.organisations.testing import (
    SHARED_TREE_SLUG as SHARED_SLUG,
    make_org_tree,
    make_organisation,
)


def build_tree(organisation, *, variable_name):
    """A data chain under ``SHARED_SLUG`` — the same slug for every organisation."""
    return make_org_tree(organisation, name=variable_name)


class MachinePlaneAddressTests(TestCase):
    """The URLs and cache keys Django emits for Titiler and Martin."""

    @classmethod
    def setUpTestData(cls):
        cls.kenya = make_organisation("kenya")
        cls.uganda = make_organisation("uganda")
        cls.kenya_tree = build_tree(cls.kenya, variable_name="Kenya Forecast")
        cls.uganda_tree = build_tree(cls.uganda, variable_name="Uganda Forecast")

    def test_org_is_read_from_the_row_not_the_caller(self):
        self.assertEqual(org_slug_of(self.kenya_tree["collection"]), "kenya")
        self.assertEqual(org_slug_of(self.uganda_tree["item"].collection), "uganda")

    def test_a_titiler_url_opens_with_its_own_organisation(self):
        url = titiler_preview_url(self.kenya_tree["item"], SHARED_SLUG)
        self.assertTrue(
            url.startswith(f"/titiler/kenya/{SHARED_SLUG}/{SHARED_SLUG}/{SHARED_SLUG}/"),
            url,
        )
        self.assertIn("time=2026-03-01T12", url)

    def test_two_organisations_sharing_a_slug_get_different_titiler_urls(self):
        self.assertNotEqual(
            titiler_preview_url(self.kenya_tree["item"], SHARED_SLUG),
            titiler_preview_url(self.uganda_tree["item"], SHARED_SLUG),
        )

    def test_a_forecast_preview_carries_its_reference_time(self):
        item = Item.objects.create(
            collection=self.kenya_tree["collection"],
            time=datetime(2026, 3, 2, tzinfo=timezone.utc),
            reference_time=datetime(2026, 3, 1, tzinfo=timezone.utc),
        )
        url = titiler_preview_url(item, SHARED_SLUG)
        self.assertIn("reftime=2026-03-01T00", url)

    def test_the_asset_preview_url_is_the_same_address(self):
        """One grammar, not two: the model property delegates rather than repeats."""
        self.assertEqual(
            self.kenya_tree["asset"].preview_url,
            titiler_preview_url(self.kenya_tree["item"], SHARED_SLUG),
        )

    def test_the_template_tag_ignores_slugs_it_is_not_given(self):
        """The tag reads catalog and org from the item, so a page cannot mix them."""
        from django.template import Context, Template

        rendered = Template(
            "{% load georiva_tags %}{% titiler_preview_url item slug %}"
        ).render(Context({"item": self.uganda_tree["item"], "slug": SHARED_SLUG}))
        self.assertTrue(rendered.startswith("/titiler/uganda/"), rendered)

    def test_a_martin_url_pins_the_tile_to_one_organisations_rows(self):
        url = martin_boundary_stats_url(self.uganda_tree["collection"], MARTIN_PREFIX)
        self.assertIn("/martin/boundary_stats/{z}/{x}/{y}?", url)
        self.assertIn("org=uganda", url)
        self.assertIn(f"catalog={SHARED_SLUG}", url)
        self.assertIn(f"collection={SHARED_SLUG}", url)

    def test_a_martin_url_can_be_absolute_without_losing_the_triple(self):
        url = martin_boundary_stats_url(
            self.kenya_tree["collection"], base="https://kenya.example/martin/",
        )
        self.assertTrue(url.startswith("https://kenya.example/martin/boundary_stats/"), url)
        self.assertIn("org=kenya", url)

    def test_palette_cache_keys_of_colliding_catalogs_differ(self):
        keys = {
            get_palette_cache_key(org, SHARED_SLUG, SHARED_SLUG, SHARED_SLUG)
            for org in ("kenya", "uganda")
        }
        self.assertEqual(len(keys), 2, keys)
        self.assertIn("georiva:palette:kenya:forecast:forecast:forecast", keys)

    def test_a_variables_key_is_derived_from_its_own_row(self):
        self.assertEqual(
            variable_cache_key(self.uganda_tree["variable"]),
            "georiva:palette:uganda:forecast:forecast:forecast",
        )


class PaletteCacheSweepTests(TestCase):
    """Warming leaves the cache a mirror of the database, not a history of it.

    Titiler reads these keys straight out of Redis and nothing expires them, so
    a key whose variable was renamed, deactivated or deleted would otherwise
    outlive it — indefinitely, and invisibly, because a stale palette renders
    perfectly well. It just renders the wrong thing.
    """

    @classmethod
    def setUpTestData(cls):
        cls.organisation = make_organisation("kenya")
        cls.tree = build_tree(cls.organisation, variable_name="Kenya Forecast")

    def setUp(self):
        from django_redis import get_redis_connection

        self.redis = get_redis_connection("default")
        self.addCleanup(self._clear)
        self._clear()

    def _clear(self):
        keys = list(self.redis.scan_iter(match="georiva:palette:*"))
        if keys:
            self.redis.delete(*keys)

    def _keys(self):
        return {
            key.decode() if isinstance(key, bytes) else key
            for key in self.redis.scan_iter(match="georiva:palette:*")
        }

    def test_warming_writes_the_active_variables_key(self):
        warm_all()
        self.assertIn(variable_cache_key(self.tree["variable"]), self._keys())

    def test_a_key_left_by_the_pre_org_format_is_swept(self):
        """The migration case: keys written before the org segment existed."""
        legacy = f"georiva:palette:{SHARED_SLUG}:{SHARED_SLUG}:{SHARED_SLUG}"
        self.redis.set(legacy, "{}")
        warm_all()
        self.assertNotIn(legacy, self._keys())

    def test_a_deactivated_variables_key_does_not_outlive_it(self):
        warm_all()
        key = variable_cache_key(self.tree["variable"])
        self.assertIn(key, self._keys())

        Variable.objects.filter(pk=self.tree["variable"].pk).update(is_active=False)
        warm_all()
        self.assertNotIn(key, self._keys())

    def test_the_sweep_leaves_other_cache_namespaces_alone(self):
        self.redis.set("georiva:something-else", "{}")
        self.addCleanup(self.redis.delete, "georiva:something-else")
        warm_all()
        self.assertTrue(self.redis.exists("georiva:something-else"))


@override_settings(ALLOWED_HOSTS=["*"])
class TileConfigOrgSegmentTests(TestCase):
    """The one lookup that resolves its organisation from the path, not the Host.

    Titiler dials it server-to-server on an internal container name. That host
    belongs to no organisation, so the endpoint is exempt from host resolution —
    which makes its own filtering the only thing standing between a palette
    request and the wrong tenant's rendering config.
    """

    @classmethod
    def setUpTestData(cls):
        cls.kenya = make_organisation("kenya")
        cls.uganda = make_organisation("uganda")
        build_tree(cls.kenya, variable_name="Kenya Forecast")
        build_tree(cls.uganda, variable_name="Uganda Forecast")

    def _url(self, org_slug):
        return reverse(
            "tile_config", args=[org_slug, SHARED_SLUG, SHARED_SLUG, SHARED_SLUG],
        )

    def test_it_answers_on_a_hostname_belonging_to_no_organisation(self):
        """Titiler's internal address. Host resolution would 404 this outright."""
        response = self.client.get(self._url("kenya"), HTTP_HOST="georiva:8000")
        self.assertEqual(response.status_code, 200)

    def test_each_organisations_config_comes_back_under_its_own_segment(self):
        for org_slug, expected in (("kenya", 10.0), ("uganda", 20.0)):
            Variable.objects.filter(
                collection__catalog__organisation__slug=org_slug,
            ).update(value_max=expected)

        for org_slug, expected in (("kenya", 10.0), ("uganda", 20.0)):
            with self.subTest(org=org_slug):
                response = self.client.get(self._url(org_slug), HTTP_HOST="georiva:8000")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["vmax"], expected)

    def test_an_unknown_organisation_is_not_found(self):
        response = self.client.get(self._url("nowhere"), HTTP_HOST="georiva:8000")
        self.assertEqual(response.status_code, 404)

    def test_a_catalog_under_the_wrong_organisation_is_not_found(self):
        """Both halves must agree — a real org and a real catalog is not enough."""
        lonely = make_organisation("lonely")
        self.assertEqual(
            self.client.get(self._url(lonely.slug), HTTP_HOST="georiva:8000").status_code,
            404,
        )

    # -- the public half ---------------------------------------------------
    #
    # `/api/` is publicly proxied, so this URL is reachable by anyone. The path
    # segment is trusted only where the reason for trusting it holds — no
    # organisation answers the host. On a tenant's host the Host wins.

    def test_a_portals_own_host_still_reads_its_own_config(self):
        response = self.client.get(self._url("kenya"), HTTP_HOST=self.kenya.hostname)
        self.assertEqual(response.status_code, 200)

    def test_one_hosts_caller_cannot_read_another_organisations_config(self):
        """The disclosure this endpoint would otherwise reopen (#278)."""
        response = self.client.get(self._url("uganda"), HTTP_HOST=self.kenya.hostname)
        self.assertEqual(response.status_code, 404)

    def test_the_answer_does_not_reveal_whether_the_other_org_exists(self):
        """A stranger's real org and an invented one must look identical."""
        real = self.client.get(self._url("uganda"), HTTP_HOST=self.kenya.hostname)
        invented = self.client.get(self._url("nowhere"), HTTP_HOST=self.kenya.hostname)
        self.assertEqual(real.status_code, invented.status_code)
        self.assertEqual(real.content, invented.content)


class CogKeyGrammarTests(TestCase):
    """The one place the storage grammar is restated outside Django.

    Titiler has no ORM and rebuilds the COG key from path segments
    (``titiler-app/app/dependencies.build_cog_url``). That reconstruction is a
    copy of ``Catalog.storage_prefix``'s grammar living in another process,
    where no import or type can keep the two in step — so a change to one and
    not the other yields tiles that 404 against storage, silently and only in
    production. This asserts the copy still matches the original.
    """

    @classmethod
    def setUpTestData(cls):
        cls.tree = build_tree(make_organisation("kenya"), variable_name="Kenya Forecast")

    @staticmethod
    def _titiler_cog_path(org, catalog, collection, variable, time_dt):
        """``build_cog_url``'s path half, transcribed from the Titiler service."""
        return (
            f"{org}/{catalog}/{collection}/{variable}"
            f"/{time_dt.strftime('%Y/%m/%d')}/{variable}_{time_dt.strftime('%H%M%S')}.tif"
        )

    def test_titilers_key_matches_the_prefix_django_writes_under(self):
        catalog = self.tree["catalog"]
        key = self._titiler_cog_path(
            "kenya", catalog.slug, SHARED_SLUG, SHARED_SLUG,
            datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc),
        )
        self.assertTrue(
            key.startswith(catalog.storage_prefix.rstrip("/") + "/"),
            f"{key} is not under {catalog.storage_prefix}",
        )

    def test_the_titiler_route_and_the_storage_key_share_their_leading_segments(self):
        """A tile URL and the object behind it differ only by prefix, by design."""
        route = titiler_variable_root("kenya", SHARED_SLUG, SHARED_SLUG, SHARED_SLUG)
        key = self._titiler_cog_path(
            "kenya", SHARED_SLUG, SHARED_SLUG, SHARED_SLUG,
            datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(route, f"/titiler/{'/'.join(key.split('/')[:4])}")
