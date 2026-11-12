"""A collection with Remote assets is a read-plane peer (ADR 0043, #638).

Each suite here harvests a faked Upstream and then asks one serving surface
about the result, at the HTTP seam. The rows are real harvested rows; nothing
about the read plane is faked except, where bytes are read, the bytes.
"""

from xml.etree import ElementTree as ET

from django.test import TestCase, override_settings
from django.urls import reverse

from georiva.core.machine_plane.palette_cache import warm_all
from georiva.core.machine_plane.remote_assets import PUBLISHED_FIELD, REMOTE_ASSETS_KEY_PREFIX, variable_key
from georiva.core.models import Asset, Collection
from georiva.organisations.testing import dial_org, org_host
from georiva.organisations.tests.factories import PASSWORD, add_member, make_user
from georiva.wmts.testing import NS, IsolatedCapabilitiesCache

from .support import RemoteCollectionMixin, day, nothing_written, rain_href, stamp


@override_settings(ALLOWED_HOSTS=["*"])
class TileConfigHandsTitilerTheUpstreamHrefTests(RemoteCollectionMixin, TestCase):
    def test_a_remote_timestep_answers_with_its_upstream_href(self):
        response = self.tile_config(f"?time={stamp(1)}")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["remote_href"], rain_href(1))

    def test_it_renders_from_the_variables_bounds_when_the_asset_has_no_statistics(self):
        asset = Asset.objects.get(item__time=day(1))
        self.assertIsNone(asset.stats_min)

        payload = self.tile_config(f"?time={stamp(1)}").json()

        self.assertEqual((payload["vmin"], payload["vmax"]), (0.0, 100.0))

    def test_a_request_naming_no_timestep_is_answered_as_before(self):
        response = self.tile_config()

        self.assertEqual(response.status_code, 200)
        self.assertNotIn("remote_href", response.json())

    def test_a_local_timestep_of_a_mixed_collection_carries_no_href(self):
        self.add_local_day(3)

        self.assertNotIn("remote_href", self.tile_config(f"?time={stamp(3)}").json())
        self.assertEqual(self.tile_config(f"?time={stamp(2)}").json()["remote_href"], rain_href(2))

    def test_a_timestep_with_no_asset_carries_no_href(self):
        response = self.tile_config(f"?time={stamp(9)}")

        self.assertEqual(response.status_code, 200)
        self.assertNotIn("remote_href", response.json())

    def test_a_timestep_that_is_not_a_datetime_is_a_bad_request(self):
        self.assertEqual(self.tile_config("?time=yesterday").status_code, 400)

    def test_a_private_collection_is_still_not_answered_for(self):
        self.set_visibility(Collection.Visibility.PRIVATE)

        self.assertEqual(self.tile_config(f"?time={stamp(1)}").status_code, 404)

    def test_answering_writes_nothing_and_computes_nothing(self):
        with nothing_written(self):
            self.tile_config(f"?time={stamp(1)}")
            self.tile_config()


class RemoteHrefsArePublishedForTitilerTests(RemoteCollectionMixin, TestCase):
    """Titiler reads these from Redis, for a collection of any tier."""

    def test_a_harvest_publishes_where_each_remote_timestep_is(self):
        self.assertEqual(
            self.published(),
            {"20260301T000000": rain_href(1), "20260302T000000": rain_href(2)},
        )

    def test_the_hash_says_it_is_django_s_whole_word(self):
        self.assertTrue(self.redis.hexists(variable_key(self.variable), PUBLISHED_FIELD))

    def test_a_private_collection_is_published_too(self):
        self.set_visibility(Collection.Visibility.PRIVATE)
        self._sweep_redis()

        warm_all()

        self.assertEqual(len(self.published()), 2)

    def test_a_local_asset_of_a_mixed_collection_is_not_published(self):
        self.add_local_day(3)

        self.assertNotIn("20260303T000000", self.published())

    def test_an_href_rewritten_upstream_is_republished_by_the_next_harvest(self):
        moved = rain_href(1).replace("chirps-v2.0", "chirps-v3.0")
        self.upstream.drop_item(day(1))
        self.upstream.add_item(day(1), {"rainfall": moved}, updated=day(20))

        self.harvest()

        self.assertEqual(self.published()["20260301T000000"], moved)

    def test_a_timestep_gone_upstream_is_withdrawn(self):
        self.upstream.drop_item(day(2))

        self.harvest()

        self.assertEqual(self.published(), {"20260301T000000": rain_href(1)})

    def test_a_materialized_asset_is_withdrawn(self):
        asset = Asset.objects.get(item__time=day(1))
        asset.href = f"{self.organisation.slug}/chirps/chirps-daily/rain/2026/03/01/rain_000000.tif"
        asset.save()

        self.assertEqual(self.published(), {"20260302T000000": rain_href(2)})

    def test_a_cold_redis_is_republished_whole_on_warm(self):
        self._sweep_redis()

        warm_all()

        self.assertEqual(len(self.published()), 2)
        self.assertTrue(self.redis.hexists(variable_key(self.variable), PUBLISHED_FIELD))

    def test_a_hash_no_variable_answers_to_is_swept_on_warm(self):
        orphan = f"{REMOTE_ASSETS_KEY_PREFIX}:kenya:gone:gone:gone"
        self.redis.hset(orphan, PUBLISHED_FIELD, "1")

        warm_all()

        self.assertFalse(self.redis.exists(orphan))


class RemoteCollectionInWMTSCapabilitiesTests(IsolatedCapabilitiesCache, RemoteCollectionMixin, TestCase):
    LAYER = "chirps:chirps-daily:rain"

    def layer(self):
        response = self.client.get(reverse("wmts:capabilities", args=[self.organisation.slug]))
        self.assertEqual(response.status_code, 200)
        layers = {
            layer.findtext("ows:Identifier", namespaces=NS): layer
            for layer in ET.fromstring(response.content).findall("wmts:Contents/wmts:Layer", NS)
        }
        self.assertIn(self.LAYER, layers)
        return layers[self.LAYER]

    def times(self):
        return [value.text for value in self.layer().findall("wmts:Dimension/wmts:Value", NS)]

    def test_the_collection_is_listed_with_its_remote_timesteps(self):
        self.assertEqual(self.times(), [stamp(1), stamp(2)])

    def test_a_mixed_collection_lists_local_and_remote_timesteps_alike(self):
        self.add_local_day(3)

        self.assertEqual(self.times(), [stamp(1), stamp(2), stamp(3)])

    def test_its_tiles_are_addressed_at_the_owning_organisation_like_any_other(self):
        template = self.layer().find("wmts:ResourceURL", NS).get("template")

        self.assertIn(f"/titiler/{self.organisation.slug}/chirps/chirps-daily/rain/", template)
        self.assertNotIn("upstream.example", template)

    def test_describing_it_writes_nothing_and_computes_nothing(self):
        with nothing_written(self):
            self.layer()


class RemoteCollectionInTheEDRMetadataPlaneTests(RemoteCollectionMixin, TestCase):
    def detail(self):
        response = self.client.get(reverse("edr:collection-detail", args=["chirps-daily"]))
        self.assertEqual(response.status_code, 200)
        return response.json()

    def test_the_collection_is_listed(self):
        response = self.client.get(reverse("edr:collection-list"))

        self.assertEqual(response.status_code, 200)
        self.assertIn("chirps-daily", {c["id"] for c in response.json()["collections"]})

    def test_it_is_described_with_its_variable_and_remote_timesteps(self):
        body = self.detail()

        self.assertIn("rain", body["parameter_names"])
        self.assertEqual(len(body["extent"]["temporal"]["values"]), 2)

    def test_a_mixed_collection_describes_local_and_remote_timesteps_alike(self):
        self.add_local_day(3)

        self.assertEqual(len(self.detail()["extent"]["temporal"]["values"]), 3)

    def test_describing_it_writes_nothing_and_computes_nothing(self):
        with nothing_written(self):
            self.client.get(reverse("edr:collection-list"))
            self.detail()


class RemoteCollectionVisibilityTests(IsolatedCapabilitiesCache, RemoteCollectionMixin, TestCase):
    """The tiers mean for a harvested collection what they mean for any other."""

    def gate(self, **extra):
        uri = (
            f"/titiler/{self.organisation.slug}/chirps/chirps-daily/rain"
            f"/tiles/WebMercatorQuad/6/38/32.png?time={stamp(1)}"
        )
        return self.client.get(reverse("tile_auth"), HTTP_X_ORIGINAL_URI=uri, **extra).status_code

    def sign_in_member(self):
        member = make_user("member")
        add_member(member, self.organisation)
        self.client.login(username="member", password=PASSWORD)

    def sign_in_member_of_another_organisation(self):
        from georiva.organisations.testing import make_organisation

        stranger = make_user("stranger")
        add_member(stranger, make_organisation("uganda"))
        self.client.login(username="stranger", password=PASSWORD)

    def test_a_public_collection_s_tiles_are_let_through_for_anyone(self):
        self.assertEqual(self.gate(), 204)

    def test_a_public_collection_s_tiles_are_let_through_for_another_organisation_s_member(self):
        self.sign_in_member_of_another_organisation()

        self.assertEqual(self.gate(), 204)

    def test_a_private_collection_s_tiles_are_refused_to_anyone_but_members(self):
        self.set_visibility(Collection.Visibility.PRIVATE)

        self.assertEqual(self.gate(), 403)
        self.sign_in_member_of_another_organisation()
        self.assertEqual(self.gate(), 403)

    def test_a_private_collection_s_tiles_are_let_through_for_a_member(self):
        self.set_visibility(Collection.Visibility.PRIVATE)
        self.sign_in_member()

        self.assertEqual(self.gate(), 204)

    def test_a_private_collection_is_in_wmts_capabilities_for_members_only(self):
        self.set_visibility(Collection.Visibility.PRIVATE)
        url = reverse("wmts:capabilities", args=[self.organisation.slug])

        self.assertNotIn(b"chirps:chirps-daily:rain", self.client.get(url).content)

        self.sign_in_member()
        self.assertIn(b"chirps:chirps-daily:rain", self.client.get(url).content)

    def test_the_gate_writes_nothing_and_computes_nothing(self):
        with nothing_written(self):
            self.gate()

    def test_a_private_collection_is_in_stac_and_edr_for_members_only(self):
        self.set_visibility(Collection.Visibility.PRIVATE)
        stac = reverse("stac:item-detail", args=["chirps", "chirps-daily", "rain", "20260301T000000Z"])
        edr = reverse("edr:collection-detail", args=["chirps-daily"])

        self.assertEqual([self.client.get(stac).status_code, self.client.get(edr).status_code], [404, 404])

        self.sign_in_member()
        self.assertEqual([self.client.get(stac).status_code, self.client.get(edr).status_code], [200, 200])

    def test_no_other_organisation_s_host_serves_it(self):
        dial_org(self.client, "uganda")

        self.assertEqual(self.client.get(reverse("edr:collection-detail", args=["chirps-daily"])).status_code, 404)
        self.assertEqual(self.gate(HTTP_HOST=org_host("uganda")), 403)
