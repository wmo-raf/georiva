"""What an API key is, and what presenting one gets you.

A key is a credential for a *person*, not for an organisation. It carries no
tenancy of its own: presenting one says who you are, and what that buys is then
decided by the same membership check a browser session goes through
(``organisations.access``). One identity path, two transports.

The storage contract is the other half. The secret exists in plaintext exactly
once — in the response to the request that created it — and what the database
keeps is a hash, so a dump of the table hands an attacker nothing they can
present.
"""

from datetime import timedelta

from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from georiva.accounts.models import KEY_PREFIX, ApiKey
from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
from georiva.organisations.provisioning import provision_organisation
from georiva.organisations.tests.factories import add_member, make_user


class ApiKeyModelTests(TestCase):
    """Minting, storage, and the three ways a key stops working."""

    @classmethod
    def setUpTestData(cls):
        cls.user = make_user("keyholder")

    def test_minting_returns_the_secret_once_and_stores_only_a_hash(self):
        key, secret = ApiKey.objects.mint(user=self.user, name="QGIS laptop")

        self.assertTrue(secret.startswith(KEY_PREFIX))
        self.assertNotIn(secret, [key.hashed_key, key.prefix])
        # Nothing on the row reproduces the secret: the column holds a digest,
        # and the prefix is a display handle short enough to be useless.
        row = ApiKey.objects.values().get(pk=key.pk)
        self.assertNotIn(secret, [str(value) for value in row.values()])

    def test_the_stored_prefix_identifies_the_key_without_revealing_it(self):
        key, secret = ApiKey.objects.mint(user=self.user, name="notebook")

        self.assertTrue(secret.startswith(key.prefix))
        self.assertLess(len(key.prefix), len(secret))

    def test_two_keys_never_collide(self):
        _, first = ApiKey.objects.mint(user=self.user, name="one")
        _, second = ApiKey.objects.mint(user=self.user, name="two")

        self.assertNotEqual(first, second)

    def test_a_live_key_resolves_to_its_user(self):
        key, secret = ApiKey.objects.mint(user=self.user, name="cron")

        self.assertEqual(ApiKey.objects.resolve(secret), key)

    def test_a_secret_that_was_never_issued_resolves_to_nothing(self):
        self.assertIsNone(ApiKey.objects.resolve(f"{KEY_PREFIX}not-a-real-key"))

    def test_a_revoked_key_resolves_to_nothing(self):
        key, secret = ApiKey.objects.mint(user=self.user, name="lost phone")
        key.revoke()

        self.assertIsNone(ApiKey.objects.resolve(secret))

    def test_an_expired_key_resolves_to_nothing(self):
        _, secret = ApiKey.objects.mint(
            user=self.user,
            name="short-lived",
            expires_at=timezone.now() - timedelta(seconds=1),
        )

        self.assertIsNone(ApiKey.objects.resolve(secret))

    def test_a_key_expiring_in_the_future_still_resolves(self):
        key, secret = ApiKey.objects.mint(
            user=self.user,
            name="this season",
            expires_at=timezone.now() + timedelta(days=30),
        )

        self.assertEqual(ApiKey.objects.resolve(secret), key)

    def test_a_key_belonging_to_a_deactivated_user_resolves_to_nothing(self):
        _, secret = ApiKey.objects.mint(user=self.user, name="ex-employee")
        self.user.is_active = False
        self.user.save()

        self.assertIsNone(ApiKey.objects.resolve(secret))

    def test_resolving_records_when_the_key_was_last_used(self):
        key, secret = ApiKey.objects.mint(user=self.user, name="cron")
        self.assertIsNone(key.last_used_at)

        ApiKey.objects.resolve(secret)

        key.refresh_from_db()
        self.assertIsNotNone(key.last_used_at)

    def test_last_used_is_not_rewritten_on_every_single_call(self):
        """A tile client makes hundreds of requests a minute; one write does."""
        key, secret = ApiKey.objects.mint(user=self.user, name="tiles")
        ApiKey.objects.resolve(secret)
        key.refresh_from_db()
        first = key.last_used_at

        ApiKey.objects.resolve(secret)

        key.refresh_from_db()
        self.assertEqual(key.last_used_at, first)

    def test_revoking_is_recorded_rather_than_deleted(self):
        key, _ = ApiKey.objects.mint(user=self.user, name="rotated")
        key.revoke()

        key.refresh_from_db()
        self.assertIsNotNone(key.revoked_at)
        self.assertFalse(key.is_live)

    def test_revoking_twice_keeps_the_first_timestamp(self):
        key, _ = ApiKey.objects.mint(user=self.user, name="rotated")
        key.revoke()
        first = key.revoked_at

        key.revoke()

        self.assertEqual(key.revoked_at, first)


PRIVATE_SLUG = "private-forecast"


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class ApiKeyAuthenticationTests(TestCase):
    """A key over the wire, on the plane it exists for.

    Both transports have to work and mean the same thing. The header is what a
    script sends; the query parameter is for the clients that cannot set one —
    QGIS and a web map take a tile URL and nothing else.
    """

    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")

        catalog = Catalog.objects.create(
            organisation=cls.kenya,
            name="Kenya Forecast",
            slug="forecast",
            file_format=Catalog.FileFormat.GEOTIFF,
        )
        collection = Collection.objects.create(
            catalog=catalog,
            name="Restricted",
            slug=PRIVATE_SLUG,
            visibility=Collection.Visibility.PRIVATE,
        )
        unit, _ = Unit.objects.get_or_create(name="Celsius", symbol="C")
        variable = Variable.objects.create(
            collection=collection,
            name="Restricted",
            slug=PRIVATE_SLUG,
            unit=unit,
            value_min=0,
            value_max=50,
        )
        item = Item.objects.create(collection=collection, time="2026-03-01T12:00:00Z")
        Asset.objects.create(item=item, variable=variable, href="restricted.tif")

        cls.member = make_user("kenya-member")
        add_member(cls.member, cls.kenya)
        cls.member_key = ApiKey.objects.mint(user=cls.member, name="laptop")[1]

        cls.outsider = make_user("uganda-member")
        add_member(cls.outsider, cls.uganda)
        cls.outsider_key = ApiKey.objects.mint(user=cls.outsider, name="laptop")[1]

    def setUp(self):
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    @property
    def url(self):
        return reverse("stac:collection-detail", args=["forecast", PRIVATE_SLUG, PRIVATE_SLUG])

    def test_without_a_key_the_private_collection_is_not_found(self):
        self.assertEqual(self.client.get(self.url).status_code, 404)

    def test_a_members_key_in_the_authorization_header_is_served(self):
        response = self.client.get(
            self.url,
            HTTP_AUTHORIZATION=f"Bearer {self.member_key}",
        )
        self.assertEqual(response.status_code, 200)

    def test_a_members_key_as_a_query_parameter_is_served(self):
        response = self.client.get(self.url, {"api_key": self.member_key})
        self.assertEqual(response.status_code, 200)

    def test_a_key_belonging_to_another_organisations_member_is_not_found(self):
        """The key authenticates; the membership check is what turns it away."""
        response = self.client.get(
            self.url,
            HTTP_AUTHORIZATION=f"Bearer {self.outsider_key}",
        )
        self.assertEqual(response.status_code, 404)

    def test_a_revoked_key_stops_working_immediately(self):
        ApiKey.objects.get(user=self.member).revoke()

        response = self.client.get(
            self.url,
            HTTP_AUTHORIZATION=f"Bearer {self.member_key}",
        )
        self.assertEqual(response.status_code, 401)

    def test_a_key_that_was_never_issued_is_rejected_as_a_credential(self):
        """Not 404: a broken credential is the caller's problem, not a missing row.

        It leaks nothing — the answer is the same whether or not the collection
        exists — and a scripting user with a typo in their key deserves to be
        told so rather than to debug a phantom 404.
        """
        response = self.client.get(
            self.url,
            HTTP_AUTHORIZATION=f"Bearer {KEY_PREFIX}nonsense",
        )
        self.assertEqual(response.status_code, 401)

    def test_a_bearer_token_that_is_not_ours_is_left_alone(self):
        """Another scheme's Bearer token is not this authenticator's business."""
        response = self.client.get(self.url, HTTP_AUTHORIZATION="Bearer eyJhbGciOi.x.y")
        self.assertEqual(response.status_code, 404)

    def test_a_key_still_only_reaches_the_organisations_it_belongs_to(self):
        """Keys carry no org of their own — the host decides which one is in play."""
        self.client.defaults["HTTP_HOST"] = "uganda.georiva.test"
        response = self.client.get(
            self.url,
            HTTP_AUTHORIZATION=f"Bearer {self.member_key}",
        )
        self.assertEqual(response.status_code, 404)

    def test_a_key_does_not_open_the_admin(self):
        """Admin is session territory; a key is a data credential (#273)."""
        response = self.client.get(
            "/admin/",
            HTTP_AUTHORIZATION=f"Bearer {self.member_key}",
        )
        self.assertNotEqual(response.status_code, 200)
