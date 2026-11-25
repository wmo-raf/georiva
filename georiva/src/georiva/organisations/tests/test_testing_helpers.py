from django.contrib.auth import get_user_model
from django.test import Client, TestCase

from georiva.organisations.testing import dial_org, join_org, make_organisation


class OrgHelpersTakeSlugsTests(TestCase):
    """``join_org`` / ``dial_org`` take a slug; an Organisation is refused (#815).

    ``str(org)`` is its name, not its slug, so passing the object built a host
    no Site answers on, and the test 404'd only where name and slug differ.
    """

    def test_join_org_refuses_an_organisation(self):
        org = make_organisation("helpers-org", name="Not The Slug")
        user = get_user_model().objects.create_user("u")
        with self.assertRaisesMessage(TypeError, "pass org.slug"):
            join_org(user, org)

    def test_dial_org_refuses_an_organisation(self):
        org = make_organisation("helpers-org", name="Not The Slug")
        with self.assertRaisesMessage(TypeError, "pass org.slug"):
            dial_org(Client(), org)

    def test_a_slug_dials_its_host(self):
        client = dial_org(Client(), "helpers-org")
        self.assertEqual(client.defaults["HTTP_HOST"], "helpers-org.testserver")
