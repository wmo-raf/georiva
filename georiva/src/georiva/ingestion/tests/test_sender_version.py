from django.test import SimpleTestCase

from georiva.ingestion import sender_version
from georiva.ingestion.sender import georiva_push


class UserAgentTests(SimpleTestCase):
    def test_the_stdlib_transport_names_itself(self):
        self.assertEqual(sender_version.from_user_agent("georiva-push/1.1.0"), "1.1.0")

    def test_the_boto3_transport_appends_it_to_botocores(self):
        ua = "Boto3/1.35.0 md/Botocore#1.35.0 ua/2.0 os/linux#6.1 lang/python#3.11.2 georiva-push/1.0.0"
        self.assertEqual(sender_version.from_user_agent(ua), "1.0.0")

    def test_other_clients_carry_no_version(self):
        for ua in ("rclone/v1.68.1", "Boto3/1.35.0 md/Botocore#1.35.0", "MinIO (linux; amd64) minio-go/v7", "", None):
            with self.subTest(ua=ua):
                self.assertEqual(sender_version.from_user_agent(ua), "")

    def test_a_lookalike_product_is_not_taken_for_it(self):
        self.assertEqual(sender_version.from_user_agent("not-georiva-push/9.9.9"), "")

    def test_a_version_too_long_to_record_is_not_taken(self):
        for ua in ("georiva-push/1.2.3.4.5", "georiva-push/1234567.0.0", "georiva-push/" + "9" * 40):
            with self.subTest(ua=ua):
                self.assertEqual(sender_version.from_user_agent(ua), "")
        self.assertLessEqual(len(sender_version.from_user_agent("georiva-push/999999.999999.999999.999999")), 32)

    def test_the_script_says_what_it_sends(self):
        self.assertEqual(sender_version.from_user_agent(georiva_push.USER_AGENT), georiva_push.__version__)


class OutdatedTests(SimpleTestCase):
    def test_older_than_served_is_outdated(self):
        self.assertTrue(sender_version.is_outdated("1.0.0", current="1.1.0"))
        self.assertTrue(sender_version.is_outdated("1.1", current="1.1.1"))

    def test_the_same_or_newer_is_not(self):
        self.assertFalse(sender_version.is_outdated("1.1.0", current="1.1.0"))
        self.assertFalse(sender_version.is_outdated("1.1", current="1.1.0"))
        self.assertFalse(sender_version.is_outdated("1.2.0", current="1.1.0"))

    def test_versions_compare_as_numbers_not_text(self):
        self.assertFalse(sender_version.is_outdated("1.10.0", current="1.9.0"))
        self.assertTrue(sender_version.is_outdated("1.9.0", current="1.10.0"))

    def test_a_pre_release_served_version_compares_by_its_release(self):
        self.assertTrue(sender_version.is_outdated("1.1.0", current="1.2.0rc1"))
        self.assertFalse(sender_version.is_outdated("1.2.0", current="1.2.0.dev0"))

    def test_no_version_is_never_outdated(self):
        self.assertFalse(sender_version.is_outdated("", current="1.1.0"))

    def test_served_defaults_to_the_script_this_instance_serves(self):
        self.assertEqual(sender_version.served(), georiva_push.__version__)
        self.assertTrue(sender_version.is_outdated("0.1.0"))
