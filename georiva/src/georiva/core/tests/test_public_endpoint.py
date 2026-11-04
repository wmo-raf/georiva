"""Where a browser's upload goes, and whether it can get there."""

from django.test import SimpleTestCase, override_settings

from georiva.core.checks import check_public_endpoint_name
from georiva.core.storage.public_endpoint import (
    MIXED_CONTENT,
    NO_S3_PREFIX,
    SAME_HOST,
    describe,
    has_s3_shape,
)


class DescribeTests(SimpleTestCase):
    def test_the_standard_endpoint_is_clean(self):
        got = describe("s3.kmsa.example.org", False, site_host="kmsa.example.org", site_is_secure=False)

        self.assertIsNone(got.blocked)
        self.assertIsNone(got.advisory)
        self.assertEqual(got.url, "http://s3.kmsa.example.org")

    def test_the_site_itself_can_never_be_the_endpoint(self):
        """The case that sent a file to the app and reported it as uploaded."""
        got = describe("kmsa.example.org", False, site_host="kmsa.example.org", site_is_secure=False)

        self.assertEqual(got.blocked, SAME_HOST)

    def test_the_site_on_its_default_port_is_still_the_site(self):
        got = describe("kmsa.example.org:443", True, site_host="kmsa.example.org", site_is_secure=True)

        self.assertEqual(got.blocked, SAME_HOST)

    def test_neither_side_naming_a_port_is_the_same_host_whatever_the_scheme(self):
        """A site over HTTPS with an endpoint left on HTTP is two faults; the host is the first."""
        got = describe("kmsa.example.org", False, site_host="kmsa.example.org", site_is_secure=True)

        self.assertEqual(got.blocked, SAME_HOST)

    def test_dev_ports_keep_localhost_apart_from_itself(self):
        """MinIO on :9000 beside the app on :8000 is the normal dev layout."""
        got = describe("localhost:9000", False, site_host="localhost:8000", site_is_secure=False)

        self.assertIsNone(got.blocked)
        self.assertIsNone(got.advisory)

    def test_an_https_page_cannot_send_to_an_http_endpoint(self):
        got = describe("s3.kmsa.example.org", False, site_host="kmsa.example.org", site_is_secure=True)

        self.assertEqual(got.blocked, MIXED_CONTENT)

    def test_an_http_page_may_send_to_an_https_endpoint(self):
        got = describe("s3.kmsa.example.org", True, site_host="kmsa.example.org", site_is_secure=False)

        self.assertIsNone(got.blocked)

    def test_a_name_without_s3_is_advisory_and_never_blocks(self):
        got = describe("storage.kmsa.example.org", False, site_host="kmsa.example.org", site_is_secure=False)

        self.assertIsNone(got.blocked)
        self.assertEqual(got.advisory, NO_S3_PREFIX)

    def test_without_a_page_only_the_naming_can_be_judged(self):
        self.assertIsNone(describe("kmsa.example.org", False).blocked)
        self.assertEqual(describe("kmsa.example.org", False).advisory, NO_S3_PREFIX)


class S3ShapeTests(SimpleTestCase):
    def test_s3_label(self):
        self.assertTrue(has_s3_shape("s3.example.org"))
        self.assertTrue(has_s3_shape("S3.Example.org:8080"))

    def test_development_hosts_are_exempt(self):
        for host in ("localhost:9000", "127.0.0.1:9000", "minio.localhost", "[::1]:9000"):
            self.assertTrue(has_s3_shape(host), host)

    def test_other_names_are_not(self):
        for host in ("example.org", "storage.example.org", "s3example.org", "ss3.example.org"):
            self.assertFalse(has_s3_shape(host), host)


class StartupCheckTests(SimpleTestCase):
    @override_settings(GEORIVA_STORAGE_BACKEND="s3", MINIO_PUBLIC_ENDPOINT="kmsa.example.org")
    def test_a_name_without_s3_is_a_warning_that_names_the_setting(self):
        (message,) = check_public_endpoint_name(None)

        self.assertEqual(message.id, "georiva.W003")
        self.assertIn("MINIO_PUBLIC_ENDPOINT", message.msg)
        self.assertIn("kmsa.example.org", message.msg)

    @override_settings(GEORIVA_STORAGE_BACKEND="s3", MINIO_PUBLIC_ENDPOINT="s3.kmsa.example.org")
    def test_the_standard_name_is_silent(self):
        self.assertEqual(check_public_endpoint_name(None), [])

    @override_settings(GEORIVA_STORAGE_BACKEND="s3", MINIO_PUBLIC_ENDPOINT="localhost:9000")
    def test_a_development_host_is_silent(self):
        self.assertEqual(check_public_endpoint_name(None), [])

    @override_settings(GEORIVA_STORAGE_BACKEND="local", MINIO_PUBLIC_ENDPOINT="kmsa.example.org")
    def test_local_storage_has_no_endpoint_to_judge(self):
        self.assertEqual(check_public_endpoint_name(None), [])
