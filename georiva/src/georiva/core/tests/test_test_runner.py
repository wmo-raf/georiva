"""What GeoRivaTestRunner sets up for the whole suite."""

from django.contrib.auth.hashers import get_hasher
from django.test import SimpleTestCase


class TestRunnerEnvironmentTests(SimpleTestCase):
    def test_passwords_are_hashed_with_a_fast_hasher(self):
        # PBKDF2 costs ~0.4s a hash; most admin tests hash twice (create_user, login).
        self.assertEqual(get_hasher().algorithm, "md5")
