"""Wind speed and direction from eastward (u) and northward (v) components.

Direction is the meteorological one, the bearing the wind blows *from*: 0 north,
clockwise. A wind blowing towards the south comes from 0°.
"""

import unittest

import numpy as np

from georiva.geoprocessing import wind_from_direction, wind_speed


def _components(*pairs):
    u = np.array([[u for u, _ in pairs]], dtype=np.float32)
    v = np.array([[v for _, v in pairs]], dtype=np.float32)
    return u, v


class WindSpeedTests(unittest.TestCase):
    def test_speed_is_the_vector_magnitude(self):
        u, v = _components((3.0, 4.0), (-3.0, -4.0), (0.0, -7.5))

        np.testing.assert_allclose(wind_speed(u, v), [[5.0, 5.0, 7.5]], atol=1e-6)


class WindFromDirectionTests(unittest.TestCase):
    def test_a_wind_from_each_compass_point(self):
        """From the north blows southwards: v negative. From the east blows westwards: u negative."""
        u, v = _components((0.0, -5.0), (-5.0, 0.0), (0.0, 5.0), (5.0, 0.0))

        np.testing.assert_allclose(wind_from_direction(u, v), [[0.0, 90.0, 180.0, 270.0]], atol=1e-4)

    def test_a_wind_from_each_quadrant(self):
        u, v = _components((-3.0, -3.0), (-3.0, 3.0), (3.0, 3.0), (3.0, -3.0))

        np.testing.assert_allclose(wind_from_direction(u, v), [[45.0, 135.0, 225.0, 315.0]], atol=1e-4)

    def test_direction_stays_below_a_full_circle(self):
        """A wind a hair west of north rounds into [0, 360), never to 360 itself."""
        u, v = _components((1e-12, -5.0), (1e-3, -5.0))

        direction = wind_from_direction(u, v)

        self.assertTrue(((direction >= 0.0) & (direction < 360.0)).all())

    def test_calm_is_given_180(self):
        u, v = _components((0.0, 0.0))

        self.assertEqual(wind_from_direction(u, v).tolist(), [[180.0]])

    def test_missing_components_give_a_missing_direction(self):
        u, v = _components((np.nan, 1.0))

        self.assertTrue(np.isnan(wind_from_direction(u, v)).all())
