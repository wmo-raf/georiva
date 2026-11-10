"""``import_towns``: GeoNames ``cities1000`` into the instance-wide gazetteer.

Tested at the command, on a small fixture in GeoNames' own tab-separated
layout, by the rows it leaves behind: which countries come in, the rank each
town derives, that a re-run updates rather than duplicates, and that a town
GeoNames stops listing is deactivated rather than deleted.
"""

import io
import tempfile
import zipfile
from pathlib import Path

from django.core.management import call_command
from django.test import TestCase

from georiva.core.models import Town
from georiva.organisations.provisioning import provision_organisation


def geonames_row(geonameid, name, lat, lon, feature_code, country, population, *, admin1="05", ascii_name=None):
    """One line of a GeoNames dump: 19 tab-separated columns."""
    columns = [
        str(geonameid),
        name,
        ascii_name or name,
        "",  # alternatenames
        str(lat),
        str(lon),
        "P",
        feature_code,
        country,
        "",  # cc2
        admin1,
        "",
        "",
        "",
        str(population),
        "",
        "1661",
        "Africa/Nairobi",
        "2024-01-01",
    ]
    return "\t".join(columns)


NAIROBI = geonames_row(184745, "Nairobi", -1.28333, 36.81667, "PPLC", "KE", 2750547)
KISUMU = geonames_row(191245, "Kisumu", -0.10221, 34.76171, "PPLA", "KE", 216479)
MALINDI = geonames_row(187968, "Malindi", -3.21799, 40.11692, "PPLA2", "KE", 207253)
ELDAMA = geonames_row(198629, "Eldama Ravine", 0.05, 35.71667, "PPL", "KE", 17581)
KAMPALA = geonames_row(232422, "Kampala", 0.31628, 32.58219, "PPLC", "UG", 1353189)


class ImportTownsTests(TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _dump(self, *rows, name="cities1000.txt"):
        path = Path(self.tmp.name) / name
        path.write_text("\n".join(rows) + "\n", encoding="utf-8")
        return path

    def _import(self, path, *countries):
        out = io.StringIO()
        call_command("import_towns", *countries, file=str(path), stdout=out)
        return out.getvalue()

    def test_only_the_named_countries_come_in(self):
        self._import(self._dump(NAIROBI, KISUMU, KAMPALA), "KE")

        self.assertEqual(
            sorted(Town.objects.values_list("name", flat=True)),
            ["Kisumu", "Nairobi"],
        )

    def test_imported_towns_are_instance_wide_and_active(self):
        self._import(self._dump(NAIROBI), "KE")

        town = Town.objects.get(geonames_id=184745)
        self.assertIsNone(town.organisation)
        self.assertTrue(town.is_active)
        self.assertEqual(town.country_code, "KE")
        self.assertAlmostEqual(town.latitude, -1.28333)
        self.assertAlmostEqual(town.longitude, 36.81667)
        self.assertEqual(town.population, 2750547)

    def test_rank_follows_the_feature_code(self):
        self._import(self._dump(NAIROBI, KISUMU, MALINDI, ELDAMA), "KE")

        ranks = dict(Town.objects.values_list("name", "rank"))
        self.assertEqual(
            ranks,
            {
                "Nairobi": Town.Rank.CAPITAL,
                "Kisumu": Town.Rank.ADMIN1_SEAT,
                "Malindi": Town.Rank.ADMIN2_SEAT,
                "Eldama Ravine": Town.Rank.TOWN,
            },
        )

    def test_a_rerun_updates_rather_than_duplicates(self):
        self._import(self._dump(NAIROBI), "KE")
        renamed = geonames_row(184745, "Nairobi City", -1.28333, 36.81667, "PPLC", "KE", 4397073)
        self._import(self._dump(renamed), "KE")

        towns = Town.objects.filter(geonames_id=184745)
        self.assertEqual(towns.count(), 1)
        self.assertEqual(towns.get().name, "Nairobi City")
        self.assertEqual(towns.get().population, 4397073)

    def test_a_town_dropped_by_geonames_is_deactivated_not_deleted(self):
        self._import(self._dump(NAIROBI, ELDAMA), "KE")
        self._import(self._dump(NAIROBI), "KE")

        eldama = Town.objects.get(geonames_id=198629)
        self.assertFalse(eldama.is_active)

    def test_a_town_that_returns_is_reactivated(self):
        self._import(self._dump(NAIROBI, ELDAMA), "KE")
        self._import(self._dump(NAIROBI), "KE")
        self._import(self._dump(NAIROBI, ELDAMA), "KE")

        self.assertTrue(Town.objects.get(geonames_id=198629).is_active)

    def test_deactivation_touches_only_the_countries_imported(self):
        self._import(self._dump(NAIROBI, KAMPALA), "KE", "UG")
        self._import(self._dump(NAIROBI), "KE")

        self.assertTrue(Town.objects.get(geonames_id=232422).is_active)

    def test_an_organisations_own_towns_are_left_alone(self):
        org = provision_organisation(name="Kenya Met", slug="kenya")
        own = Town.objects.create(
            organisation=org,
            name="Dagoretti",
            country_code="KE",
            latitude=-1.3,
            longitude=36.75,
        )
        self._import(self._dump(NAIROBI), "KE")

        own.refresh_from_db()
        self.assertTrue(own.is_active)
        self.assertEqual(Town.objects.filter(organisation__isnull=True).count(), 1)

    def test_a_zipped_dump_is_read_as_well(self):
        archive = Path(self.tmp.name) / "cities1000.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("cities1000.txt", NAIROBI + "\n")
        self._import(archive, "KE")

        self.assertTrue(Town.objects.filter(geonames_id=184745).exists())

    def test_country_codes_are_case_insensitive(self):
        self._import(self._dump(NAIROBI), "ke")

        self.assertTrue(Town.objects.filter(geonames_id=184745).exists())

    def test_an_instance_wide_town_added_by_hand_is_left_alone(self):
        hand_added = Town.objects.create(name="Dagoretti", country_code="KE", latitude=-1.3, longitude=36.75)
        self._import(self._dump(NAIROBI), "KE")

        hand_added.refresh_from_db()
        self.assertTrue(hand_added.is_active)
