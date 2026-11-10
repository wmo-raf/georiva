"""
Import GeoNames ``cities1000`` into the instance-wide town gazetteer.

Towns are upserted on their GeoNames id, so a re-run picks up GeoNames'
corrections without duplicating anything. A town of an imported country that
the dump no longer lists is **deactivated, not deleted**: whatever refers to it
still resolves, and it comes back if GeoNames lists it again. Organisations'
own towns are never touched.

The dump is downloaded unless ``--file`` points at one already on disk (the
``.zip`` as GeoNames ships it, or the ``.txt`` inside), for instances with no
route to the internet.

Examples::

    georiva import_towns KE
    georiva import_towns KE UG TZ --file /data/cities1000.zip
"""

import csv
import io
import tempfile
import urllib.request
import zipfile
from collections.abc import Iterable, Iterator
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from georiva.core.models import Town

GEONAMES_URL = "https://download.geonames.org/export/dump/cities1000.zip"

#: Columns of a GeoNames dump line (https://download.geonames.org/export/dump/readme.txt).
GEONAMEID, NAME, ASCIINAME, LATITUDE, LONGITUDE, FEATURE_CLASS, FEATURE_CODE, COUNTRY, ADMIN1, POPULATION = (
    0,
    1,
    2,
    4,
    5,
    6,
    7,
    8,
    10,
    14,
)
COLUMN_COUNT = 19

#: The fields an import owns; anything else on a row is left as it is.
IMPORTED_FIELDS = [
    "name",
    "ascii_name",
    "country_code",
    "admin1_code",
    "feature_code",
    "latitude",
    "longitude",
    "population",
    "rank",
    "is_active",
]


def read_lines(path: Path) -> Iterator[str]:
    """The text lines of a dump, from the ``.zip`` GeoNames ships or a plain ``.txt``."""
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            members = [name for name in archive.namelist() if name.endswith(".txt")]
            if not members:
                raise CommandError(f"{path} holds no .txt dump")
            with archive.open(members[0]) as raw:
                yield from io.TextIOWrapper(raw, encoding="utf-8")
    else:
        with open(path, encoding="utf-8") as handle:
            yield from handle


def parse(lines: Iterable[str], countries: set[str]) -> Iterator[Town]:
    """Unsaved instance-wide towns for the populated places of ``countries``."""
    for row in csv.reader(lines, delimiter="\t", quoting=csv.QUOTE_NONE):
        if len(row) < COLUMN_COUNT or row[FEATURE_CLASS] != "P" or row[COUNTRY] not in countries:
            continue
        feature_code = row[FEATURE_CODE]
        yield Town(
            organisation=None,
            geonames_id=int(row[GEONAMEID]),
            name=row[NAME],
            ascii_name=row[ASCIINAME],
            country_code=row[COUNTRY],
            admin1_code=row[ADMIN1],
            feature_code=feature_code,
            latitude=float(row[LATITUDE]),
            longitude=float(row[LONGITUDE]),
            population=int(row[POPULATION] or 0),
            rank=Town.rank_for(feature_code),
            is_active=True,
        )


class Command(BaseCommand):
    help = "Import GeoNames cities1000 towns for the given countries into the instance-wide gazetteer."

    def add_arguments(self, parser):
        parser.add_argument("countries", nargs="+", metavar="ISO", help="ISO 3166-1 alpha-2 codes, e.g. KE UG.")
        parser.add_argument(
            "--file",
            metavar="PATH",
            help="A cities1000 dump on disk (.zip or .txt) instead of downloading it.",
        )

    def handle(self, *args, **options):
        countries = {code.upper() for code in options["countries"]}
        if any(len(code) != 2 or not code.isalpha() for code in countries):
            raise CommandError("Countries are ISO 3166-1 alpha-2 codes, e.g. KE")

        if options["file"]:
            towns = list(parse(read_lines(Path(options["file"])), countries))
        else:
            with tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "cities1000.zip"
                self.stdout.write(f"Downloading {GEONAMES_URL}")
                urllib.request.urlretrieve(GEONAMES_URL, path)  # noqa: S310 - fixed https URL
                towns = list(parse(read_lines(path), countries))

        created, updated, deactivated = self._upsert(towns, countries)
        self.stdout.write(
            self.style.SUCCESS(
                f"{', '.join(sorted(countries))}: {created} created, {updated} updated, {deactivated} deactivated"
            )
        )

    @transaction.atomic
    def _upsert(self, towns: list[Town], countries: set[str]) -> tuple[int, int, int]:
        # Only rows GeoNames put there: a town the instance admin added by hand
        # has no id, so the dump can neither update nor deactivate it.
        baseline = Town.objects.filter(organisation__isnull=True, country_code__in=countries, geonames_id__isnull=False)
        existing = dict(baseline.values_list("geonames_id", "pk"))

        to_create, to_update = [], []
        for town in towns:
            pk = existing.get(town.geonames_id)
            if pk is None:
                to_create.append(town)
            else:
                town.pk = pk
                to_update.append(town)

        Town.objects.bulk_create(to_create, batch_size=1000)
        Town.objects.bulk_update(to_update, IMPORTED_FIELDS, batch_size=1000)

        seen = {town.geonames_id for town in towns}
        deactivated = baseline.filter(is_active=True).exclude(geonames_id__in=seen).update(is_active=False)
        return len(to_create), len(to_update), deactivated
