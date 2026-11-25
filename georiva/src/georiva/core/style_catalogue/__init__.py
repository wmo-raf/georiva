"""The shipped Style template catalogue (#789).

A catalogue is a vendored JSON file written by a dev-only generator
(``georiva/scripts/earthkit_styles/generate.py``, or
``georiva/scripts/magics_styles/generate.py`` for Magics, #811): the units its templates are
declared in, and the templates themselves. :func:`load_catalogue` puts one into
the database, and a data migration is what calls it. A refresh is a new JSON
file beside this module and a one-line migration that loads it.

Shipped templates are read-only to everyone (their ``origin`` says GeoRiva
shipped them), which is what makes it safe for a later catalogue to overwrite
them by slug.
"""

import json
from dataclasses import dataclass
from pathlib import Path

#: Where the vendored catalogue files live.
CATALOGUES = Path(__file__).resolve().parent

#: The current earthkit-plots catalogue (© ECMWF, Apache-2.0 — see NOTICE).
EARTHKIT_PLOTS = CATALOGUES / "earthkit-plots-1.0.4.json"

#: The current Magics catalogue, an additional source beside it (© ECMWF, Apache-2.0 — see NOTICE).
MAGICS = CATALOGUES / "magics-cb97794b.json"

_TEMPLATE_FIELDS = ("name", "quantity", "levels", "colors", "under", "over", "scale_type", "source_name")


@dataclass(frozen=True)
class Loaded:
    templates_created: int
    templates_updated: int
    units_created: int


def load_catalogue(path, apps=None) -> Loaded:
    """Upsert ``path``'s templates into the instance-wide tier, by slug.

    Every template gets ``origin`` = the catalogue's source name. An
    organisation's own template with the same slug is a different row (its own
    tier) and is not touched. A unit the catalogue declares that the instance
    lacks is created as shared reference data; one it has, by symbol, is used as
    it is. Running it twice changes nothing the second time.

    ``apps`` is a migration's historical app registry; by default, the live one.
    """
    if apps is None:
        from django.apps import apps
    Unit = apps.get_model("georivacore", "Unit")
    StyleTemplate = apps.get_model("georivacore", "StyleTemplate")

    catalogue = json.loads(Path(path).read_text())
    origin = catalogue["source"]["name"]

    units, units_created = {}, 0
    for declared in catalogue["units"]:
        unit = Unit.objects.filter(symbol=declared["symbol"]).first()
        if unit is None:
            name = declared["name"]
            if Unit.objects.filter(name=name).exists():  # names are unique too
                name = f"{name} ({declared['symbol']})"
            unit = Unit.objects.create(symbol=declared["symbol"], name=name, description=declared.get("description"))
            units_created += 1
        units[unit.symbol] = unit

    created = updated = 0
    for entry in catalogue["templates"]:
        defaults = {field: entry[field] for field in _TEMPLATE_FIELDS}
        defaults.update(unit=units[entry["unit"]], origin=origin)
        _, was_created = StyleTemplate.objects.update_or_create(
            organisation=None, slug=entry["slug"], defaults=defaults
        )
        created += was_created
        updated += not was_created
    return Loaded(templates_created=created, templates_updated=updated, units_created=units_created)
