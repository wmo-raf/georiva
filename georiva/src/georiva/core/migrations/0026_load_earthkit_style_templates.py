from django.db import migrations

from georiva.core.style_catalogue import CATALOGUES, load_catalogue


def load(apps, schema_editor):
    # Named, not the latest: a refresh adds a file and a migration of its own.
    load_catalogue(CATALOGUES / "earthkit-plots-1.0.4.json", apps=apps)


class Migration(migrations.Migration):
    dependencies = [
        ("georivacore", "0025_styletemplate"),
    ]

    operations = [
        migrations.RunPython(load, migrations.RunPython.noop),
    ]
