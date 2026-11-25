from django.db import migrations

from georiva.core.style_catalogue import CATALOGUES, load_catalogue


def load(apps, schema_editor):
    # Named, not the latest: a refresh adds a file and a migration of its own.
    # An additional source beside earthkit-plots (#811): its own slugs, nothing of 0026's touched.
    load_catalogue(CATALOGUES / "magics-cb97794b.json", apps=apps)


class Migration(migrations.Migration):
    dependencies = [
        ("georivacore", "0031_alter_variablestyle_ramp"),
    ]

    operations = [
        migrations.RunPython(load, migrations.RunPython.noop),
    ]
