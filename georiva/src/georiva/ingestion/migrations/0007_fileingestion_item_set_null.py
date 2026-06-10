import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("georivacore", "0001_initial"),
        ("georivaingestion", "0006_manualuploadconfig_unique_name_per_catalog"),
    ]

    operations = [
        migrations.AlterField(
            model_name="fileingestion",
            name="item",
            field=models.ForeignKey(
                blank=True,
                db_constraint=False,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="file_ingestions",
                to="georivacore.item",
            ),
        ),
    ]
