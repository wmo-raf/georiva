import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("georivacore", "0030_backfill_variablestyle_origin"),
    ]

    operations = [
        migrations.AlterField(
            model_name="variablestyle",
            name="ramp",
            field=models.ForeignKey(
                blank=True,
                help_text=(
                    "The ramp a ramp style's stops are generated from on every save; "
                    "for a custom style, history only. The stops below are what renders."
                ),
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="+",
                to="georivacore.colorramp",
            ),
        ),
    ]
