import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("georivacore", "0027_variablestyle_under_over"),
    ]

    operations = [
        migrations.AddField(
            model_name="variablestyle",
            name="template",
            field=models.ForeignKey(
                blank=True,
                help_text=(
                    "The Style template this style was applied from (#791) — lineage "
                    "only; the stops below are a snapshot and never follow the template."
                ),
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="+",
                to="georivacore.styletemplate",
            ),
        ),
    ]
