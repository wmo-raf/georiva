import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("georivaingestion", "0007_fileingestion_item_set_null"),
    ]

    operations = [
        migrations.AlterField(
            model_name="fileingestionjob",
            name="file_ingestion",
            field=models.ForeignKey(
                blank=True,
                help_text="Lock record for this file; set after the lock is acquired.",
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="jobs",
                to="georivaingestion.fileingestion",
            ),
        ),
    ]
