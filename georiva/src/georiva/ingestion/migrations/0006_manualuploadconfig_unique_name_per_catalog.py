from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("georivaingestion", "0005_alter_manualuploadconfig_valid_time_format"),
    ]

    operations = [
        migrations.AddConstraint(
            model_name="manualuploadconfig",
            constraint=models.UniqueConstraint(
                fields=("catalog", "name"),
                name="unique_manual_upload_config_name_per_catalog",
            ),
        ),
    ]
