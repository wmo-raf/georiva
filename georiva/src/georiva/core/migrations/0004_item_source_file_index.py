from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("georivacore", "0003_add_default_units"),
    ]

    operations = [
        migrations.AlterField(
            model_name="item",
            name="source_file",
            field=models.CharField(
                blank=True,
                db_index=True,
                help_text="Original source file path",
                max_length=500,
            ),
        ),
    ]
