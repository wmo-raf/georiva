from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("georivacore", "0026_load_earthkit_style_templates"),
    ]

    operations = [
        migrations.AddField(
            model_name="variablestyle",
            name="under",
            field=models.CharField(
                choices=[("clamp", "Clamp to the end colour"), ("transparent", "Transparent")],
                default="clamp",
                help_text="What a value below the first stop is drawn as.",
                max_length=16,
            ),
        ),
        migrations.AddField(
            model_name="variablestyle",
            name="over",
            field=models.CharField(
                choices=[("clamp", "Clamp to the end colour"), ("transparent", "Transparent")],
                default="clamp",
                help_text="What a value above the last stop is drawn as.",
                max_length=16,
            ),
        ),
    ]
