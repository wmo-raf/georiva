from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("georivacore", "0028_variablestyle_template"),
    ]

    operations = [
        migrations.AddField(
            model_name="variablestyle",
            name="origin",
            field=models.CharField(
                choices=[("template", "From a template"), ("ramp", "From a ramp"), ("custom", "Custom")],
                default="custom",
                help_text=(
                    "How this style was made: from a template, from a ramp, or custom. "
                    "A custom style keeps any ramp or template link as history only."
                ),
                max_length=16,
            ),
        ),
        migrations.AddField(
            model_name="variablestyle",
            name="reverse",
            field=models.BooleanField(
                default=False,
                help_text="A ramp style: whether its ramp's colours run the other way over the range.",
            ),
        ),
    ]
