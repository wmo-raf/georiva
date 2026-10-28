import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("georivaingestion", "0013_pushfeed_expected_since_pushfeedoverdue"),
    ]

    operations = [
        migrations.AddField(
            model_name="delivery",
            name="push_sender",
            field=models.ForeignKey(
                blank=True,
                help_text="The machine sender that wrote the file, matched from the Bucket notification's principal.",
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="deliveries",
                to="georivaingestion.pushsender",
            ),
        ),
        migrations.AlterField(
            model_name="deliveryreceived",
            name="sender",
            field=models.CharField(
                blank=True,
                help_text="Username of the person or label of the machine sender; blank when unknown.",
                max_length=150,
            ),
        ),
    ]
