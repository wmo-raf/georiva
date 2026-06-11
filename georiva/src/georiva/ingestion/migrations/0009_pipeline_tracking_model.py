import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("georivaingestion", "0008_fileingestionjob_file_ingestion_fk"),
        ("georivacore", "0001_initial"),
    ]

    operations = [
        # DataArrival: swap collection FK for catalog FK
        migrations.AddField(
            model_name="dataarrival",
            name="catalog",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="data_arrivals",
                to="georivacore.catalog",
            ),
        ),
        migrations.RemoveField(
            model_name="dataarrival",
            name="collection",
        ),

        # FileIngestion: add collections M2M
        migrations.AddField(
            model_name="fileingestion",
            name="collections",
            field=models.ManyToManyField(
                blank=True,
                related_name="file_ingestions",
                to="georivacore.collection",
            ),
        ),

        # FileIngestion: remove item FK
        migrations.RemoveField(
            model_name="fileingestion",
            name="item",
        ),

        # FileIngestion: remove catalog_slug and collection_slug char fields
        migrations.RemoveField(
            model_name="fileingestion",
            name="catalog_slug",
        ),
        migrations.RemoveField(
            model_name="fileingestion",
            name="collection_slug",
        ),
    ]
