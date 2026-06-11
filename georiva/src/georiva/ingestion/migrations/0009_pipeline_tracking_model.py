import django.db.models.deletion
from django.db import migrations, models


def _backfill_catalog(apps, schema_editor):
    """
    Populate DataArrival.catalog from the old collection FK before it is dropped.
    Rows without a collection (or whose collection has no catalog) are left NULL.
    """
    DataArrival = apps.get_model("georivaingestion", "DataArrival")
    for arrival in DataArrival.objects.filter(collection__isnull=False).select_related(
        "collection__catalog"
    ):
        if arrival.collection and arrival.collection.catalog_id:
            arrival.catalog_id = arrival.collection.catalog_id
            arrival.save(update_fields=["catalog_id"])


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
        # Backfill catalog from the existing collection FK before it is dropped.
        migrations.RunPython(_backfill_catalog, migrations.RunPython.noop),
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
