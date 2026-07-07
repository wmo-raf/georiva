"""Backfill StagingCollection.collection for rows registered before the link
existed (ADR-0010 §3): match each unlinked staging collection to the core
Collection with the same catalog + slug. Unmatched rows stay null. Pure ORM over
historical models — no plugin logic needed.
"""
from django.db import migrations


def backfill_staging_links(apps, schema_editor):
    StagingCollection = apps.get_model("georivastaging", "StagingCollection")
    Collection = apps.get_model("georivacore", "Collection")

    for sc in StagingCollection.objects.filter(collection__isnull=True):
        core = Collection.objects.filter(
            catalog_id=sc.catalog_id, slug=sc.slug
        ).first()
        if core is not None:
            sc.collection = core
            sc.save(update_fields=["collection"])


class Migration(migrations.Migration):

    dependencies = [
        ("georivastaging", "0003_stagingcollection_collection"),
    ]

    operations = [
        migrations.RunPython(backfill_staging_links, migrations.RunPython.noop),
    ]
