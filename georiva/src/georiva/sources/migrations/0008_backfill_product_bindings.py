"""Backfill DerivedProductInput/Output rows for products enabled before
pinning existed (ADR-0010 §2).

Resolution needs each feed's plugin-declared products (``get_derived_products``),
which live on the concrete model, not the historical one — so the backfill calls
the live service. It is idempotent (upsert on ``(product, role)``), tolerant of
orphans and un-provisioned inputs, and a no-op on a fresh database.
"""
from django.db import migrations


def backfill(apps, schema_editor):
    from georiva.sources.product_service import backfill_bindings

    backfill_bindings()


class Migration(migrations.Migration):

    dependencies = [
        ("georivasources", "0007_derivedproductinput_derivedproductoutput"),
    ]

    operations = [
        migrations.RunPython(backfill, migrations.RunPython.noop),
    ]
