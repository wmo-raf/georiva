"""
Big-bang migration to Icechunk (design decision 8).

Every manifest row is flipped to PENDING with its coverage cleared; the
5-minute sweep then rebuilds each variable's repo from scratch.  Accepted
cost: one build-cycle of not-READY per variable.  The retired kerchunk JSON
keys are removed separately by the `cleanup_kerchunk_manifests` command.
"""

from django.db import migrations


def flip_all_pending(apps, schema_editor):
    VirtualZarrManifest = apps.get_model("georivavirtualzarr", "VirtualZarrManifest")
    VirtualZarrManifest.objects.update(
        status="pending",
        repo_path="",
        snapshot_id="",
        watermark=None,
        time_start=None,
        time_end=None,
        item_count=0,
        built_at=None,
        locked_at=None,
        locked_by="",
        error="",
    )


class Migration(migrations.Migration):
    dependencies = [
        ("georivavirtualzarr", "0002_remove_virtualzarrmanifest_manifest_path_and_more"),
    ]

    operations = [
        migrations.RunPython(flip_all_pending, migrations.RunPython.noop),
    ]
