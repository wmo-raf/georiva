from django.db import migrations

from georiva.events.operations import AppendOnlyTable


class Migration(migrations.Migration):
    dependencies = [
        ("georivaingestion", "0005_runingestionclosed_runingestionreopened"),
    ]

    operations = [
        AppendOnlyTable("RunIngestionClosed"),
        AppendOnlyTable("RunIngestionReopened"),
    ]
