from django.db import migrations


class Migration(migrations.Migration):
    """Manual Upload data is not migrated (ADR 0034): both tables are dropped and the session recreated."""

    dependencies = [
        ('georivaingestion', '0009_pushfeed_drop_manualuploadconfig'),
    ]

    operations = [
        migrations.DeleteModel(name='UploadedFile'),
        migrations.DeleteModel(name='UploadSession'),
    ]
