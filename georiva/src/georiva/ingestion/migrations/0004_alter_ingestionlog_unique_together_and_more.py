# Hand-written to use RenameModel (preserves existing data) instead of
# the auto-generated delete+create pair that Django produced.

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('georivacore', '0003_add_default_units'),
        ('georivaingestion', '0003_data_arrival'),
        ('georivasources', '0002_delete_datafeedjob'),
        ('task_ferry', '0002_add_ingestion_job'),
    ]

    operations = [
        # ── Rename IngestionLog → FileIngestion ───────────────────────────────
        migrations.RenameModel('IngestionLog', 'FileIngestion'),

        # ── Rename IngestionJob → FileIngestionJob ────────────────────────────
        migrations.RenameModel('IngestionJob', 'FileIngestionJob'),

        # ── Rename ingestion_log FK on FileIngestionJob → file_ingestion ──────
        migrations.RenameField(
            model_name='fileingestionjob',
            old_name='ingestion_log',
            new_name='file_ingestion',
        ),

        # ── Update related_name on item FK (ingestion_logs → file_ingestions) ─
        migrations.AlterField(
            model_name='fileingestion',
            name='item',
            field=models.ForeignKey(
                blank=True,
                db_constraint=False,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name='file_ingestions',
                to='georivacore.item',
            ),
        ),

        # ── Update related_name on data_feed_run FK ───────────────────────────
        migrations.AlterField(
            model_name='fileingestion',
            name='data_feed_run',
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name='file_ingestions',
                to='georivasources.datafeedrun',
            ),
        ),

        # ── Add data_arrival FK to FileIngestion ──────────────────────────────
        migrations.AddField(
            model_name='fileingestion',
            name='data_arrival',
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name='file_ingestions',
                to='georivaingestion.dataarrival',
            ),
        ),

        # ── Create DataArrivalJob ─────────────────────────────────────────────
        migrations.CreateModel(
            name='DataArrivalJob',
            fields=[
                ('job_ptr', models.OneToOneField(
                    auto_created=True,
                    on_delete=django.db.models.deletion.CASCADE,
                    parent_link=True,
                    primary_key=True,
                    serialize=False,
                    to='task_ferry.job',
                )),
                ('files_total', models.IntegerField(default=0)),
                ('files_fetched', models.IntegerField(default=0)),
                ('files_skipped', models.IntegerField(default=0)),
                ('files_failed', models.IntegerField(default=0)),
                ('bytes_transferred', models.BigIntegerField(default=0)),
                ('collection', models.ForeignKey(
                    blank=True, null=True,
                    on_delete=django.db.models.deletion.SET_NULL,
                    related_name='data_arrival_jobs',
                    to='georivacore.collection',
                )),
                ('data_arrival', models.ForeignKey(
                    blank=True, null=True,
                    on_delete=django.db.models.deletion.SET_NULL,
                    related_name='jobs',
                    to='georivaingestion.dataarrival',
                )),
                ('data_feed', models.ForeignKey(
                    blank=True, null=True,
                    on_delete=django.db.models.deletion.SET_NULL,
                    related_name='arrival_jobs',
                    to='georivasources.datafeed',
                )),
            ],
            bases=('task_ferry.job',),
        ),
    ]
