"""Live ingestion events, each stamped with the organisation it belongs to.

Every event published here is fanned out on one Redis channel and read by SSE
listeners on every organisation's admin. The ``org`` field is what keeps those
listeners apart: without it the stream is a single instance-wide feed carrying
one institution's file paths onto another's screen.

The org is read from the record's storage key: ``FileIngestion`` and its jobs
exist before their collections are known, so no FK chain is available, but the
leading segment of the key is the organisation slug by the storage grammar.

An event whose organisation cannot be determined is published with ``org: None``
and reaches nobody's stream. That is deliberate: an unattributable event is not
one to broadcast to everybody.
"""
from django.db.models.signals import post_save
from django.dispatch import receiver

from georiva.core.storage.path_resolution import org_slug_from_key


@receiver(post_save, sender="georivaingestion.FileIngestion")
def _file_ingestion_post_save(sender, instance, created, update_fields, **kwargs):
    from georiva.ingestion.events import publish_event
    org = org_slug_from_key(instance.file_path)
    if created:
        publish_event({
            "type": "file_ingestion.created",
            "org": org,
            "id": instance.pk,
            "status": instance.status,
            "bucket": instance.bucket,
            "file_path": instance.file_path,
            "created_at": instance.created_at.isoformat() if instance.created_at else None,
        })
        return
    if update_fields is None or "status" not in update_fields:
        return
    publish_event({
        "type": "file_ingestion.status_changed",
        "org": org,
        "id": instance.pk,
        "status": instance.status,
    })


@receiver(post_save, sender="georivaingestion.FileIngestionJob")
def _file_ingestion_job_state_changed(sender, instance, created, update_fields, **kwargs):
    if created:
        return
    if update_fields is None or "state" not in update_fields:
        return
    from georiva.ingestion.events import publish_event
    publish_event({
        "type": "job.state_changed",
        "org": org_slug_from_key(instance.file_path),
        "id": instance.pk,
        "state": instance.state,
    })
