from django.db.models.signals import post_save
from django.dispatch import receiver


@receiver(post_save, sender="georivaingestion.FileIngestion")
def _file_ingestion_post_save(sender, instance, created, update_fields, **kwargs):
    from georiva.ingestion.events import publish_event
    if created:
        publish_event({
            "type": "file_ingestion.created",
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
        "id": instance.pk,
        "state": instance.state,
    })


@receiver(post_save, sender="georivaingestion.UploadSession")
def _upload_session_post_save(sender, instance, created, update_fields, **kwargs):
    from georiva.ingestion.events import publish_event
    if created:
        publish_event({
            "type": "upload_session.created",
            "id": instance.pk,
            "status": instance.status,
            "catalog_name": instance.catalog.name if instance.catalog_id else None,
        })
        return
    if update_fields is None or "status" not in update_fields:
        return
    publish_event({
        "type": "upload_session.status_changed",
        "id": instance.pk,
        "status": instance.status,
    })
