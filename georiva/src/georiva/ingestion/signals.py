from django.db.models.signals import post_save
from django.dispatch import receiver


@receiver(post_save, sender="georivaingestion.DataArrival")
def _data_arrival_post_save(sender, instance, created, update_fields, **kwargs):
    from georiva.ingestion.events import publish_event
    if created:
        collection_name = None
        catalog_name = None
        try:
            if instance.collection_id:
                col = instance.collection
                collection_name = col.name if col else None
                catalog_name = col.catalog.name if col and col.catalog_id else None
        except Exception:
            pass
        publish_event({
            "type": "data_arrival.created",
            "id": instance.pk,
            "trigger": instance.trigger,
            "status": instance.status,
            "file_path": instance.file_path,
            "started_at": instance.started_at.isoformat() if instance.started_at else None,
            "collection_name": collection_name,
            "catalog_name": catalog_name,
            "file_ingestions": [],
        })
        return
    if update_fields is None or "status" not in update_fields:
        return
    publish_event({
        "type": "data_arrival.status_changed",
        "id": instance.pk,
        "status": instance.status,
    })


@receiver(post_save, sender="georivaingestion.FileIngestion")
def _file_ingestion_status_changed(sender, instance, created, update_fields, **kwargs):
    if created:
        return
    if update_fields is None or "status" not in update_fields:
        return
    from georiva.ingestion.events import publish_event
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
