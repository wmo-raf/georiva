from django.db.models.signals import post_save
from django.dispatch import receiver


@receiver(post_save, sender="georivaingestion.DataArrival")
def _data_arrival_status_changed(sender, instance, created, update_fields, **kwargs):
    if created:
        return
    if update_fields is None or "status" not in update_fields:
        return
    from georiva.ingestion.events import publish_event
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
