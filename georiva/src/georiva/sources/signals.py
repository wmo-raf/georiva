from django.db.models.signals import post_save
from django.dispatch import receiver


@receiver(post_save, sender="georivasources.FetchRun")
def _fetch_run_post_save(sender, instance, created, update_fields, **kwargs):
    from georiva.ingestion.events import publish_event
    if created:
        publish_event({
            "type": "fetch_run.created",
            "id": instance.pk,
            "status": instance.status,
            "data_feed_id": instance.data_feed_id,
        })
        return
    if update_fields is None or "status" not in update_fields:
        return
    publish_event({
        "type": "fetch_run.status_changed",
        "id": instance.pk,
        "status": instance.status,
    })


@receiver(post_save, sender="georivasources.FetchedFile")
def _fetched_file_post_save(sender, instance, created, update_fields, **kwargs):
    if created:
        return
    if update_fields is None or "status" not in update_fields:
        return
    from georiva.ingestion.events import publish_event
    publish_event({
        "type": "fetched_file.status_changed",
        "id": instance.pk,
        "fetch_run_id": instance.fetch_run_id,
        "status": instance.status,
        "file_path": instance.file_path,
    })
