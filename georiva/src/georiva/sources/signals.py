"""Live acquisition events, each stamped with the organisation it belongs to.

Shares one Redis channel with the ingestion events, and the same rule: the
``org`` field is what keeps each organisation's SSE listeners to their own
records. A fetch run reaches its organisation through its feed's catalog; an
event that cannot be attributed carries ``org: None`` and reaches no stream.
"""
from django.db.models.signals import post_save
from django.dispatch import receiver


def _feed_org_slug(data_feed):
    catalog = getattr(data_feed, "catalog", None)
    organisation = getattr(catalog, "organisation", None)
    return organisation.slug if organisation else None


@receiver(post_save, sender="georivasources.FetchRun")
def _fetch_run_post_save(sender, instance, created, update_fields, **kwargs):
    from georiva.ingestion.events import publish_event
    org = _feed_org_slug(instance.data_feed)
    if created:
        publish_event({
            "type": "fetch_run.created",
            "org": org,
            "id": instance.pk,
            "status": instance.status,
            "data_feed_id": instance.data_feed_id,
        })
        return
    if update_fields is None or "status" not in update_fields:
        return
    publish_event({
        "type": "fetch_run.status_changed",
        "org": org,
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
        "org": _feed_org_slug(instance.fetch_run.data_feed),
        "id": instance.pk,
        "fetch_run_id": instance.fetch_run_id,
        "status": instance.status,
        "file_path": instance.file_path,
    })
