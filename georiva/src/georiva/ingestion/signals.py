"""Live ingestion events, each stamped with the organisation it belongs to.

Every event published here is fanned out on one Redis channel and read by SSE
listeners on every organisation's admin. The ``org`` field is what keeps those
listeners apart: without it the stream is a single instance-wide feed carrying
one institution's file paths onto another's screen.

Where the org comes from differs by record, and neither route is a fallback for
the other:

* records that own a storage key (``FileIngestion``, its jobs) read the leading
  segment of that key, which is the organisation slug by the storage grammar —
  they exist before their collections are known, so no FK chain is available;
* records with a catalog behind them (``UploadSession`` and its files) read it
  through that chain.

An event whose organisation cannot be determined is published with ``org: None``
and reaches nobody's stream. That is deliberate: an unattributable event is not
one to broadcast to everybody.
"""
from django.db.models.signals import post_save
from django.dispatch import receiver

from georiva.core.storage.path_resolution import org_slug_from_key


def _session_org_slug(session):
    catalog = getattr(session, "catalog", None)
    organisation = getattr(catalog, "organisation", None)
    return organisation.slug if organisation else None


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


@receiver(post_save, sender="georivaingestion.UploadSession")
def _upload_session_post_save(sender, instance, created, update_fields, **kwargs):
    from georiva.ingestion.events import publish_event
    org = _session_org_slug(instance)
    if created:
        publish_event({
            "type": "upload_session.created",
            "org": org,
            "id": instance.pk,
            "status": instance.status,
            "catalog_name": instance.catalog.name if instance.catalog_id else None,
        })
        return
    if update_fields is None or "status" not in update_fields:
        return
    publish_event({
        "type": "upload_session.status_changed",
        "org": org,
        "id": instance.pk,
        "status": instance.status,
    })


@receiver(post_save, sender="georivaingestion.UploadedFile")
def _uploaded_file_post_save(sender, instance, created, update_fields, **kwargs):
    if created:
        return
    if update_fields is None or "status" not in update_fields:
        return
    from georiva.ingestion.events import publish_event
    publish_event({
        "type": "uploaded_file.status_changed",
        "org": _session_org_slug(instance.session),
        "id": instance.pk,
        "session_id": instance.session_id,
        "status": instance.status,
        "filename": instance.original_filename or instance.file_path or "",
    })
