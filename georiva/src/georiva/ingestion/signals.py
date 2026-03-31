from django.db.models.signals import post_delete
from django.dispatch import receiver


@receiver(post_delete, sender='georivacore.Item')
def cleanup_orphan_ingestion_log(sender, instance, **kwargs):
    """
    Delete the IngestionLog that produced this Item if no other Items
    still reference it.

    Note: only fires for individual .delete() calls, not queryset.delete().
    """
    log_id = instance.ingestion_log_id
    if log_id is None:
        return

    from georiva.ingestion.models import IngestionLog

    try:
        log = IngestionLog.objects.get(pk=log_id)
    except IngestionLog.DoesNotExist:
        return

    if not log.items.exists():
        log.delete()


@receiver(post_delete, sender='georivaingestion.IngestionLog')
def cleanup_orphan_loader_run(sender, instance, **kwargs):
    """
    Delete the LoaderRun that produced this IngestionLog if no other
    IngestionLog entries still reference it.
    """
    run_id = instance.loader_run_id
    if run_id is None:
        return

    from georiva.sources.models import LoaderRun

    try:
        run = LoaderRun.objects.get(pk=run_id)
    except LoaderRun.DoesNotExist:
        return

    if not run.ingestion_logs.exists():
        run.delete()