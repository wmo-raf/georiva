from django.db.models.signals import post_delete
from django.dispatch import receiver


@receiver(post_delete, sender='georivaingestion.IngestionLog')
def cleanup_orphan_data_feed_run(sender, instance, **kwargs):
    """
    Delete the DataFeedRun that produced this IngestionLog if no other
    IngestionLog entries still reference it.
    """
    run_id = instance.data_feed_run_id
    if run_id is None:
        return

    from georiva.sources.models import DataFeedRun

    try:
        run = DataFeedRun.objects.get(pk=run_id)
    except DataFeedRun.DoesNotExist:
        return

    if not run.ingestion_logs.exists():
        run.delete()