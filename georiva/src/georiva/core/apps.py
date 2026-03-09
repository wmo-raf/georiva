import logging

from django.apps import AppConfig
from django.db.models.signals import post_save

logger = logging.getLogger(__name__)


def _put_keep(bucket, path):
    try:
        bucket.save(path, b"")
        logger.info("Created .keep: %s", path)
    except Exception as e:
        logger.warning("Could not create .keep at %s: %s", path, e)


def _delete_keep(bucket, path):
    try:
        bucket.delete(path)
        logger.info("Deleted .keep: %s", path)
    except Exception as e:
        logger.warning("Could not delete .keep at %s: %s", path, e)


def collection_post_save(sender, instance, created, **kwargs):
    from georiva.core.storage import storage, BucketType
    
    bucket = storage.bucket(BucketType.INCOMING)
    keep_path = f"{instance.catalog.slug}/{instance.slug}/.keep"
    
    if instance.loader_profile_id:
        _delete_keep(bucket, keep_path)  # already silent fail
    else:
        _put_keep(bucket, keep_path)


class CoreConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.core'
    label = 'georivacore'
    verbose_name = "GeoRIVA Core"
    
    def ready(self):
        from .models import Collection
        from .tasks import update_collection_loader_plugin_periodic_task
        
        post_save.connect(update_collection_loader_plugin_periodic_task, sender=Collection)
        
        # Create .keep file in incoming bucket when a new collection is created
        post_save.connect(collection_post_save, sender=Collection)
