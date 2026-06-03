import logging

from django.apps import AppConfig
from django.db.models.signals import post_save, m2m_changed

logger = logging.getLogger(__name__)


def on_variable_save(sender, instance, **kwargs):
    from georiva.core.palette_cache import warm_variable
    warm_variable(instance)


def on_palette_save(sender, instance, **kwargs):
    from georiva.core.palette_cache import warm_variable
    for variable in (
            instance.variable_set
                    .filter(is_active=True)
                    .select_related('collection__catalog', 'palette')
                    .prefetch_related('palette__stops')
    ):
        warm_variable(variable)


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


def _sync_keep_for_collection(collection):
    """Create or remove the incoming/.keep for a collection based on whether
    it has any linked DataFeeds. Automated collections (with a feed) don't
    use the manual dropzone, so no .keep is needed there."""
    from georiva.core.storage import storage, BucketType

    bucket = storage.bucket(BucketType.INCOMING)
    keep_path = f"{collection.catalog.slug}/{collection.slug}/.keep"

    if collection.data_feeds.exists():
        _delete_keep(bucket, keep_path)
    else:
        _put_keep(bucket, keep_path)


def collection_post_save(sender, instance, created, **kwargs):
    _sync_keep_for_collection(instance)


def data_feed_collections_changed(sender, instance, action, pk_set, **kwargs):
    """Fired when collections are added to or removed from a DataFeed via M2M.

    `instance` is the DataFeed; `pk_set` is the set of Collection PKs affected.
    Re-evaluate the .keep status for each affected collection.
    """
    if action not in ('post_add', 'post_remove', 'post_clear'):
        return

    from georiva.core.models import Collection

    if action == 'post_clear':
        # All collections removed — re-sync every collection that was linked
        # (we don't have pk_set for post_clear, so sync all in catalog)
        for collection in instance.collections.all():
            _sync_keep_for_collection(collection)
    else:
        for collection in Collection.objects.filter(pk__in=pk_set):
            _sync_keep_for_collection(collection)


class CoreConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.core'
    label = 'georivacore'
    verbose_name = "GeoRIVA Core"

    def ready(self):
        from .models import Collection, Variable
        from .tasks import update_collection_data_feed_periodic_task
        from .models.visualization import ColorPalette
        from georiva.sources.models import DataFeed

        post_save.connect(update_collection_data_feed_periodic_task, sender=Collection)
        post_save.connect(collection_post_save, sender=Collection)

        # When a collection is linked/unlinked from a DataFeed via M2M,
        # keep the incoming/.keep in sync.
        m2m_changed.connect(
            data_feed_collections_changed,
            sender=DataFeed.collections.through,
        )

        post_save.connect(on_variable_save, sender=Variable)
        post_save.connect(on_palette_save, sender=ColorPalette)
