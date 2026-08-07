import logging

from django.apps import AppConfig
from django.db.models.signals import post_save, post_delete

logger = logging.getLogger(__name__)


def on_variable_save(sender, instance, **kwargs):
    from georiva.core.machine_plane.palette_cache import warm_variable
    warm_variable(instance)


def on_style_change(sender, instance, **kwargs):
    """A style was saved or deleted: re-warm its variable's render config.

    Styles are variable-owned rows, so no fan-out is needed — and because the
    stops snapshot lives on this very row, an edit made anywhere (admin,
    shell, API) lands here, closing the old gap where stop edits outside
    Wagtail never re-warmed. On delete the variable may itself be mid-cascade;
    a vanished variable has nothing to warm.
    """
    from georiva.core.machine_plane.palette_cache import warm_variable
    from .models import Variable

    variable = (
        Variable.objects
        .filter(pk=instance.variable_id)
        .select_related('collection__catalog__organisation')
        .prefetch_related('styles')
        .first()
    )
    if variable is not None:
        warm_variable(variable)


def on_variable_delete(sender, instance, **kwargs):
    """A deleted variable's render config must not outlive it until the next
    sweep — the old missing-delete-signal gap."""
    from georiva.core.machine_plane.palette_cache import prune_variable
    prune_variable(instance)


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
    keep_path = f"{collection.catalog.storage_prefix}/{collection.slug}/.keep"
    
    if collection.feed_links.exists():
        _delete_keep(bucket, keep_path)
    else:
        _put_keep(bucket, keep_path)


def collection_post_save(sender, instance, created, **kwargs):
    _sync_keep_for_collection(instance)


def reindex_catalog_for_collection(sender, instance, **kwargs):
    """Reindex a collection's parent catalog so the admin header search stays
    in sync with the catalog's denormalized ``get_collection_names`` field."""
    from wagtail.search import index as search_index

    catalog = getattr(instance, "catalog", None)
    if catalog is not None:
        try:
            search_index.insert_or_update_object(catalog)
        except Exception as e:  # pragma: no cover - indexing must never break saves
            logger.warning("Could not reindex catalog %s: %s", catalog, e)


def data_feed_collection_link_saved(sender, instance, **kwargs):
    """Fired when a DataFeedCollectionLink is created or updated."""
    _sync_keep_for_collection(instance.collection)


def data_feed_collection_link_deleted(sender, instance, **kwargs):
    """Fired when a DataFeedCollectionLink is deleted."""
    _sync_keep_for_collection(instance.collection)


class CoreConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'georiva.core'
    label = 'georivacore'
    verbose_name = "GeoRIVA Core"
    
    def ready(self):
        from .models import Collection, Variable
        from georiva.sources.tasks import update_collection_data_feed_periodic_task, update_link_data_feed_periodic_task
        from .models.visualization import VariableStyle
        from georiva.sources.models import DataFeed

        post_save.connect(update_collection_data_feed_periodic_task, sender=Collection)
        post_save.connect(collection_post_save, sender=Collection)

        # Keep the parent catalog's search index fresh as collections change,
        # so the admin header search can match a catalog by collection name.
        post_save.connect(reindex_catalog_for_collection, sender=Collection)
        post_delete.connect(reindex_catalog_for_collection, sender=Collection)

        # When a collection is linked/unlinked from a DataFeed:
        # keep the incoming/.keep in sync and recalculate the PeriodicTask interval.
        from georiva.sources.models import DataFeedCollectionLink
        post_save.connect(data_feed_collection_link_saved, sender=DataFeedCollectionLink)
        post_delete.connect(data_feed_collection_link_deleted, sender=DataFeedCollectionLink)
        post_save.connect(update_link_data_feed_periodic_task, sender=DataFeedCollectionLink)
        post_delete.connect(update_link_data_feed_periodic_task, sender=DataFeedCollectionLink)
        
        post_save.connect(on_variable_save, sender=Variable)
        post_delete.connect(on_variable_delete, sender=Variable)
        post_save.connect(on_style_change, sender=VariableStyle)
        post_delete.connect(on_style_change, sender=VariableStyle)
