import logging

logger = logging.getLogger(__name__)


class DataFeedViewSetRegistry:
    def __init__(self):
        self._viewsets = {}

    def register(self, viewset_class):
        """Register a viewset class. Uses the class's 'model' as key."""
        if not getattr(viewset_class, "model", None):
            raise ValueError(f"{viewset_class.type} must define 'model'")

        self._viewsets[viewset_class.type] = viewset_class

        logger.info(f"Registered data feed viewset: {viewset_class.type}")

    def get(self, model_cls):
        """The viewset of a DataFeed model, by model name; `None` for one with
        none. Viewsets are generated from the subclasses installed when the
        admin URLs load, so a subclass that joins later has none — and the
        callers (the add-feed page, `DataFeed.viewset`) are written for that."""
        return self._viewsets.get(model_cls)


data_feed_viewset_registry = DataFeedViewSetRegistry()
