import logging

logger = logging.getLogger(__name__)


class LoaderProfileViewSetRegistry:
    def __init__(self):
        self._viewsets = {}
    
    def register(self, viewset_class):
        """Register a viewset class. Uses the class's 'model' as key."""
        if not getattr(viewset_class, 'model', None):
            raise ValueError(f"{viewset_class.type} must define 'model'")
        
        self._viewsets[viewset_class.type] = viewset_class
        
        logger.info(f"Registered loader profile viewset: {viewset_class.type}")
    
    def get(self, model_cls):
        if model_cls not in self._viewsets:
            raise ValueError(f"Unknown loader profile model: {model_cls}")
        return self._viewsets[model_cls]


loader_profile_viewset_registry = LoaderProfileViewSetRegistry()
