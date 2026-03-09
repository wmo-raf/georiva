import logging

logger = logging.getLogger(__name__)


class DataSourceRegistry:
    def __init__(self):
        self._sources = {}
    
    def register(self, source_class, default_config: dict = None):
        """Register a source_class class. Uses the class's 'type' as key."""
        if not getattr(source_class, 'type', None):
            raise ValueError(f"{source_class.__name__} must define 'type'")
        
        if not getattr(source_class, 'label', None):
            raise ValueError(f"{source_class.__name__} must define 'label'")
        
        self._sources[source_class.type] = {
            'class': source_class,
            'default_config': default_config or {},
        }
        
        logger.info(f"Registered data source class: {source_class.__name__}")
    
    def get(self, type_slug: str):
        if type_slug not in self._sources:
            raise ValueError(f"Unknown data source: {type_slug}")
        return self._sources[type_slug]
    
    def get_class(self, type_slug: str):
        return self.get(type_slug)['class']
    
    def choices(self):
        """For Django model field choices."""
        
        choices = [
            (info['class'].type, info['class'].label)
            for info in self._sources.values()
        ]
        
        return choices


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


data_source_registry = DataSourceRegistry()

loader_profile_viewset_registry = LoaderProfileViewSetRegistry()
