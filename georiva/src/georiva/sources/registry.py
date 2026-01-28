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


class FetchStrategyRegistry:
    def __init__(self):
        self._strategies = {}
    
    def register(self, strategy_class, default_config: dict = None):
        """Register a strategy class. Uses the class's 'type' as key."""
        if not getattr(strategy_class, 'type', None):
            raise ValueError(f"{strategy_class.__name__} must define 'type'")
        
        if not getattr(strategy_class, 'label', None):
            raise ValueError(f"{strategy_class.__name__} must define 'label'")
        
        self._strategies[strategy_class.type] = {
            'class': strategy_class,
            'default_config': default_config or {},
        }
        
        logger.info(f"Registered strategy class: {strategy_class.__name__}")
    
    def get(self, type_slug: str):
        if type_slug not in self._strategies:
            raise ValueError(f"Unknown fetch strategy: {type_slug}")
        return self._strategies[type_slug]
    
    def get_class(self, type_slug: str):
        return self.get(type_slug)['class']
    
    def choices(self):
        """For Django model field choices."""
        
        return [
            (info['class'].type, info['class'].label)
            for info in self._strategies.values()
        ]


data_source_registry = DataSourceRegistry()
fetch_strategy_registry = FetchStrategyRegistry()
