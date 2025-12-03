import importlib
import pkgutil
from dataclasses import dataclass
from typing import Callable, Dict


class OperatorRegistry:
    """
    Registry for analysis operators.
    
    Supports:
    - Built-in operators
    - User-defined operators (Python functions)
    - Plugin operators (from external packages)
    """
    
    _operators: Dict[str, 'RegisteredOperator'] = {}
    
    @classmethod
    def register(
            cls,
            name: str,
            category: str,
            inputs: list,
            parameters: list = None,
            description: str = '',
            requires_time_series: bool = False,
    ):
        """Decorator to register an operator function."""
        
        def decorator(func: Callable):
            cls._operators[name] = RegisteredOperator(
                name=name,
                func=func,
                category=category,
                inputs=inputs,
                parameters=parameters or [],
                description=description,
                requires_time_series=requires_time_series,
            )
            return func
        
        return decorator
    
    @classmethod
    def get(cls, name: str) -> 'RegisteredOperator':
        if name not in cls._operators:
            raise KeyError(f"Unknown operator: {name}")
        return cls._operators[name]
    
    @classmethod
    def list_operators(cls, category: str = None) -> list:
        ops = cls._operators.values()
        if category:
            ops = [o for o in ops if o.category == category]
        return list(ops)
    
    @classmethod
    def load_plugins(cls):
        """
        Auto-discover operator plugins.
        
        Looks for packages named `georiva_operators_*` and loads them.
        """
        
        for importer, modname, ispkg in pkgutil.iter_modules():
            if modname.startswith('georiva_operators_'):
                try:
                    module = importlib.import_module(modname)
                    if hasattr(module, 'register_operators'):
                        module.register_operators(cls)
                except Exception as e:
                    print(f"Failed to load operator plugin {modname}: {e}")


@dataclass
class RegisteredOperator:
    name: str
    func: Callable
    category: str
    inputs: list
    parameters: list
    description: str
    requires_time_series: bool
    
    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)
    
    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'category': self.category,
            'inputs': self.inputs,
            'parameters': self.parameters,
            'description': self.description,
            'requires_time_series': self.requires_time_series,
        }


# Convenience decorator
def register_operator(name: str, **kwargs):
    return OperatorRegistry.register(name, **kwargs)
