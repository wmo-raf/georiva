"""
Built-in recipe families. Importing this package registers them on the engine
(via the RecipeRegistry decorator). Loaded from ProcessingConfig.ready().
"""
from . import climatology, promotion  # noqa: F401
