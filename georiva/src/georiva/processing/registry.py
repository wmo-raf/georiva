"""
RecipeRegistry — decorator registry for derivation recipes.

Mirrors the format/source plugin registries: families register by ``type`` and
the engine looks them up by name. Adding a family never edits the engine.
"""
import logging

logger = logging.getLogger(__name__)


class RecipeRegistry:
    _recipes: dict = {}

    @classmethod
    def register(cls, recipe_cls):
        """Class decorator: register a recipe by its ``type``."""
        if not getattr(recipe_cls, "type", ""):
            raise ValueError(f"{recipe_cls.__name__} must define a non-empty 'type'")
        cls._recipes[recipe_cls.type] = recipe_cls
        logger.info("Registered recipe: %s (v%s)", recipe_cls.type, recipe_cls.version)
        return recipe_cls

    @classmethod
    def get(cls, recipe_type: str):
        """Return a recipe instance by type, or None."""
        recipe_cls = cls._recipes.get(recipe_type)
        return recipe_cls() if recipe_cls else None

    @classmethod
    def all_types(cls) -> list[str]:
        return sorted(cls._recipes)


recipe_registry = RecipeRegistry()
