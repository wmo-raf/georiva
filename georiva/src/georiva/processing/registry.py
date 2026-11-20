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
        refused = cls.undeclared_units(recipe_cls)
        if refused:
            # Logged, not raised: a plugin's recipe registers as the app loads,
            # and one refused recipe must not stop the rest from loading.
            logger.error("Refused recipe %s (v%s): %s", recipe_cls.type, recipe_cls.version, refused)
            return recipe_cls
        cls._recipes[recipe_cls.type] = recipe_cls
        logger.info("Registered recipe: %s (v%s)", recipe_cls.type, recipe_cls.version)
        return recipe_cls

    @staticmethod
    def undeclared_units(recipe_cls) -> str:
        """Why ``recipe_cls`` cannot be registered for the units it declares,
        or ``""`` (ADR 0046 §13): every input names the unit it is read in, and
        a recipe that creates its output Variables names their unit."""
        from georiva.processing.recipe import BaseRecipe

        units = getattr(recipe_cls, "input_units", None) or {}
        if not units:
            return (
                "it declares no input units. Name the unit each input is read in "
                "(input_units = {role: unit}), so its output never follows a served unit."
            )
        missing = sorted(role for role, unit in units.items() if not unit)
        if missing:
            return f"input(s) {', '.join(repr(r) for r in missing)} declare no unit to be read in."
        if recipe_cls.creates_output_variables and recipe_cls.output_unit is BaseRecipe.output_unit:
            return "it creates its output Variables but declares no output unit (output_unit)."
        return ""

    @classmethod
    def get(cls, recipe_type: str):
        """Return a recipe instance by type, or None."""
        recipe_cls = cls._recipes.get(recipe_type)
        return recipe_cls() if recipe_cls else None

    @classmethod
    def all_types(cls) -> list[str]:
        return sorted(cls._recipes)


recipe_registry = RecipeRegistry()
