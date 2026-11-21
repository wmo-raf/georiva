import logging
from pathlib import Path

from georiva.utils.path import PathLike

from .base import BaseFormatPlugin

logger = logging.getLogger(__name__)


class FormatRegistry:
    _plugins: dict[str, type[BaseFormatPlugin]] = {}
    _extension_map: dict[str, str] = {}  # extension -> format name

    @classmethod
    def register(cls, plugin_class: type[BaseFormatPlugin]) -> type[BaseFormatPlugin]:
        """
        Decorator to register a format plugin.

        Usage:
            @FormatRegistry.register
            class GRIBFormatPlugin(BaseFormatPlugin):
                name = "grib2"
                ...
        """
        cls._plugins[plugin_class.name] = plugin_class

        for ext in plugin_class.extensions:
            ext_lower = ext.lower().lstrip(".")
            cls._extension_map[ext_lower] = plugin_class.name

        logger.info(f"Registered format plugin: {plugin_class.name}")
        return plugin_class

    @classmethod
    def get(cls, name: str) -> BaseFormatPlugin | None:
        plugin_class = cls._plugins.get(name)
        if plugin_class:
            return plugin_class()
        return None

    @classmethod
    def get_by_extension(cls, extension: str) -> BaseFormatPlugin | None:
        ext_lower = extension.lower().lstrip(".")
        format_name = cls._extension_map.get(ext_lower)
        if format_name:
            return cls.get(format_name)
        return None

    @classmethod
    def get_for_file(cls, file_path: PathLike) -> BaseFormatPlugin | None:
        """
        Get the appropriate plugin for a file.

        First tries by extension, then asks each plugin if it can handle the file.
        """

        file_path = Path(file_path)

        plugin = cls.get_by_extension(file_path.suffix)
        if plugin and plugin.can_handle(file_path):
            return plugin

        for plugin_class in cls._plugins.values():
            plugin = plugin_class()
            if plugin.can_handle(file_path):
                return plugin

        return None

    @classmethod
    def all(cls) -> dict[str, type[BaseFormatPlugin]]:
        return cls._plugins.copy()

    @classmethod
    def choices(cls) -> list[tuple[str, str]]:
        return [(name, plugin.display_name) for name, plugin in cls._plugins.items()]


format_registry = FormatRegistry()
