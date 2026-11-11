"""Shared by the handler tests: one file through ingestion, up to the failure record."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from georiva.ingestion.service import IngestionService


def ingest_file(path: Path, plugin, variable_name: str):
    """Run one file through IngestionService with the real *plugin* reading *path*.

    The real IngestionHandler and VariableExtractor run; only what surrounds
    them (catalog and collection lookup, storage) is stubbed. The point is that
    the plugin's refusal travels out of process_timestamp into the result the
    failure record is built from.
    """
    primary = SimpleNamespace(block_type="primary", value={"source_name": variable_name})
    variable = SimpleNamespace(slug=variable_name, is_active=True, sources=[primary])
    collection = MagicMock(slug="test-collection")
    collection.variables.all.return_value = [variable]
    catalog = MagicMock(clip_mode="none")

    sfm = MagicMock()
    sfm.download_to_temp.return_value.__enter__.return_value = path
    with (
        patch("georiva.ingestion.service.format_registry") as registry,
        patch("georiva.ingestion.service.storage"),
        patch("georiva.ingestion.service.resolve_org_catalog", return_value=(catalog, None)),
        patch("georiva.ingestion.service.FileIngestion"),
        patch.object(IngestionService, "_resolve_collections", return_value=[collection]),
        patch.object(IngestionService, "_get_first_variable_name", return_value=variable_name),
    ):
        registry.get.return_value = plugin
        service = IngestionService()
        service._source_file_manager = sfm
        return service.process_file(f"org/cat/test-collection/2026/01/01/{path.name}")
