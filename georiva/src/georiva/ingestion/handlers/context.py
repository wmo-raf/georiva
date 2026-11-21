"""
IngestionContext — shared state for a single file ingestion run.

Constructed once in IngestionService.process_file() and passed to every
handler.  Avoids threading 8+ arguments through nested call stacks.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from georiva.ingestion.asset_writer import AssetWriter
    from georiva.ingestion.clipper import BoundaryClipper
    from georiva.ingestion.extractor import VariableExtractor
    from georiva.ingestion.models import FileIngestion


@dataclass
class IngestionContext:
    plugin: object
    clipper: BoundaryClipper
    writer: AssetWriter
    extractor: VariableExtractor

    origin_bucket: str
    reference_time: datetime | None = None
    ingestion_log: FileIngestion | None = None
