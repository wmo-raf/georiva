"""
IngestionContext — shared state for a single file ingestion run.

Constructed once in IngestionService.process_file() and passed to every
handler.  Avoids threading 8+ arguments through nested call stacks.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from georiva.ingestion.asset_writer import AssetWriter
    from georiva.ingestion.clipper import BoundaryClipper
    from georiva.ingestion.encoder import VariableEncoder
    from georiva.ingestion.extractor import VariableExtractor
    from georiva.ingestion.models import IngestionLog


@dataclass
class IngestionContext:
    """
    Immutable bag of shared objects for one file ingestion run.

    All handlers receive a single IngestionContext instance instead of
    individual constructor/method arguments for plugin, clipper, writer, etc.
    """

    # --- Processing objects (constructed once per file) ----------------------
    plugin: object
    clipper: "BoundaryClipper"
    writer: "AssetWriter"
    extractor: "VariableExtractor"
    encoder: "VariableEncoder"

    # --- Run metadata --------------------------------------------------------
    origin_bucket: str
    reference_time: Optional[datetime] = None
    ingestion_log: Optional["IngestionLog"] = None
