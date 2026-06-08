"""
SourceFileManager — all raw-file I/O for a single ingestion run.

Owns:
  - Streaming download from origin bucket to a local temp directory
  - Archiving the raw file to georiva-archive
  - The archive-then-delete decision once processing is complete
"""
import logging
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from georiva.core.storage import storage

if TYPE_CHECKING:
    from georiva.core.storage import Bucket
    from georiva.ingestion.result import IngestionResult

logger = logging.getLogger(__name__)


class SourceFileManager:
    """
    Handles the lifecycle of the raw source file during ingestion.
    """
    
    # =========================================================================
    # Download
    # =========================================================================
    
    @contextmanager
    def download_to_temp(self, origin: "Bucket", file_path: str):
        """
        Stream a file from *origin* to a local temporary directory.

        Yields the local Path and cleans up automatically on exit.
        Streams in 8 MB chunks to keep memory flat regardless of file
        size — important for large files.
        """
        original_name = Path(file_path).name
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir) / original_name
            
            with origin.open(file_path, "rb") as src, open(tmp_path, "wb") as dst:
                while chunk := src.read(8 * 1024 * 1024):  # 8 MB chunks
                    dst.write(chunk)
            
            yield tmp_path
    
    # =========================================================================
    # Archive
    # =========================================================================
    
    def archive(self, origin: "Bucket", file_path: str) -> Optional[str]:
        """
        Copy the raw source file to georiva-archive.

        Archive failure is non-fatal — a warning is logged but ingestion
        continues.  Returns the archive path on success, None on failure.
        """
        try:
            archived = storage.archive_raw(origin, file_path)
            logger.info(
                "Archived: %s/%s → archive/%s",
                origin.bucket_name, file_path, archived,
            )
            return archived
        except Exception as e:
            logger.warning(
                "Archive failed: %s/%s — %s",
                origin.bucket_name, file_path, e,
            )
            return None
    
    # =========================================================================
    # Cleanup
    # =========================================================================
    
    def cleanup(
            self,
            origin: "Bucket",
            file_path: str,
            catalog,
            result: "IngestionResult",
    ) -> None:
        """
        Archive and/or delete the source file based on the ingestion outcome.

        Rules:
          Full success (no partial failures)
            → archive if catalog.archive_source_files
            → delete from origin bucket

          Partial success (some variables failed)
            → keep in origin so sweep_unprocessed can retry

          No items created (complete failure)
            → keep in origin (FileIngestion already marks it failed)
        """
        has_partial_failures = any(
            "Partial failure" in e for e in result.errors
        )
        
        if result.success and not has_partial_failures:
            if catalog.archive_source_files:
                archived = self.archive(origin, file_path)
                result.archive_path = archived or ""
            origin.delete(file_path)
        
        elif result.success and has_partial_failures:
            logger.warning(
                "Partial variable failures — keeping source file "
                "for re-processing: %s",
                file_path,
            )
