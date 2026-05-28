import logging
from datetime import datetime
from typing import Optional

import pytz
from django.db.models import Prefetch

from georiva.core.filename import parse_path
from georiva.core.models import Catalog, Collection, Variable
from georiva.core.storage import storage, BucketType
from georiva.formats.registry import format_registry
from georiva.ingestion.asset_writer import AssetWriter
from georiva.ingestion.clipper import BoundaryClipper
from georiva.ingestion.encoder import VariableEncoder
from georiva.ingestion.extractor import VariableExtractor
from georiva.ingestion.handlers import (
    IngestionContext,
    IngestionHandler,
    SourceFileManager,
)
from georiva.ingestion.models import IngestionLog
from georiva.ingestion.result import IngestionResult

logger = logging.getLogger(__name__)


# =============================================================================
# Ingestion Service
# =============================================================================

class IngestionService:
    """
    Orchestrates the full ingestion pipeline for geospatial data files.

    Operates across GeoRiva's multi-bucket storage architecture:

        georiva-incoming  ──┐
                            ├──► process ──► georiva-assets  (COG + PNG + JSON)
        georiva-sources   ──┘
                            │
                            └──► georiva-archive             (raw copy, optional)

    Processing Flow:
    ─────────────────
    1.  Parse the file path → extract catalog slug, collection slug, reference time
    2.  Resolve the Catalog from the database (must exist and be active)
    3.  Resolve Collection(s):
          - If collection slug present in path → process that one collection
          - If absent → process ALL active collections under the catalog
    4.  Look up the format plugin (GRIB, NetCDF, GeoTIFF etc)
    5.  Initialize the boundary clipper if the catalog has a spatial boundary
    6.  Build an IngestionContext (shared processing objects, instantiated once)
    7.  Download the source file once to a local temp directory
    8.  For each collection × timestamp, delegate to IngestionHandler which:
          a. Computes the spatial clip window
          b. Gets or creates one Item for the collection + timestamp
          c. Runs each Variable through AssetHandler (extract → encode → write)
          d. Expands the Collection's temporal + spatial extent
    9.  Archive raw file to georiva-archive (if configured)
    10. Delete from origin bucket (only if fully successful)

    Partial Failure Behaviour:
    ──────────────────────────
    If some variables fail but others succeed, the Item and successful
    Assets are kept. The source file is NOT deleted so it can be
    re-processed. This is intentional — a partial ingest is better than
    losing data silently.
    """
    
    def __init__(self):
        self.logger = logging.getLogger("georiva.ingestion")
        self._source_file_manager = SourceFileManager()
    
    # =========================================================================
    # Main Entry Point
    # =========================================================================
    
    def process_file(
            self,
            file_path: str,
            origin_bucket: str = BucketType.INCOMING,
            reference_time: datetime = None,
    ) -> IngestionResult:
        """
        Process a single incoming geospatial file end-to-end.

        The catalog and collection are resolved entirely from the file path.
        Reference time is extracted from the GR-- filename prefix if present,
        or can be supplied explicitly (e.g. from a Celery task argument).

        Archive and cleanup behaviour is driven by the Catalog model:
            catalog.archive_source_files = True  →  copy to georiva-archive
            Successful, fully-complete runs       →  delete from origin bucket
            Partial variable failures             →  keep in origin for retry

        Args:
            file_path:       Path relative to the bucket root.
            origin_bucket:   Which bucket the file came from.
            reference_time:  Explicit reference time (overrides GR-- prefix).

        Returns:
            IngestionResult summarising what was created and any errors.
        """
        origin = storage.bucket(origin_bucket)
        self.logger.info("Processing: %s/%s", origin.bucket_name, file_path)
        
        # ── Path parsing ──────────────────────────────────────────────────────
        path_meta = parse_path(file_path)
        catalog_slug = path_meta.get("catalog")
        collection_slug = path_meta.get("collection")
        
        if reference_time is None:
            reference_time = path_meta.get("reference_time")
        
        result = IngestionResult(
            origin_file=file_path,
            origin_bucket=origin_bucket,
            catalog_slug=catalog_slug or "",
            collection_slug=collection_slug or "",
            success=False,
            timestamp=datetime.now(pytz.utc),
        )
        
        try:
            # ── Catalog resolution ────────────────────────────────────────────
            if not catalog_slug:
                result.add_error(f"Cannot determine catalog from path: {file_path}")
                return result
            
            try:
                catalog = Catalog.objects.select_related("boundary").get(
                    slug=catalog_slug, is_active=True
                )
            except Catalog.DoesNotExist:
                result.add_error(f"Catalog not found or inactive: {catalog_slug}")
                return result
            
            # ── Collection resolution ─────────────────────────────────────────
            collections = self._resolve_collections(catalog, collection_slug)
            
            if not collections:
                if collection_slug:
                    result.add_error(
                        f"Collection not found or inactive: "
                        f"{catalog_slug}/{collection_slug}"
                    )
                else:
                    result.add_error(
                        f"No active collections found for catalog: {catalog_slug}"
                    )
                return result
            
            self.logger.info(
                "Processing against %d collection(s): %s",
                len(collections),
                ", ".join(c.slug for c in collections),
            )
            
            # ── Format plugin ─────────────────────────────────────────────────
            plugin = format_registry.get(catalog.file_format)
            if not plugin:
                result.add_error(f"No format plugin for: {catalog.file_format}")
                return result
            
            # ── Boundary clipper ──────────────────────────────────────────────
            clipper = BoundaryClipper(
                boundary=catalog.boundary if catalog.clip_mode != "none" else None,
                apply_mask=(catalog.clip_mode == "mask"),
            )
            if clipper.is_active:
                result.clipped = True
                result.clip_boundary = str(catalog.boundary)
                self.logger.info("Clipping enabled: %s", catalog.boundary)
            
            # ── IngestionLog reference ────────────────────────────────────────
            ingestion_log = IngestionLog.objects.filter(
                bucket=origin_bucket, file_path=file_path
            ).first()
            
            # ── Build shared context + handler ────────────────────────────────
            ctx = IngestionContext(
                plugin=plugin,
                clipper=clipper,
                writer=AssetWriter(storage.assets),
                extractor=VariableExtractor(plugin),
                encoder=VariableEncoder(),
                origin_bucket=origin_bucket,
                reference_time=reference_time,
                ingestion_log=ingestion_log,
            )
            handler = IngestionHandler(ctx)
            
            # ── Process ───────────────────────────────────────────────────────
            try:
                sfm = self._source_file_manager
                with sfm.download_to_temp(origin, file_path) as local_path:
                    for collection in collections:
                        first_variable_name = self._get_first_variable_name(collection)
                        if not first_variable_name:
                            result.add_error(
                                f"No active variables for: {collection.slug}"
                            )
                            continue
                        
                        timestamps = plugin.get_timestamps(
                            local_path, first_variable_name
                        )
                        if not timestamps:
                            result.add_error(
                                f"No timestamps found in: {file_path}"
                            )
                            continue
                        
                        self.logger.info(
                            "Found %d timestamp(s) for %s",
                            len(timestamps), collection.slug,
                        )
                        result.collection_slug = collection.slug
                        
                        for ts in timestamps:
                            try:
                                item, assets, clip_info, failed_vars = (
                                    handler.process_timestamp(
                                        collection=collection,
                                        local_path=local_path,
                                        timestamp=ts,
                                        source_file=f"{origin_bucket}:{file_path}",
                                    )
                                )
                                
                                if failed_vars:
                                    result.add_error(
                                        f"Partial failure for {collection.slug} "
                                        f"@ {ts}: variables failed: "
                                        f"{', '.join(failed_vars)}"
                                    )
                                
                                if item is None:
                                    continue
                                
                                result.items_created.append(str(item.pk))
                                result.assets_created.extend(
                                    [str(a.pk) for a in assets]
                                )
                                
                                if clip_info and result.original_size is None:
                                    result.original_size = clip_info.get("original_size")
                                    result.clipped_size = clip_info.get("clipped_size")
                            
                            except Exception as e:
                                result.add_error(
                                    f"Failed {collection.slug} @ {ts}: {e}"
                                )
                
                result.success = len(result.items_created) > 0
            
            finally:
                if hasattr(plugin, "clear_cache"):
                    plugin.clear_cache()
            
            # ── Archive + cleanup ─────────────────────────────────────────────
            self._source_file_manager.cleanup(origin, file_path, catalog, result)
            
            if result.clipped and result.size_reduction_percent:
                self.logger.info(
                    "Clipping reduced data size by %.1f%%",
                    result.size_reduction_percent,
                )
        
        except Exception as e:
            self.logger.exception("Ingestion failed: %s", file_path)
            result.add_error(str(e))
        
        return result
    
    # =========================================================================
    # Collection Resolution
    # =========================================================================
    
    def _resolve_collections(
            self,
            catalog: Catalog,
            collection_slug: str = None,
    ) -> list[Collection]:
        """
        Resolve which collections to process for a given catalog.

        Variables and their sources are prefetched here to avoid N+1 queries
        later when iterating over variables per timestamp.

        Returns a single-element list if collection_slug is given,
        or all active collections if not.
        """
        base_qs = Collection.objects.select_related("catalog").prefetch_related(
            Prefetch(
                "variables",
                queryset=Variable.objects.select_related(
                    "source_unit",
                    "unit",
                    "palette",
                ).order_by("sort_order"),
            )
        )
        
        if collection_slug:
            try:
                return [
                    base_qs.get(
                        catalog=catalog,
                        slug=collection_slug,
                        is_active=True,
                    )
                ]
            except Collection.DoesNotExist:
                return []
        
        return list(base_qs.filter(catalog=catalog, is_active=True))
    
    def _get_first_variable_name(self, collection: Collection) -> Optional[str]:
        for variable in collection.variables.all():
            if not variable.is_active:
                continue
            if variable.sources:
                return variable.sources[0].value['source_name']
        return None
