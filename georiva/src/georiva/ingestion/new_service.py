# georiva/ingestion/service.py

from .clipper import BoundaryClipper

class IngestionService:
    
    def _process_timestamp(
        self,
        collection: 'Collection',
        plugin,
        local_path: Path,
        timestamp: datetime,
        source_file: str,
        reference_time: datetime = None,
    ) -> tuple['Item', list['Asset']]:
        """Process all Variables for a single timestamp."""
        from georiva.core.models import Item
        
        self.logger.info(f"Processing {collection} @ {timestamp}")
        
        # Initialize clipper from Catalog's boundary
        catalog = collection.catalog
        clipper = BoundaryClipper(
            boundary=catalog.boundary,
            apply_mask=True  # Set False for bbox-only clipping
        )
        
        if clipper.is_active:
            self.logger.info(f"Clipping to boundary: {catalog.boundary}")
        
        extractor = VariableExtractor(plugin)
        encoder = VariableEncoder()
        writer = AssetWriter(self.storage)
        
        variables = list(
            collection.variables.filter(is_active=True).prefetch_related('sources')
        )
        
        if not variables:
            raise ValueError(f"Collection '{collection.slug}' has no active variables")
        
        # Get source metadata
        first_var = variables[0]
        meta = extractor.get_metadata(first_var, local_path, timestamp)
        src_width, src_height = meta['width'], meta['height']
        src_bounds = tuple(meta['bounds'])
        
        # Compute clip window
        clip_window = None
        if clipper.is_active:
            try:
                clip_window = clipper.compute_window(src_bounds, src_width, src_height)
                width = clip_window['width']
                height = clip_window['height']
                bounds = clip_window['bounds']
                self.logger.info(
                    f"Clipping: {src_width}x{src_height} → {width}x{height} "
                    f"({100 * (1 - (width * height) / (src_width * src_height)):.1f}% reduction)"
                )
            except ValueError as e:
                self.logger.warning(f"Clip window failed: {e}, using full extent")
                width, height, bounds = src_width, src_height, src_bounds
        else:
            width, height, bounds = src_width, src_height, src_bounds
        
        crs = meta.get('crs', collection.crs or 'EPSG:4326')
        
        # Ensure UTC
        ts_utc = self._ensure_utc(timestamp)
        ref_utc = self._ensure_utc(reference_time) if reference_time else None
        
        # Get or create Item
        item, created = Item.objects.get_or_create(
            collection=collection,
            time=ts_utc,
            reference_time=ref_utc,
            defaults={
                'source_file': source_file,
                'bounds': list(bounds),
                'width': width,
                'height': height,
                'resolution_x': abs((bounds[2] - bounds[0]) / width) if width else 0,
                'resolution_y': abs((bounds[3] - bounds[1]) / height) if height else 0,
                'crs': crs,
            }
        )
        
        if not created:
            self.logger.info(f"Item already exists, updating assets")
            if item.source_file != source_file:
                item.source_file = source_file
                item.save(update_fields=['source_file'])
        
        # Process each Variable
        assets = []
        for variable in variables:
            try:
                variable_assets = self._process_variable(
                    item=item,
                    variable=variable,
                    extractor=extractor,
                    encoder=encoder,
                    writer=writer,
                    local_path=local_path,
                    timestamp=timestamp,
                    bounds=bounds,
                    crs=crs,
                    width=width,
                    height=height,
                    clipper=clipper,
                    clip_window=clip_window,
                )
                assets.extend(variable_assets)
            except Exception as e:
                self.logger.error(f"Variable {variable.slug} failed: {e}")
        
        self._update_collection_extent(collection, ts_utc, bounds)
        self.logger.info(f"Created Item {item.pk} with {len(assets)} assets")
        
        return item, assets
    
    def _process_variable(
        self,
        item: 'Item',
        variable: 'Variable',
        extractor: VariableExtractor,
        encoder: VariableEncoder,
        writer: AssetWriter,
        local_path: Path,
        timestamp: datetime,
        bounds: tuple,
        crs: str,
        width: int,
        height: int,
        clipper: BoundaryClipper = None,
        clip_window: dict = None,
    ) -> list['Asset']:
        """Process a single Variable with optional clipping."""
        from georiva.core.models import Asset
        
        self.logger.debug(f"Processing variable: {variable.slug}")
        
        # Compute stats (on clipped region if applicable)
        stats = extractor.compute_stats(variable, local_path, timestamp, window=clip_window)
        
        # Extract data
        if clip_window:
            window = (
                clip_window['x_off'],
                clip_window['y_off'],
                clip_window['width'],
                clip_window['height']
            )
            final_data = extractor.extract(variable, local_path, timestamp, window)
        else:
            final_data = extractor.extract(variable, local_path, timestamp)
        
        # Apply unit conversion
        final_data = apply_unit_conversion(final_data, variable.unit_conversion)
        
        # Apply geometry mask to data
        if clipper and clipper.is_active:
            final_data = clipper.apply_geometry_mask(final_data, bounds, nodata=np.nan)
        
        # Encode to RGBA
        final_rgba = encoder.encode_to_rgba(final_data, variable, stats)
        
        # Apply geometry mask to RGBA (transparent outside boundary)
        if clipper and clipper.is_active:
            final_rgba = clipper.apply_rgba_mask(final_rgba, bounds)
        
        # Generate output paths
        catalog_slug = item.collection.catalog.slug
        collection_slug = item.collection.slug
        date_path = timestamp.strftime('%Y/%m/%d')
        time_str = timestamp.strftime('%H%M%S')
        base_dir = f"processed/{catalog_slug}/{collection_slug}/{variable.slug}/{date_path}"
        base_name = f"{variable.slug}_{time_str}"
        
        assets = []
        
        # Save PNG
        png_path = f"{base_dir}/{base_name}.png"
        try:
            stored_png = writer.write_png(final_rgba, png_path)
            
            visual_asset, _ = Asset.objects.update_or_create(
                item=item,
                variable=variable,
                format=Asset.Format.PNG,
                defaults={
                    'href': stored_png,
                    'media_type': 'image/png',
                    'roles': ['visual'],
                    'file_size': self._get_file_size(stored_png),
                    'width': width,
                    'height': height,
                    'bands': 4,
                    'stats_min': stats.get('min'),
                    'stats_max': stats.get('max'),
                    'stats_mean': stats.get('mean'),
                    'stats_std': stats.get('std'),
                    'extra_fields': {
                        'imageUnscale': [
                            variable.value_min if variable.value_min is not None else stats.get('min'),
                            variable.value_max if variable.value_max is not None else stats.get('max'),
                        ],
                        'scale': variable.scale_type or 'linear',
                    },
                }
            )
            assets.append(visual_asset)
        except Exception as e:
            self.logger.error(f"PNG save failed for {variable.slug}: {e}")
        
        # Save COG
        cog_path = f"{base_dir}/{base_name}.tif"
        try:
            stored_cog = writer.write_cog(final_data, cog_path, bounds, crs)
            
            data_asset, _ = Asset.objects.update_or_create(
                item=item,
                variable=variable,
                format=Asset.Format.COG,
                defaults={
                    'href': stored_cog,
                    'media_type': 'image/tiff; application=geotiff; profile=cloud-optimized',
                    'roles': ['data'],
                    'file_size': self._get_file_size(stored_cog),
                    'width': width,
                    'height': height,
                    'bands': 1,
                    'stats_min': stats.get('min'),
                    'stats_max': stats.get('max'),
                    'stats_mean': stats.get('mean'),
                    'stats_std': stats.get('std'),
                    'extra_fields': {
                        'compression': 'deflate',
                        'nodata': np.nan,
                    },
                }
            )
            assets.append(data_asset)
        except Exception as e:
            self.logger.error(f"COG save failed for {variable.slug}: {e}")
        
        # Cleanup
        del final_data, final_rgba
        gc.collect()
        
        return assets