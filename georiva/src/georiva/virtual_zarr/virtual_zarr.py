"""
Core building blocks for virtual Zarr manifests over GeoRiva COG assets.

    MinioStoreConfig     — connection parameters + store factory methods
    MinioVirtualTIFF     — VirtualTIFF subclass that correctly wires AsyncS3Store
                           for MinIO (bypasses the broken obstore → async-tiff
                           conversion that panics on missing aws_ prefix)
    VirtualZarrBuilder   — scans COG headers, builds and serialises a kerchunk
                           manifest to a local path
    open_kerchunk_dataset — opens a kerchunk manifest as a lazy xarray Dataset
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from async_tiff.store import S3Store as AsyncS3Store
from obspec_utils.registry import ObjectStoreRegistry
from obstore.store import S3Store
from virtual_tiff import VirtualTIFF
from virtual_tiff.parser import _construct_manifest_group
from virtualizarr import open_virtual_dataset
from virtualizarr.manifests import ManifestStore

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class MinioStoreConfig:
    """
    Connection parameters for a MinIO instance.

    Parameters
    ----------
    bucket : str
        MinIO bucket name for COG assets (georiva-assets).
    access_key, secret_key : str
        MinIO credentials.
    internal_endpoint : str
        Endpoint reachable from inside the GeoRiva container,
        e.g. ``http://georiva-minio:9000``.
    public_endpoint : str
        Publicly reachable endpoint used for chunk URLs baked into the
        kerchunk manifest.  Defaults to ``internal_endpoint`` if not set.
    path_style : bool
        Use path-style URLs (required for MinIO). Default True.
    """
    
    bucket: str
    access_key: str
    secret_key: str
    internal_endpoint: str
    public_endpoint: str = ""
    path_style: bool = True
    
    def __post_init__(self) -> None:
        if not self.public_endpoint:
            self.public_endpoint = self.internal_endpoint
    
    @classmethod
    def from_django_settings(cls) -> "MinioStoreConfig":
        """
        Construct from Django settings.

        Expected settings keys::

            AWS_S3_ENDPOINT_URL    # internal MinIO endpoint
            AWS_ACCESS_KEY_ID
            AWS_SECRET_ACCESS_KEY
            GEORIVA_ASSETS_BUCKET  # e.g. "georiva-assets"
            MINIO_PUBLIC_ENDPOINT  # optional public CDN/proxy URL
        """
        from django.conf import settings
        
        return cls(
            bucket=settings.GEORIVA_ASSETS_BUCKET,
            access_key=settings.AWS_ACCESS_KEY_ID,
            secret_key=settings.AWS_SECRET_ACCESS_KEY,
            internal_endpoint=settings.AWS_S3_ENDPOINT_URL,
            public_endpoint=getattr(settings, "MINIO_PUBLIC_ENDPOINT", ""),
        )
    
    # ------------------------------------------------------------------
    # Store factories
    # ------------------------------------------------------------------
    
    def _build_obstore(self) -> S3Store:
        """
        obstore S3Store for the assets bucket, pointed at the internal endpoint.

        The ``prefix`` strips the bucket name from paths returned by
        ``ObjectStoreRegistry.resolve()``, so callers receive a clean
        bucket-relative key rather than ``{bucket}/{key}``.
        """
        return S3Store(
            bucket=self.bucket,
            prefix=f"{self.bucket}/",
            aws_endpoint=self.internal_endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            aws_virtual_hosted_style_request=str(not self.path_style).lower(),
            aws_allow_http="true",
        )
    
    def _build_async_store(self, bucket: str | None = None) -> AsyncS3Store:
        """
        async-tiff S3Store pointed at the internal endpoint.

        ``aws_allow_http="true"`` is required when the endpoint is plain HTTP
        (MinIO default inside the container).  Without it the Rust object_store
        crate refuses the connection immediately with a "builder error".
        """
        return AsyncS3Store(
            bucket=bucket or self.bucket,
            aws_endpoint=self.internal_endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            aws_virtual_hosted_style_request=str(not self.path_style).lower(),
            aws_allow_http="true",
        )
    
    def build_registry(self) -> ObjectStoreRegistry:
        """
        Registry that maps ``<internal_endpoint>/<bucket>/`` → obstore.

        The registry key must match the URL prefix used in ``url_for()`` so
        that ``registry.resolve(url)`` strips the right prefix and returns a
        clean bucket-relative path.
        """
        store = self._build_obstore()
        prefix = f"{self.internal_endpoint}/{self.bucket}/"
        return ObjectStoreRegistry({prefix: store})
    
    # ------------------------------------------------------------------
    # URL helpers
    # ------------------------------------------------------------------
    
    def url_for(self, path: str) -> str:
        """
        Full internal URL for a bucket-relative asset path.

        ``path`` must NOT start with the bucket name.
        """
        return f"{self.internal_endpoint}/{self.bucket}/{path}"
    
    def s3_uri_for(self, internal_url: str) -> str:
        """
        Convert an internal ``http(s)://host/bucket/key`` URL to an
        ``s3://bucket/key`` URI for embedding in the kerchunk manifest.

        fsspec resolves ``s3://`` URIs using ``remote_options`` supplied at
        open time, so the actual endpoint is injected then rather than baked
        into every chunk reference.
        """
        for prefix in (
                f"{self.internal_endpoint}/{self.bucket}/",
                f"{self.public_endpoint}/{self.bucket}/",
        ):
            if internal_url.startswith(prefix):
                return f"s3://{self.bucket}/{internal_url[len(prefix):]}"
        return internal_url  # already s3:// or unrecognised
    
    # ------------------------------------------------------------------
    # fsspec remote options (for open_kerchunk_dataset)
    # ------------------------------------------------------------------
    
    @property
    def fsspec_remote_options(self) -> dict:
        """Options for fsspec when reading chunks from *outside* the container."""
        return {
            "key": self.access_key,
            "secret": self.secret_key,
            "client_kwargs": {"endpoint_url": self.public_endpoint},
        }
    
    @property
    def fsspec_remote_options_internal(self) -> dict:
        """Options for fsspec when reading chunks from *inside* the container."""
        return {
            "key": self.access_key,
            "secret": self.secret_key,
            "client_kwargs": {"endpoint_url": self.internal_endpoint},
        }


# ---------------------------------------------------------------------------
# VirtualTIFF parser
# ---------------------------------------------------------------------------

class MinioVirtualTIFF(VirtualTIFF):
    """
    VirtualTIFF subclass that correctly wires AsyncS3Store for MinIO.

    The stock VirtualTIFF calls ``convert_obstore_to_async_tiff_store`` which
    reads ``obstore.S3Store.__getnewargs_ex__()``.  obstore strips the ``aws_``
    prefix from config keys internally, so the reconstructed kwargs arrive at
    AsyncS3Store without the required prefix → Rust panics.

    This subclass bypasses that conversion and builds AsyncS3Store directly
    from the MinioStoreConfig, which always uses the correct ``aws_`` prefix.
    """
    
    def __init__(
            self,
            config: MinioStoreConfig,
            *,
            ifd: int | None = 0,
            ifd_layout: str = "flat",
    ) -> None:
        super().__init__(ifd=ifd, ifd_layout=ifd_layout)
        self._config = config
    
    def __call__(self, url: str, registry: ObjectStoreRegistry) -> ManifestStore:
        obstore_store, path_in_store = registry.resolve(url)
        bucket = getattr(obstore_store, "bucket", self._config.bucket)
        async_store = self._config._build_async_store(bucket)
        manifest_group = _construct_manifest_group(
            url,
            store=async_store,
            path=path_in_store,
            ifd=self._ifd,
            ifd_layout=self.ifd_layout,
        )
        return ManifestStore(registry=registry, group=manifest_group)


# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------

@dataclass
class VirtualZarrBuilder:
    """
    Builds a kerchunk JSON manifest from a collection of GeoRiva COGs.

    Scans COG IFD headers (range requests only — no pixel data downloaded),
    assigns geographic coordinates from the affine geotransform, rewrites
    chunk URLs to ``s3://`` URIs, and writes the manifest to a local path.
    """
    
    config: MinioStoreConfig
    _registry: ObjectStoreRegistry = field(init=False, repr=False)
    _parser: MinioVirtualTIFF = field(init=False, repr=False)
    
    def __post_init__(self) -> None:
        self._registry = self.config.build_registry()
        self._parser = MinioVirtualTIFF(self.config)
    
    def build(
            self,
            url_df: pd.DataFrame,
            output_path: str | Path,
            variable_name: str = "data",
    ) -> Path:
        """
        Build and serialise a kerchunk manifest.

        Parameters
        ----------
        url_df : pd.DataFrame
            Columns: ``date`` (datetime-like, tz-naive UTC) and ``url`` (str).
        output_path : str or Path
            Local filesystem path for the output JSON manifest.
        variable_name : str
            Name for the data variable in the output dataset.  The raw IFD
            index variable (``"0"``) is renamed to this value.

        Returns
        -------
        Path
            Absolute path to the written manifest file.
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info("Scanning %d COG headers…", len(url_df))
        combined = self._build_virtual_dataset(url_df)
        
        logger.info("Reading geotransform from sample COG…")
        combined = self._assign_geo_coords(combined, url_df.iloc[0]["url"])
        
        raw_var = list(combined.data_vars)[0]
        if raw_var != variable_name:
            combined = combined.rename({raw_var: variable_name})
        
        logger.info("Rewriting chunk URLs to s3:// URIs…")
        combined = combined.vz.rename_paths(self.config.s3_uri_for)
        
        logger.info("Writing manifest → %s", output_path)
        combined.vz.to_kerchunk(str(output_path), format="json")
        
        return output_path
    
    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------
    
    def _build_virtual_dataset(self, url_df: pd.DataFrame) -> xr.Dataset:
        virtual_datasets = []
        for _, row in url_df.iterrows():
            vds = open_virtual_dataset(
                url=row["url"],
                registry=self._registry,
                parser=self._parser,
            )
            # Normalise to tz-naive UTC — TimescaleDB returns timezone-aware
            # datetime64[us, UTC] which numpy/xarray cannot use as a coord dtype.
            ts = pd.Timestamp(row["date"])
            if ts.tzinfo is not None:
                ts = ts.tz_convert("UTC").tz_localize(None)
            
            vds = vds.assign_coords(time=ts).expand_dims("time")
            virtual_datasets.append(vds)
        return xr.concat(virtual_datasets, dim="time")
    
    def _assign_geo_coords(self, ds: xr.Dataset, sample_url: str) -> xr.Dataset:
        """
        Read the affine geotransform from one COG and attach lat/lon coords.

        Uses a single HTTP request to read the IFD — no pixel data is fetched.
        Pixel-centre convention: offset by +0.5 pixel from the edge origin.
        """
        with rasterio.open(sample_url) as src:
            transform = src.transform
            height, width = src.height, src.width
        
        lons = transform.c + transform.a * (np.arange(width) + 0.5)
        lats = transform.f + transform.e * (np.arange(height) + 0.5)
        
        return (
            ds
            .assign_coords(lat=("y", lats), lon=("x", lons))
            .swap_dims({"y": "lat", "x": "lon"})
        )


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

def open_kerchunk_dataset(
        manifest_path: str | Path,
        config: MinioStoreConfig,
        *,
        internal: bool = True,
        chunks: dict | None = {},
) -> xr.Dataset:
    """
    Open a kerchunk manifest as a lazy xarray Dataset.

    Parameters
    ----------
    manifest_path : str or Path
        Local path to the kerchunk JSON manifest.
    config : MinioStoreConfig
        Supplies S3 credentials and the correct endpoint URL.
    internal : bool
        True  → resolve chunks via the internal container endpoint.
        False → resolve chunks via the public endpoint (external callers).
    chunks : dict or None
        Passed to ``xr.open_dataset``.
        ``{}``   = dask-backed lazy arrays.
        ``None`` = eager loading.

    Returns
    -------
    xr.Dataset
    """
    # kerchunk registers its xarray backend via an entry point that may not
    # have been scanned if the package was installed after interpreter start.
    import kerchunk.xarray_backend  # noqa: F401  registers the engine
    from xarray.backends import plugins as xr_plugins
    
    xr_plugins.list_engines.cache_clear()
    
    remote_options = (
        config.fsspec_remote_options_internal if internal
        else config.fsspec_remote_options
    )
    
    return xr.open_dataset(
        str(manifest_path),
        engine="kerchunk",
        storage_options={
            "remote_protocol": "s3",
            "remote_options": remote_options,
        },
        chunks=chunks,
    )
