from datetime import timedelta

from django.db import models
from django.utils import timezone
from django_extensions.db.models import TimeStampedModel
from wagtail.snippets.models import register_snippet


@register_snippet
class VirtualZarrManifest(TimeStampedModel):
    """
    Tracks the build state of one kerchunk manifest per Variable.

    One manifest per variable.

    The manifest covers the full available time axis — every COG Asset with
    format=COG for this variable.  It is rebuilt from scratch whenever marked
    stale (debounced via sweep_virtual_zarr_pending).
    """
    
    LOCK_TIMEOUT = timedelta(minutes=30)
    
    class Status(models.TextChoices):
        PENDING = "pending", "Pending build"
        BUILDING = "building", "Building"
        READY = "ready", "Ready"
        STALE = "stale", "Stale"
        FAILED = "failed", "Failed"
    
    # -------------------------------------------------------------------------
    # Identity — one manifest per variable
    # -------------------------------------------------------------------------
    
    variable = models.OneToOneField(
        "georivacore.Variable",
        on_delete=models.CASCADE,
        related_name="virtual_zarr_manifest",
    )
    
    # -------------------------------------------------------------------------
    # Storage
    # -------------------------------------------------------------------------
    
    manifest_path = models.CharField(
        max_length=500,
        blank=True,
        help_text=(
            "MinIO key for the kerchunk JSON, relative to the georiva-assets bucket. "
            "e.g. __manifests__/chirps/chirps-monthly/precipitation.json"
        ),
    )
    
    # -------------------------------------------------------------------------
    # Coverage (populated on successful build)
    # -------------------------------------------------------------------------
    
    time_start = models.DateTimeField(null=True, blank=True, editable=False)
    time_end = models.DateTimeField(null=True, blank=True, editable=False)
    item_count = models.PositiveIntegerField(default=0, editable=False)
    
    # -------------------------------------------------------------------------
    # State machine
    # -------------------------------------------------------------------------
    
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING,
        db_index=True,
    )
    built_at = models.DateTimeField(null=True, blank=True, editable=False)
    locked_at = models.DateTimeField(null=True, blank=True)
    locked_by = models.CharField(max_length=100, blank=True, default="")
    error = models.TextField(blank=True)
    
    class Meta:
        # ordering by collection then variable slug for readable admin lists
        ordering = ["variable__collection", "variable__slug"]
        indexes = [
            models.Index(
                fields=["status"],
                name="idx_vzm_status",
            ),
        ]
    
    def __str__(self):
        col = self.variable.collection
        return (
            f"{col.catalog.slug}/{col.slug}"
            f"/{self.variable.slug} [{self.status}]"
        )
    
    # -------------------------------------------------------------------------
    # Collection convenience property
    # -------------------------------------------------------------------------
    
    @property
    def collection(self):
        """Convenience accessor — always self.variable.collection."""
        return self.variable.collection
    
    # -------------------------------------------------------------------------
    # Derived path
    # -------------------------------------------------------------------------
    
    @classmethod
    def make_manifest_path(cls, variable: "Variable") -> str:
        """
        Canonical MinIO key for a manifest, derived from the variable's
        collection and catalog.
        """
        collection = variable.collection
        return f"{collection.catalog.slug}/{collection.slug}/{variable.slug}.json"
    
    def get_manifest_path(self) -> str:
        """Return (or derive) the manifest path for this record."""
        if self.manifest_path:
            return self.manifest_path
        return self.make_manifest_path(self.variable)
    
    # -------------------------------------------------------------------------
    # State transitions
    # -------------------------------------------------------------------------
    
    def mark_building(self, worker_id: str = "") -> None:
        self.__class__.objects.filter(pk=self.pk).update(
            status=self.Status.BUILDING,
            locked_at=timezone.now(),
            locked_by=worker_id,
            error="",
        )
    
    def mark_ready(
            self,
            manifest_path: str,
            item_count: int,
            time_start,
            time_end,
    ) -> None:
        self.__class__.objects.filter(pk=self.pk).update(
            status=self.Status.READY,
            manifest_path=manifest_path,
            item_count=item_count,
            time_start=time_start,
            time_end=time_end,
            built_at=timezone.now(),
            locked_at=None,
            locked_by="",
            error="",
        )
    
    def mark_failed(self, error: str) -> None:
        self.__class__.objects.filter(pk=self.pk).update(
            status=self.Status.FAILED,
            locked_at=None,
            locked_by="",
            error=error[:2000],
        )
    
    def mark_stale(self) -> None:
        """
        Mark as stale when new COG assets are added.

        Only transitions READY → STALE.  A manifest already PENDING, BUILDING,
        or FAILED is left untouched — it will be rebuilt by the next sweep
        anyway.
        """
        self.__class__.objects.filter(
            pk=self.pk,
            status=self.Status.READY,
        ).update(status=self.Status.STALE)
    
    # -------------------------------------------------------------------------
    # Bulk helpers
    # -------------------------------------------------------------------------
    
    @classmethod
    def get_buildable(cls):
        """
        Return manifests that need building: PENDING, STALE, or retryable FAILED.

        Excludes any currently locked (BUILDING with a fresh lock) to avoid
        duplicate dispatches when the sweep runs concurrently with a build task.
        """
        stale_cutoff = timezone.now() - cls.LOCK_TIMEOUT
        return cls.objects.filter(
            status__in=[
                cls.Status.PENDING,
                cls.Status.STALE,
                cls.Status.FAILED,
            ]
        ).exclude(
            status=cls.Status.BUILDING,
            locked_at__gte=stale_cutoff,
        ).select_related(
            "variable",
            "variable__collection",
            "variable__collection__catalog",
        )
    
    @classmethod
    def reset_stale_locks(cls) -> int:
        """
        Reset BUILDING records whose locks have expired (worker crash recovery).
        Resets to PENDING so the next sweep re-dispatches them.
        """
        stale_cutoff = timezone.now() - cls.LOCK_TIMEOUT
        return cls.objects.filter(
            status=cls.Status.BUILDING,
            locked_at__lt=stale_cutoff,
        ).update(
            status=cls.Status.PENDING,
            locked_at=None,
            locked_by="",
        )
    
    # -------------------------------------------------------------------------
    # Dataset access
    # -------------------------------------------------------------------------
    
    def open_dataset(self, *, internal: bool = True, chunks: dict | None = {}):
        """
        Open this manifest as a lazy xarray Dataset.

        Parameters
        ----------
        internal : bool
            True  → use the internal container endpoint (georiva-minio:9000).
            False → use the public endpoint (for external callers).
        chunks : dict or None
            Passed to xr.open_dataset.  {} = dask-backed lazy, None = eager.

        Raises
        ------
        ValueError
            If the manifest is not in READY state or has no manifest_path.
        """
        if self.status != self.Status.READY:
            raise ValueError(
                f"Manifest {self} is not ready (status={self.status}). "
                "Trigger a build first."
            )
        if not self.manifest_path:
            raise ValueError(f"Manifest {self} has no manifest_path recorded.")
        
        from georiva.virtual_zarr.virtual_zarr import (
            MinioStoreConfig,
            open_kerchunk_dataset,
        )
        
        config = MinioStoreConfig.from_django_settings()
        
        # The manifest is stored in MinIO — download it to a temp file so
        # xarray/kerchunk can read it as a local JSON path.
        import tempfile
        from georiva.core.storage import storage
        
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name
        
        manifest_bytes = storage.zarr.read_bytes(self.manifest_path)
        with open(tmp_path, "wb") as f:
            f.write(manifest_bytes)
        
        return open_kerchunk_dataset(tmp_path, config, internal=internal, chunks=chunks)
