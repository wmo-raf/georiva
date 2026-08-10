from datetime import timedelta

from django.db import models
from django.utils import timezone
from django_extensions.db.models import TimeStampedModel
from wagtail.snippets.models import register_snippet


@register_snippet
class VirtualZarrManifest(TimeStampedModel):
    """
    Tracks the build state of one Icechunk repository per Variable.

    One repo per variable, holding only virtual references to COG bytes.

    The repo covers the full available time axis — every COG Asset with
    format=COG for this variable.  New items strictly after the committed
    time_end are appended; anything earlier/overlapping triggers a full
    rebuild as a new commit in place (never a prefix delete), so readers
    pinned to a prior snapshot keep working.

    The Django fields (repo_path, watermark, snapshot_id, coverage) are
    derived caches — the source of truth for what the repo contains is the
    latest Icechunk commit's metadata (design decision 5).
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
    
    ORGANISATION_LOOKUP = "variable__collection__catalog__organisation"

    variable = models.OneToOneField(
        "georivacore.Variable",
        on_delete=models.CASCADE,
        related_name="virtual_zarr_manifest",
    )
    
    # -------------------------------------------------------------------------
    # Storage
    # -------------------------------------------------------------------------
    
    repo_path = models.CharField(
        max_length=500,
        blank=True,
        help_text=(
            "Icechunk repository prefix on the zarr bucket, org slug first. "
            "e.g. kenya/chirps/chirps-monthly/precipitation/"
        ),
    )

    # -------------------------------------------------------------------------
    # Coverage (derived cache — populated on successful build)
    # -------------------------------------------------------------------------

    time_start = models.DateTimeField(null=True, blank=True, editable=False)
    time_end = models.DateTimeField(null=True, blank=True, editable=False)
    item_count = models.PositiveIntegerField(default=0, editable=False)
    watermark = models.DateTimeField(
        null=True, blank=True, editable=False,
        help_text="Max Asset.modified at the last successful commit.",
    )
    snapshot_id = models.CharField(
        max_length=100, blank=True, default="", editable=False,
        help_text="Icechunk snapshot id of the last successful commit.",
    )
    
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
    def make_repo_path(cls, variable: "Variable") -> str:
        """
        Canonical Icechunk repo prefix on the zarr bucket, derived from the
        variable's collection and catalog — org slug first, like every other
        bucket key.  Trailing slash: this names a prefix, not an object.
        """
        collection = variable.collection
        return (
            f"{collection.catalog.storage_prefix}/"
            f"{collection.slug}/{variable.slug}/"
        )

    def get_repo_path(self) -> str:
        """Return (or derive) the repo prefix for this record."""
        if self.repo_path:
            return self.repo_path
        return self.make_repo_path(self.variable)
    
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
            repo_path: str,
            item_count: int,
            time_start,
            time_end,
            snapshot_id: str = "",
            watermark=None,
    ) -> None:
        self.__class__.objects.filter(pk=self.pk).update(
            status=self.Status.READY,
            repo_path=repo_path,
            item_count=item_count,
            time_start=time_start,
            time_end=time_end,
            snapshot_id=snapshot_id,
            watermark=watermark,
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
        Open this variable's Icechunk repo as a lazy xarray Dataset.

        A read-only session is opened at the tip of ``main``; the container
        endpoint/credentials for the virtual COG chunks are supplied here at
        open time (late binding), not baked into the committed refs.

        Parameters
        ----------
        internal : bool
            True  → use the internal container endpoint (georiva-minio:9000).
            False → use the public endpoint (for external callers).
        chunks : dict or None
            Passed to xr.open_zarr.  {} = dask-backed lazy, None = eager.

        Raises
        ------
        ValueError
            If the manifest is not in READY state.
        """
        if self.status != self.Status.READY:
            raise ValueError(
                f"Manifest {self} is not ready (status={self.status}). "
                "Trigger a build first."
            )

        import xarray as xr

        from georiva.virtual_zarr.repo import open_repo

        repo = open_repo(self.get_repo_path(), internal=internal)
        session = repo.readonly_session(branch="main")
        return xr.open_zarr(session.store, consolidated=False, chunks=chunks)
