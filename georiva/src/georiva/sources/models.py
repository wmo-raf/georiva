from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.urls import reverse
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _
from django_extensions.db.models import TimeStampedModel
from polymorphic.models import PolymorphicModel
from wagtail.admin.panels import FieldPanel

from .source import BaseDataSource


class DataFeed(PolymorphicModel, TimeStampedModel):
    """
    A configured data source: what to fetch, how often, and with what settings.

    Each subclass pairs with one BaseDataSource implementation and holds the
    operator-supplied configuration (schedule, credentials, variable selection).
    """
    
    name = models.CharField(
        max_length=255,
        verbose_name=_("Name"),
    )
    
    is_active = models.BooleanField(
        default=True,
        verbose_name=_("Active"),
    )
    
    interval_minutes = models.PositiveIntegerField(
        default=360,  # 6 hours
        validators=[MinValueValidator(5), MaxValueValidator(1440)],
        verbose_name=_("Run Interval"),
        help_text=_("Minutes between runs"),
    )
    
    # Run tracking
    last_run_at = models.DateTimeField(null=True, blank=True)
    last_run_status = models.CharField(
        max_length=20,
        blank=True,
        choices=[
            ('success', _('Success')),
            ('partial', _('Partial')),
            ('failed', _('Failed')),
            ('queued', _('Queued')),
            ('running', _('Running')),
            ('empty', _('No Data')),
        ],
    )
    last_run_message = models.TextField(blank=True)
    last_success_at = models.DateTimeField(null=True, blank=True)
    
    # Statistics
    total_runs = models.PositiveIntegerField(default=0)
    total_files_fetched = models.PositiveIntegerField(default=0)
    total_bytes_transferred = models.BigIntegerField(default=0)
    
    # Collections this feed populates (owned by DataFeed, not Collection)
    collections = models.ManyToManyField(
        "georivacore.Collection",
        blank=True,
        related_name="data_feeds",
        verbose_name=_("Collections"),
        help_text=_("Collections this feed populates"),
    )
    
    setup_via_wizard = models.BooleanField(
        default=False,
        verbose_name=_("Set up via wizard"),
    )
    
    base_panels = [
        FieldPanel('name'),
        FieldPanel('is_active'),
        FieldPanel('interval_minutes'),
        FieldPanel('collections'),
    ]
    
    panels = base_panels
    
    class Meta:
        verbose_name = _("Data Feed")
        verbose_name_plural = _("Data Feeds")
        ordering = ['name']
    
    def __str__(self):
        return f"{self.name} - {self.get_real_instance_class().__name__}"
    
    def get_loader_config(self) -> dict:
        """Get loader configuration dictionary."""
        return {}
    
    @property
    def data_source_cls(self):
        return None
    
    @classmethod
    def get_wizard_defaults(cls) -> dict:
        """
        Field values applied when the setup wizard creates a new instance.

        Override in subclasses to supply required fields or sensible starting
        values. Only needs to cover fields that have no model-level default.
        """
        return {}
    
    @classmethod
    def get_catalog_defaults(cls) -> dict:
        """
        Suggested Catalog field values pre-filled on the wizard catalog step.

        Keys correspond to Catalog model fields: file_format, description, etc.
        Override in subclasses so the wizard pre-selects the right format and
        provides a meaningful description without the operator having to know it.
        """
        return {}
    
    # =========================================================================
    # Factory Methods
    # =========================================================================
    
    def get_data_source(self):
        """Instantiate configured data source."""
        source_class = self.data_source_cls
        if not source_class:
            raise ValueError("No data source class defined for this data feed.")
        
        if not issubclass(source_class, BaseDataSource):
            raise ValueError(f"Data source class {source_class} must inherit from BaseDataSource.")
        
        config = {**self.get_loader_config()}
        return source_class(config)
    
    def get_loader(self, collection=None):
        """Create fully configured Loader instance."""
        from .loader import Loader
        
        return Loader(
            data_source=self.get_data_source(),
            collection=collection,
            data_feed=self,
        )
    
    # =========================================================================
    # Run Management
    # =========================================================================
    
    def record_run(self, result, collection):
        """Record data feed run result."""
        from django.utils import timezone
        from georiva.core.storage import BucketType
        from georiva.ingestion.models import IngestionLog
        
        data_feed_run = DataFeedRun.objects.create(
            collection=collection,
            data_feed=self,
            started_at=result.started_at,
            finished_at=result.finished_at,
            status=result.status,
            files_requested=result.files_requested,
            files_fetched=result.files_fetched,
            files_skipped=result.files_skipped,
            files_failed=result.files_failed,
            files_queued=result.files_queued,
            bytes_transferred=result.bytes_transferred,
            run_time=result.run_time,
            errors=result.errors,
        )
        
        if result.stored_paths:
            IngestionLog.objects.filter(
                file_path__in=result.stored_paths,
                bucket=BucketType.SOURCES,
                data_feed_run__isnull=True,
            ).update(data_feed_run=data_feed_run)
        
        self.last_run_at = timezone.now()
        self.last_run_status = result.status
        self.last_run_message = '; '.join(result.errors[:3]) if result.errors else ''
        self.total_runs += 1
        self.total_files_fetched += result.files_fetched
        self.total_bytes_transferred += result.bytes_transferred
        
        if result.success:
            self.last_success_at = self.last_run_at
        
        self.save(update_fields=[
            'last_run_at', 'last_run_status', 'last_run_message',
            'last_success_at', 'total_runs', 'total_files_fetched',
            'total_bytes_transferred',
        ])
    
    def is_due(self) -> bool:
        """Check if data feed is due to run."""
        if not self.is_active:
            return False
        
        if not self.last_run_at:
            return True
        
        from django.utils import timezone
        from datetime import timedelta
        
        next_run = self.last_run_at + timedelta(minutes=self.interval_minutes)
        return timezone.now() >= next_run
    
    def run_now(self, collection=None, *, user=None, async_run: bool = True):
        """
        Dispatch a loader run.

        collection — if given, run only for that collection; if None, run for
                     all collections linked to this feed.

        async_run=True  (default) — creates a DataFeedJob with real-time
                         progress tracking.  Returns the Job instance.

        async_run=False — runs synchronously; useful for management commands
                          and tests.  Returns a list of LoaderRunResults.
        """
        if async_run:
            from task_ferry.handler import JobHandler

            return JobHandler.create_and_start(
                user=user,
                job_type_name="data_source_load",
                data_feed_id=self.pk,
                collection_id=collection.pk if collection else None,
            )

        collections = [collection] if collection else list(self.collections.all())
        results = []
        for coll in collections:
            loader = self.get_loader(coll)
            result = loader.run()
            self.record_run(result, coll)
            results.append(result)
        return results
    
    @cached_property
    def viewset(self):
        from .registry import data_feed_viewset_registry
        model_name = self.get_real_instance_class().__name__.lower()
        return data_feed_viewset_registry.get(model_name)
    
    @property
    def edit_url(self):
        if self.viewset:
            return reverse(self.viewset.get_url_name("edit"), kwargs={"pk": self.pk})
        return None
    
    @property
    def delete_url(self):
        if self.viewset:
            return reverse(self.viewset.get_url_name("delete"), kwargs={"pk": self.pk})
        return None
    
    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        
        from georiva.core.tasks import update_collection_data_feed_periodic_task
        
        update_collection_data_feed_periodic_task(
            sender=self.__class__, instance=self, created=False
        )


class DataFeedRun(TimeStampedModel):
    """Tracks each execution of the DataFeed for a Collection."""
    
    class Status(models.TextChoices):
        RUNNING = 'running', 'Running'
        SUCCESS = 'success', 'Success'
        PARTIAL = 'partial', 'Partial'
        FAILED = 'failed', 'Failed'
        EMPTY = 'empty', 'Empty'
        QUEUED = 'queued', 'Queued'
    
    collection = models.ForeignKey(
        'georivacore.Collection',
        on_delete=models.CASCADE,
        related_name='data_feed_runs',
    )
    
    data_feed = models.ForeignKey(
        'georivasources.DataFeed',
        on_delete=models.SET_NULL,
        null=True,
        related_name='runs',
    )
    # Timing
    started_at = models.DateTimeField()
    finished_at = models.DateTimeField(null=True, blank=True)
    
    # Status
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.RUNNING)
    
    # Counts
    files_requested = models.IntegerField(default=0)
    files_fetched = models.IntegerField(default=0)
    files_skipped = models.IntegerField(default=0)
    files_failed = models.IntegerField(default=0)
    files_queued = models.IntegerField(default=0)
    bytes_transferred = models.BigIntegerField(default=0)
    
    # Context
    run_time = models.DateTimeField(null=True, blank=True)  # forecast reference time
    
    # Errors (last 50)
    errors = models.JSONField(default=list, blank=True)
    
    class Meta:
        ordering = ['-started_at']
        indexes = [
            models.Index(fields=['collection', '-started_at']),
            models.Index(fields=['status']),
        ]
    
    def __str__(self):
        return f"{self.collection} | {self.started_at:%Y-%m-%d %H:%M} | {self.status}"
    
    @property
    def duration_seconds(self):
        if self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None


# ---------------------------------------------------------------------------
# Task-ferry Job model
# ---------------------------------------------------------------------------

from task_ferry.models import Job  # noqa: E402


class DataFeedJob(Job):
    """
    Operator-visible record for a single Loader.run() execution.

    Progress is available in real-time via GET /api/jobs/<id>/
    A companion DataFeedRun is created by DataFeed.record_run() at the
    end of the run — it holds the aggregate statistics.
    """
    
    data_feed = models.ForeignKey(
        DataFeed,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="jobs",
    )
    collection = models.ForeignKey(
        "georivacore.Collection",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="data_feed_jobs",
    )
    
    # Live counters updated as each file completes — readable from the API
    # before the DataFeedRun aggregate is written.
    files_total = models.IntegerField(default=0)
    files_fetched = models.IntegerField(default=0)
    files_skipped = models.IntegerField(default=0)
    files_failed = models.IntegerField(default=0)
    bytes_transferred = models.BigIntegerField(default=0)
    
    class Meta:
        app_label = "georivasources"
