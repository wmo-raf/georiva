from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.utils.translation import gettext_lazy as _
from django_extensions.db.models import TimeStampedModel
from polymorphic.models import PolymorphicModel
from wagtail.admin.panels import FieldPanel

from .registry import data_source_registry
from .widgets import DataSourceClassSelectWidget


class LoaderProfile(PolymorphicModel, TimeStampedModel):
    """
    Configuration for a data loader using DataSource .
    
    Each profile defines:
    - What data to fetch (data_source_type + data_source_config)
    - When to run (interval_minutes)
    """
    
    name = models.CharField(
        max_length=255,
        verbose_name=_("Name"),
    )
    
    data_source_type = models.CharField(
        max_length=100,
        verbose_name=_("Data Source"),
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
    
    base_panels = [
        FieldPanel('name'),
        FieldPanel('data_source_type', widget=DataSourceClassSelectWidget),
        FieldPanel('is_active'),
        FieldPanel('interval_minutes'),
    ]
    
    panels = base_panels
    
    class Meta:
        verbose_name = _("Loader Profile")
        verbose_name_plural = _("Loader Profiles")
        ordering = ['name']
    
    def __str__(self):
        return f"{self.name} - {self.get_real_instance_class().__name__}"
    
    def get_loader_config(self) -> dict:
        """Get loader configuration dictionary."""
        return {}
    
    # =========================================================================
    # Factory Methods
    # =========================================================================
    
    def get_data_source(self):
        """Instantiate configured data source."""
        source_class = data_source_registry.get_class(self.data_source_type)
        if not source_class:
            raise ValueError(f"Unknown data source: {self.data_source_type}")
        
        # Merge default config with instance config
        info = data_source_registry.get(self.data_source_type)
        loader_config = self.get_loader_config()
        config = {**info.get('default_config', {}), **loader_config}
        
        return source_class(config)
    
    def get_loader(self, collection=None):
        """Create fully configured Loader instance."""
        from .loader import Loader
        
        return Loader(
            data_source=self.get_data_source(),
            collection=collection,
            loader_profile=self,
        )
    
    # =========================================================================
    # Run Management
    # =========================================================================
    
    def record_run(self, result, collection):
        """Record loader run result."""
        from django.utils import timezone
        
        LoaderRun.objects.create(
            collection=collection,
            loader_profile=self,
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
        """Check if loader is due to run."""
        if not self.is_active:
            return False
        
        if not self.last_run_at:
            return True
        
        from django.utils import timezone
        from datetime import timedelta
        
        next_run = self.last_run_at + timedelta(minutes=self.interval_minutes)
        return timezone.now() >= next_run
    
    def run_now(self, collection, **kwargs):
        """Convenience method to run immediately."""
        loader = self.get_loader(collection)
        result = loader.run(**kwargs)
        self.record_run(result, collection)
        return result


class LoaderRun(TimeStampedModel):
    """Tracks each execution of the Loader for a Collection."""
    
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
        related_name='loader_runs',
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
