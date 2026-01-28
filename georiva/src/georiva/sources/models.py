from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.utils.translation import gettext_lazy as _
from django_extensions.db.models import TimeStampedModel
from polymorphic.models import PolymorphicModel
from wagtail.admin.panels import FieldPanel

from .registry import data_source_registry, fetch_strategy_registry
from .widgets import DataSourceClassSelectWidget, FetchStrategyClassSelectWidget


class LoaderProfile(PolymorphicModel, TimeStampedModel):
    """
    Configuration for a data loader combining DataSource + FetchStrategy.
    
    Each profile defines:
    - What data to fetch (data_source_type + data_source_config)
    - How to fetch it (fetch_strategy_type + fetch_strategy_config)
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
    
    fetch_strategy_type = models.CharField(
        max_length=100,
        verbose_name=_("Fetch Strategy"),
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
        FieldPanel('fetch_strategy_type', widget=FetchStrategyClassSelectWidget),
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
        config = {**info.get('default_config', {}), **self.data_source_config}
        
        return source_class(config)
    
    def get_fetch_strategy(self):
        """Instantiate configured fetch strategy."""
        strategy_class = fetch_strategy_registry.get_class(self.fetch_strategy_type)
        if not strategy_class:
            raise ValueError(f"Unknown fetch strategy: {self.fetch_strategy_type}")
        
        info = fetch_strategy_registry.get(self.fetch_strategy_type)
        config = {**info.get('default_config', {}), **self.fetch_strategy_config}
        
        return strategy_class(config)
    
    def get_loader(self, collection=None):
        """Create fully configured Loader instance."""
        from .loader import Loader
        
        # Determine collection
        if collection is None:
            if self.catalog and self.catalog.collections.exists():
                collection = self.catalog.collections.first()
            else:
                raise ValueError("No collection available for this profile")
        
        return Loader(
            data_source=self.get_data_source(),
            fetch_strategy=self.get_fetch_strategy(),
            collection=collection,
        )
    
    # =========================================================================
    # Run Management
    # =========================================================================
    
    def record_run(self, result):
        """Record loader run result."""
        from django.utils import timezone
        
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
    
    def run_now(self, **kwargs):
        """Convenience method to run immediately."""
        loader = self.get_loader()
        result = loader.run(**kwargs)
        self.record_run(result)
        return result
