from datetime import timedelta
from typing import TYPE_CHECKING

from django.core.validators import MinValueValidator
from django.db import models
from django.urls import reverse
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _
from django_extensions.db.models import TimeStampedModel
from modelcluster.fields import ParentalKey
from modelcluster.models import ClusterableModel
from polymorphic.models import PolymorphicModel
from wagtail.admin.forms import WagtailAdminModelForm
from wagtail.admin.panels import FieldPanel

from .source import BaseDataSource

if TYPE_CHECKING:
    from georiva.sources.collection_definitions import CollectionDefinition


class DataFeed(PolymorphicModel, TimeStampedModel, ClusterableModel):
    """
    A configured data source: what to fetch, how often, and with what settings.

    Each subclass pairs with one BaseDataSource implementation and holds the
    operator-supplied configuration (schedule, credentials, variable selection).
    """
    
    catalog = models.OneToOneField(
        'georivacore.Catalog',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='data_feed',
        verbose_name=_("Catalog"),
    )
    
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
        validators=[MinValueValidator(5)],
        verbose_name=_("Run Interval"),
        help_text=_("Minutes between runs (global default for all collections)"),
    )

    class TargetTier(models.TextChoices):
        PUBLISHED = 'published', _('Published (serve directly)')
        STAGING = 'staging', _('Staging (hold for derivation)')

    target_tier = models.CharField(
        max_length=20,
        choices=TargetTier.choices,
        default=TargetTier.PUBLISHED,
        verbose_name=_("Target Tier"),
        help_text=_(
            "Published: fetched files are materialized into served layers. "
            "Staging: fetched files are held as raw inputs for derivation and "
            "are not served or auto-materialized."
        ),
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
        FieldPanel('is_active'),
        FieldPanel('interval_minutes'),
        FieldPanel('target_tier'),
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
    
    @classmethod
    def get_collection_definitions(cls) -> list['CollectionDefinition']:
        """
        Return the finite list of collections this plugin can create.

        Override in subclasses to declare every CollectionDefinition the plugin
        supports. The wizard presents these as a checklist in step 3; the setup
        service provisions Collection + Variable + DataFeedCollectionLink for each
        selected definition.
        """
        return []
    
    # =========================================================================
    # Collection link helpers
    # =========================================================================
    
    @classmethod
    def get_collection_link_model(cls):
        """Return the DataFeedCollectionLink subclass for this feed type. Default: base class."""
        return DataFeedCollectionLink
    
    @classmethod
    def get_link_config_for_definition(cls, definition) -> dict:
        """
        Return extra link model fields derived from the definition (not user-configurable).

        Override in subclasses to supply per-link fields that are baked into the
        definition rather than collected from the operator.  For example, CHIRPS
        overrides this to set `period` from the definition key so operators never
        see it as an editable field.
        """
        return {}
    
    # =========================================================================
    # Factory Methods
    # =========================================================================
    
    def get_data_source(self, collection=None):
        """Instantiate configured data source, merging per-collection link config."""
        source_class = self.data_source_cls
        if not source_class:
            raise ValueError("No data source class defined for this data feed.")
        
        if not issubclass(source_class, BaseDataSource):
            raise ValueError(f"Data source class {source_class} must inherit from BaseDataSource.")
        
        config = {**self.get_loader_config()}
        if collection is not None:
            try:
                link = self.collection_links.get(collection=collection).get_real_instance()
                config.update(link.config)
            except DataFeedCollectionLink.DoesNotExist:
                pass
        return source_class(config)
    
    def get_loader(self, collection=None):
        """Create fully configured Loader instance."""
        from .loader import Loader
        
        return Loader(
            data_source=self.get_data_source(collection=collection),
            collection=collection,
            data_feed=self,
        )
    
    # =========================================================================
    # Run Management
    # =========================================================================
    
    def _update_run_stats(self, result, collection):
        """Update feed-level and collection-link scheduling stats after a run."""
        from django.utils import timezone

        now = timezone.now()
        self.collection_links.filter(collection=collection).update(last_run_at=now)

        self.last_run_at = now
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

        async_run=True  (default) — creates a LoaderJob with real-time
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
        
        collections = [collection] if collection else [
            link.collection
            for link in self.collection_links.select_related('collection')
        ]
        results = []
        for coll in collections:
            loader = self.get_loader(coll)
            result = loader.run()
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
    
    def delete(self, *args, **kwargs):
        """Delete this feed and its linked Catalog (which cascades to Collections/Variables/Items)."""
        catalog_id = self.catalog_id
        result = super().delete(*args, **kwargs)
        if catalog_id:
            from georiva.core.models import Catalog
            Catalog.objects.filter(pk=catalog_id).delete()
        return result
    
    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        
        from georiva.sources.tasks import update_collection_data_feed_periodic_task
        
        update_collection_data_feed_periodic_task(
            sender=self.__class__, instance=self, created=False
        )


class DataFeedCollectionLink(PolymorphicModel):
    """
    Through model for the DataFeed ↔ Collection M2M.

    Subclasses (e.g. CHIRPSDataFeedCollectionLink) add plugin-specific fields
    (e.g. period) that the Loader merges into the DataSource config at runtime
    via the `config` property.

    Scheduling:
      - interval_minutes: per-collection override; null means use DataFeed.interval_minutes
      - last_run_at: updated after each successful collection run for is_due() checks
    """
    data_feed = ParentalKey(DataFeed, on_delete=models.CASCADE, related_name='collection_links')
    collection = models.ForeignKey('georivacore.Collection', on_delete=models.CASCADE, related_name='feed_links')
    
    definition_key = models.CharField(
        max_length=100,
        blank=True,
        help_text="The CollectionDefinition.key used to provision this link.",
    )
    
    # Per-collection scheduling (overrides DataFeed.interval_minutes when set)
    interval_minutes = models.PositiveIntegerField(
        null=True,
        blank=True,
        validators=[MinValueValidator(5)],
        verbose_name=_("Collection Run Interval"),
        help_text=_("Minutes between runs for this collection. Leave blank to use the feed's global interval."),
    )
    last_run_at = models.DateTimeField(null=True, blank=True)
    
    class Meta:
        unique_together = ['data_feed', 'collection']
        ordering = ['id']
    
    @property
    def effective_interval(self) -> int:
        """Per-collection interval if set, else the feed's global interval."""
        return self.interval_minutes or self.data_feed.interval_minutes
    
    def is_due(self) -> bool:
        """Return True if this collection is due to run."""
        if not self.data_feed.is_active:
            return False
        if not self.last_run_at:
            return True
        from django.utils import timezone
        next_run = self.last_run_at + timedelta(minutes=self.effective_interval)
        return timezone.now() >= next_run
    
    @property
    def config(self) -> dict:
        """Per-collection config dict merged into the DataSource config at runtime."""
        return {}
    
    def __str__(self):
        return f"{self.data_feed_id} → {self.collection_id}"
    
    # -------------------------------------------------------------------------
    # UI / form discovery
    # -------------------------------------------------------------------------
    
    @classmethod
    def get_panels(cls) -> list:
        """
        Wagtail panels for the per-collection config fields of this link type.
        Override in subclasses to expose typed fields in the admin form.
        """
        return []
    
    def has_configurable_fields(self) -> bool:
        """Return True if there are any operator-editable config fields on this link."""
        return bool(self.get_panels())
    
    @classmethod
    def get_form_class(cls):
        """
        Return a ModelForm for editing this link's configurable fields.

        Always includes interval_minutes (the per-collection schedule override) plus
        any subclass-specific fields declared in get_panels().
        Returns None only when get_panels() is empty (no plugin-specific config).
        """
        from django.forms import modelform_factory
        from wagtail.admin.panels import FieldPanel
        
        panel_fields = [
            p.field_name for p in cls.get_panels()
            if isinstance(p, FieldPanel)
        ]
        
        if not panel_fields:
            return None
        
        base_form_class = getattr(cls, 'base_form_class', WagtailAdminModelForm)
        
        all_fields = panel_fields + ['interval_minutes']
        return modelform_factory(cls, form=base_form_class, fields=all_fields)


# ---------------------------------------------------------------------------
# Acquisition tracking models
# ---------------------------------------------------------------------------

class FetchRun(models.Model):
    """Tracks one fetch run of a DataFeed — the acquisition phase before ingestion."""

    class Status(models.TextChoices):
        RUNNING = 'running', 'Running'
        COMPLETED = 'completed', 'Completed'
        FAILED = 'failed', 'Failed'
        CANCELLED = 'cancelled', 'Cancelled'

    data_feed = models.ForeignKey(
        DataFeed,
        on_delete=models.CASCADE,
        related_name='fetch_runs',
    )
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.RUNNING)
    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    files_requested = models.IntegerField(default=0)
    files_fetched = models.IntegerField(default=0)
    files_skipped = models.IntegerField(default=0)
    files_failed = models.IntegerField(default=0)
    bytes_transferred = models.BigIntegerField(default=0)
    error_message = models.TextField(blank=True)

    class Meta:
        ordering = ['-started_at']

    def _finish(self, status, **update_fields):
        from django.utils import timezone
        self.status = status
        self.finished_at = timezone.now()
        for k, v in update_fields.items():
            setattr(self, k, v)
        fields = ['status', 'finished_at'] + list(update_fields.keys())
        self.save(update_fields=fields)

    def mark_completed(self, files_fetched=0, files_skipped=0, files_failed=0, bytes_transferred=0):
        self._finish(
            self.Status.COMPLETED,
            files_fetched=files_fetched,
            files_skipped=files_skipped,
            files_failed=files_failed,
            bytes_transferred=bytes_transferred,
        )

    def mark_failed(self, error=''):
        self._finish(self.Status.FAILED, error_message=error)

    def mark_cancelled(self):
        self._finish(self.Status.CANCELLED)


class FetchedFile(models.Model):
    """Per-file record within a FetchRun."""

    class Status(models.TextChoices):
        PENDING = 'pending', 'Pending'
        FETCHING = 'fetching', 'Fetching'
        STORED = 'stored', 'Stored'
        SKIPPED = 'skipped', 'Skipped'
        FAILED = 'failed', 'Failed'

    fetch_run = models.ForeignKey(
        FetchRun,
        on_delete=models.CASCADE,
        related_name='fetched_files',
    )
    file_path = models.CharField(max_length=500)
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.PENDING)
    skip_reason = models.CharField(max_length=255, blank=True)
    error = models.TextField(blank=True)
    bytes_transferred = models.BigIntegerField(default=0)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ['id']

    def mark_fetching(self):
        from django.utils import timezone
        self.status = self.Status.FETCHING
        self.started_at = timezone.now()
        self.save(update_fields=['status', 'started_at'])

    def mark_stored(self, bytes_transferred=0):
        from django.utils import timezone
        self.status = self.Status.STORED
        self.bytes_transferred = bytes_transferred
        self.completed_at = timezone.now()
        self.save(update_fields=['status', 'bytes_transferred', 'completed_at'])

    def mark_skipped(self, reason=''):
        self.status = self.Status.SKIPPED
        self.skip_reason = reason
        self.save(update_fields=['status', 'skip_reason'])

    def mark_failed(self, error=''):
        from django.utils import timezone
        self.status = self.Status.FAILED
        self.error = error
        self.completed_at = timezone.now()
        self.save(update_fields=['status', 'error', 'completed_at'])


# ---------------------------------------------------------------------------
# Task-ferry Job model
# ---------------------------------------------------------------------------

