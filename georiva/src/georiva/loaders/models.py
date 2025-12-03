"""
GeoRiva Loader Models

Polymorphic models for automated data loading from various sources.
Each loader type has its own configuration fields and UI.

The LoaderConfig base class is linked to a Collection via OneToOneField,
ensuring each collection can have at most one loader.
"""

from django.db import models
from django.utils.translation import gettext_lazy as _
from django_extensions.db.models import TimeStampedModel
from polymorphic.models import PolymorphicModel
from wagtail.admin.panels import FieldPanel, MultiFieldPanel


class LoaderConfig(PolymorphicModel, TimeStampedModel):
    """
    Base configuration for all loader types.
    
    Each Collection can have one LoaderConfig (or none for manual-only uploads).
    Subclasses define source-specific configuration fields.
    """
    
    collection = models.OneToOneField(
        'core.Collection',
        on_delete=models.CASCADE,
        related_name='loader',
        help_text=_("Collection this loader fetches data for"),
    )
    
    # Common fields for all loaders
    enabled = models.BooleanField(
        default=True,
        help_text=_("Enable/disable automated loading"),
    )
    schedule = models.CharField(
        max_length=100,
        blank=True,
        help_text=_("Cron expression for scheduled runs (e.g., '0 */6 * * *' for every 6 hours)"),
    )
    
    # Run tracking
    last_run_at = models.DateTimeField(null=True, blank=True)
    last_run_status = models.CharField(
        max_length=50,
        blank=True,
        choices=[
            ('success', _('Success')),
            ('partial', _('Partial Success')),
            ('failed', _('Failed')),
            ('running', _('Running')),
        ],
    )
    last_run_message = models.TextField(blank=True)
    last_successful_run_at = models.DateTimeField(null=True, blank=True)
    
    # Statistics
    total_runs = models.IntegerField(default=0)
    successful_runs = models.IntegerField(default=0)
    failed_runs = models.IntegerField(default=0)
    total_files_fetched = models.IntegerField(default=0)
    
    base_panels = [
        FieldPanel('collection'),
        FieldPanel('enabled'),
        FieldPanel('schedule'),
    ]
    
    class Meta:
        verbose_name = _("Loader Configuration")
        verbose_name_plural = _("Loader Configurations")
    
    def __str__(self):
        return f"{self.get_real_instance_class().__name__} for {self.collection}"
    
    def get_loader(self):
        """
        Returns the loader implementation instance for this config.
        Override in subclasses.
        """
        raise NotImplementedError("Subclasses must implement get_loader()")
    
    def record_run(self, status: str, message: str = "", files_fetched: int = 0):
        """Record a loader run."""
        from django.utils import timezone
        
        self.last_run_at = timezone.now()
        self.last_run_status = status
        self.last_run_message = message
        self.total_runs += 1
        self.total_files_fetched += files_fetched
        
        if status == 'success':
            self.successful_runs += 1
            self.last_successful_run_at = self.last_run_at
        elif status == 'failed':
            self.failed_runs += 1
        
        self.save(update_fields=[
            'last_run_at', 'last_run_status', 'last_run_message',
            'last_successful_run_at', 'total_runs', 'successful_runs',
            'failed_runs', 'total_files_fetched',
        ])


class FTPLoaderConfig(LoaderConfig):
    """
    Configuration for loading data from FTP/SFTP servers.
    Supports FTP, SFTP, and FTPS protocols.
    """
    
    PROTOCOL_CHOICES = [
        ('ftp', 'FTP'),
        ('sftp', 'SFTP'),
        ('ftps', 'FTPS'),
    ]
    
    protocol = models.CharField(
        max_length=10,
        choices=PROTOCOL_CHOICES,
        default='ftp',
    )
    host = models.CharField(max_length=255)
    port = models.IntegerField(default=21)
    
    # Authentication
    username = models.CharField(max_length=100, blank=True)
    password = models.CharField(
        max_length=255,
        blank=True,
        help_text=_("Password (will be encrypted)"),
    )
    private_key = models.TextField(
        blank=True,
        help_text=_("SSH private key for SFTP (optional)"),
    )
    
    # Path configuration
    remote_path = models.CharField(
        max_length=500,
        help_text=_("Remote directory path"),
    )
    filename_pattern = models.CharField(
        max_length=255,
        default="*",
        help_text=_("Filename pattern (glob or regex)"),
    )
    use_regex = models.BooleanField(
        default=False,
        help_text=_("Use regex instead of glob for filename matching"),
    )
    
    # File handling
    recursive = models.BooleanField(
        default=False,
        help_text=_("Search subdirectories recursively"),
    )
    
    # Connection settings
    timeout = models.IntegerField(
        default=30,
        help_text=_("Connection timeout in seconds"),
    )
    passive_mode = models.BooleanField(
        default=True,
        help_text=_("Use passive mode for FTP"),
    )
    
    panels = LoaderConfig.base_panels + [
        MultiFieldPanel([
            FieldPanel('protocol'),
            FieldPanel('host'),
            FieldPanel('port'),
        ], heading=_("Connection")),
        MultiFieldPanel([
            FieldPanel('username'),
            FieldPanel('password'),
            FieldPanel('private_key'),
        ], heading=_("Authentication")),
        MultiFieldPanel([
            FieldPanel('remote_path'),
            FieldPanel('filename_pattern'),
            FieldPanel('use_regex'),
            FieldPanel('recursive'),
        ], heading=_("File Selection")),
        MultiFieldPanel([
            FieldPanel('timeout'),
            FieldPanel('passive_mode'),
        ], heading=_("Options")),
    ]
    
    class Meta:
        verbose_name = _("FTP Loader")
        verbose_name_plural = _("FTP Loaders")
    
    def get_loader(self):
        from georiva.loaders.ftp import FTPLoader
        return FTPLoader(self)


class HTTPLoaderConfig(LoaderConfig):
    """
    Configuration for loading data from HTTP/HTTPS endpoints.
    
    Supports various authentication methods and can handle
    APIs that require specific headers or parameters.
    """
    
    base_url = models.URLField(
        help_text=_("Base URL for data downloads"),
    )
    
    # URL pattern for constructing download URLs
    url_pattern = models.CharField(
        max_length=500,
        blank=True,
        help_text=_("URL pattern with placeholders (e.g., '{base_url}/data/{year}/{month}/{day}.nc')"),
    )
    
    # Authentication
    AUTH_TYPES = [
        ('none', 'None'),
        ('basic', 'Basic Auth'),
        ('bearer', 'Bearer Token'),
        ('api_key', 'API Key'),
        ('custom_header', 'Custom Header'),
    ]
    auth_type = models.CharField(
        max_length=20,
        choices=AUTH_TYPES,
        default='none',
    )
    auth_username = models.CharField(max_length=100, blank=True)
    auth_password = models.CharField(max_length=255, blank=True)
    auth_token = models.CharField(
        max_length=500,
        blank=True,
        help_text=_("Bearer token or API key"),
    )
    auth_header_name = models.CharField(
        max_length=100,
        blank=True,
        default="Authorization",
        help_text=_("Custom header name for authentication"),
    )
    
    # Additional headers
    custom_headers = models.JSONField(
        default=dict,
        blank=True,
        help_text=_("Additional HTTP headers as JSON object"),
    )
    
    # Request parameters
    query_params = models.JSONField(
        default=dict,
        blank=True,
        help_text=_("Query parameters to include in requests"),
    )
    
    # Timeouts and retries
    timeout = models.IntegerField(default=60)
    max_retries = models.IntegerField(default=3)
    
    # SSL verification
    verify_ssl = models.BooleanField(
        default=True,
        help_text=_("Verify SSL certificates"),
    )
    
    panels = LoaderConfig.base_panels + [
        MultiFieldPanel([
            FieldPanel('base_url'),
            FieldPanel('url_pattern'),
        ], heading=_("URL Configuration")),
        MultiFieldPanel([
            FieldPanel('auth_type'),
            FieldPanel('auth_username'),
            FieldPanel('auth_password'),
            FieldPanel('auth_token'),
            FieldPanel('auth_header_name'),
        ], heading=_("Authentication")),
        MultiFieldPanel([
            FieldPanel('custom_headers'),
            FieldPanel('query_params'),
        ], heading=_("Request Options")),
        MultiFieldPanel([
            FieldPanel('timeout'),
            FieldPanel('max_retries'),
            FieldPanel('verify_ssl'),
        ], heading=_("Connection Settings")),
    ]
    
    class Meta:
        verbose_name = _("HTTP Loader")
        verbose_name_plural = _("HTTP Loaders")
    
    def get_loader(self):
        from georiva.loaders.http import HTTPLoader
        return HTTPLoader(self)


class S3LoaderConfig(LoaderConfig):
    """
    Configuration for loading data from S3-compatible storage.
    
    Supports AWS S3, MinIO, and other S3-compatible services.
    Useful for loading data from public buckets or partner data shares.
    """
    
    # S3 connection
    bucket_name = models.CharField(max_length=255)
    prefix = models.CharField(
        max_length=500,
        blank=True,
        help_text=_("S3 prefix (folder path)"),
    )
    
    # Endpoint (for MinIO or custom S3-compatible services)
    endpoint_url = models.URLField(
        blank=True,
        help_text=_("Custom endpoint URL (leave blank for AWS S3)"),
    )
    region = models.CharField(
        max_length=50,
        default='us-east-1',
    )
    
    # Credentials (blank for public buckets)
    access_key = models.CharField(max_length=255, blank=True)
    secret_key = models.CharField(max_length=255, blank=True)
    
    # File selection
    filename_pattern = models.CharField(
        max_length=255,
        default="*",
        help_text=_("Filename pattern (glob)"),
    )
    
    # Options
    requester_pays = models.BooleanField(
        default=False,
        help_text=_("Enable requester pays for AWS buckets"),
    )
    
    panels = LoaderConfig.base_panels + [
        MultiFieldPanel([
            FieldPanel('bucket_name'),
            FieldPanel('prefix'),
            FieldPanel('endpoint_url'),
            FieldPanel('region'),
        ], heading=_("S3 Connection")),
        MultiFieldPanel([
            FieldPanel('access_key'),
            FieldPanel('secret_key'),
        ], heading=_("Credentials")),
        MultiFieldPanel([
            FieldPanel('filename_pattern'),
            FieldPanel('requester_pays'),
        ], heading=_("Options")),
    ]
    
    class Meta:
        verbose_name = _("S3 Loader")
        verbose_name_plural = _("S3 Loaders")
    
    def get_loader(self):
        from georiva.loaders.s3 import S3Loader
        return S3Loader(self)
