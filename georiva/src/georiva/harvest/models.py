"""
The Harvest feed: a pull feed of the harvesting kind (ADR 0042 §2).

It is a `DataFeed` species, so it is still its catalog's one Feed (ADR 0034) and
its runs are `FetchRun`s on the shared run spine. What it does in a run is its
own: it copies an Upstream's index into Published rows and fetches no file.
"""

from datetime import timedelta
from urllib.parse import urlsplit

from django.contrib.postgres.fields import ArrayField
from django.core.exceptions import ValidationError
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.utils import timezone
from django.utils.text import slugify
from django.utils.translation import gettext_lazy as _
from modelcluster.fields import ParentalKey
from wagtail.admin.panels import FieldPanel, InlinePanel, MultiFieldPanel

from georiva.sources.models import DataFeed, DataFeedCollectionLink

from .forms import HarvestFeedForm

#: How many days of runs a forecast store's feed keeps unless told otherwise (ADR 0044 §7).
DEFAULT_ROLLING_WINDOW_DAYS = 3

#: The longest Rolling window a feed may be given: a year of runs.
MAX_ROLLING_WINDOW_DAYS = 366


class UpstreamShape(models.TextChoices):
    """The shape of Upstream collection a feed was created for. The values are
    the Upstream survey's own, for the two shapes it goes on from."""

    ITEMS = "one-cog-per-time", _("One image file per time step")
    STORE = "map-shaped-store", _("One data store, laid out a whole map per time step")


class HarvestFeed(DataFeed):
    """An Upstream collection, a time window, a cadence (`interval_minutes`), a
    Variable mapping (`variable_mappings`) and an Asset host allowlist.

    The collection is items of COGs, or an Upstream store (ADR 0044 §1): which,
    the feed records as `upstream_shape`, and it is fixed from then on, like
    the Upstream itself."""

    upstream_url = models.URLField(
        max_length=500,
        verbose_name=_("Upstream catalog"),
        help_text=_("Root of the upstream STAC API. It must be readable without credentials."),
    )
    upstream_collection_id = models.CharField(
        max_length=255,
        verbose_name=_("Upstream collection"),
        help_text=_("The id of the upstream collection this feed harvests."),
    )

    upstream_shape = models.CharField(
        max_length=40,
        choices=UpstreamShape.choices,
        default=UpstreamShape.ITEMS,
        verbose_name=_("How the upstream collection is published"),
        help_text=_("What the feed was set up for. Another shape is another feed."),
    )
    store_href = models.URLField(
        max_length=500,
        blank=True,
        verbose_name=_("Upstream store"),
        help_text=_(
            "For a feed over an upstream store: the store's https address, as the upstream published it when "
            "the feed was set up or last re-checked."
        ),
    )

    window_start = models.DateTimeField(
        null=True,
        blank=True,
        help_text=_("Only upstream items at or after this time are indexed. Blank: from the upstream's beginning."),
    )
    window_end = models.DateTimeField(
        null=True,
        blank=True,
        help_text=_("Only upstream items at or before this time are indexed. Blank: no end, the window stays open."),
    )

    rolling_window_days = models.PositiveIntegerField(
        null=True,
        blank=True,
        validators=[MinValueValidator(1), MaxValueValidator(MAX_ROLLING_WINDOW_DAYS)],
        verbose_name=_("Keep runs from the last (days)"),
        help_text=_(
            "For a forecast store: only runs started in the last this many days are indexed, and older ones are "
            "dropped as time passes. A run inside the window keeps all its steps, however far ahead they reach."
        ),
    )

    asset_hosts = ArrayField(
        models.CharField(max_length=255),
        default=list,
        blank=True,
        verbose_name=_("Additional asset hosts"),
        help_text=_(
            "Hosts asset links may name besides the upstream catalog's own. An asset on any other host is not indexed."
        ),
    )
    pinned_buckets = models.JSONField(
        default=dict,
        blank=True,
        verbose_name=_("Pinned buckets"),
        help_text=_(
            "For an upstream that names its files by s3://bucket/…: each bucket and the public https host its "
            'files are read at, as {"bucket": "bucket.s3.region.amazonaws.com"}. A file in any other '
            "bucket is not indexed."
        ),
    )
    pinned_chunk_locations = models.JSONField(
        default=dict,
        blank=True,
        verbose_name=_("Pinned chunk locations"),
        help_text=_(
            "For an upstream store whose data lives in its producer's files: each s3:// or gs:// place an "
            "operator confirmed, exactly as the store declares it, with its region where it has one, as "
            '{"s3://bucket/": "us-east-1", "gs://bucket/": ""}. Data anywhere else is not read.'
        ),
    )
    surveyed_assets = models.JSONField(
        default=list,
        blank=True,
        verbose_name=_("Surveyed assets"),
        help_text=_(
            "The raster asset keys the Upstream survey last saw — for an upstream store, the paths of the "
            'arrays it offered — each as {"key": …, "title": …}.'
        ),
    )
    allow_http = models.BooleanField(
        default=False,
        verbose_name=_("Allow plain http assets"),
        help_text=_("Index assets whose link is http rather than https."),
    )

    run_task_name = "georiva.harvest.tasks.run_harvest"

    # Created by the Harvest Feed Wizard and nowhere else (ADR 0042 §2).
    setup_url_name = "harvest_wizard_step1"

    # A harvest writes rows, not objects: its runs have no files to count or list.
    fetches_files = False
    setup_panel_template = "georivaharvest/includes/feed_setup_panel.html"

    base_form_class = HarvestFeedForm

    panels = [
        *DataFeed.base_panels,
        MultiFieldPanel(
            # Fixed once the feed exists: another catalog, collection or shape is another feed.
            [
                FieldPanel("upstream_url", read_only=True),
                FieldPanel("upstream_collection_id", read_only=True),
                FieldPanel("upstream_shape", read_only=True),
            ],
            heading=_("Upstream"),
        ),
        MultiFieldPanel(
            # The form keeps the fixed window or the Rolling one, whichever the feed has.
            [FieldPanel("window_start"), FieldPanel("window_end"), FieldPanel("rolling_window_days")],
            heading=_("Time window"),
        ),
        InlinePanel(
            "variable_mappings",
            heading=_("Variable mapping"),
            label=_("Mapped asset key"),
            help_text=_(
                "Only mapped asset keys — for an upstream store, mapped arrays — are indexed. Variables are "
                "curated in the feed's collection."
            ),
            panels=[FieldPanel("asset_key"), FieldPanel("variable")],
        ),
        # Where its files are read from is not edited here: it is confirmed, on
        # the feed's page, from what a re-check of the Upstream proposes.
    ]

    class Meta:
        verbose_name = _("Harvest feed")
        verbose_name_plural = _("Harvest feeds")

    # ── Configuration, as a harvest reads it ────────────────────────────────

    @property
    def is_store(self) -> bool:
        """Whether this feed points at an Upstream store rather than at items of COGs."""
        return self.upstream_shape == UpstreamShape.STORE

    @property
    def window(self):
        """The fixed window, or — for a forecast store — the Rolling window as
        it stands now: runs started in the last `rolling_window_days` days."""
        if self.rolling_window_days:
            return (timezone.now() - timedelta(days=self.rolling_window_days), None)
        return (self.window_start, self.window_end)

    @property
    def allowed_asset_hosts(self) -> set[str]:
        """The Asset host allowlist: the upstream catalog's own host, those the
        operator added, and those its pinned buckets are read at."""
        own = urlsplit(self.upstream_url).hostname
        return {host.lower() for host in [own, *self.asset_hosts, *self.pinned_buckets.values()] if host}

    def upstream_client(self):
        from .upstream import StacApiClient

        return StacApiClient(self.upstream_url)

    def survey_client(self):
        """The client an Upstream survey reads through: it looks at one small page of a listing."""
        from .survey import SAMPLE_ITEMS
        from .upstream import StacApiClient

        return StacApiClient(self.upstream_url, page_size=SAMPLE_ITEMS)

    @property
    def surveyed_asset_keys(self) -> list[tuple[str, str]]:
        """`(key, label)` for each raster asset key the Upstream survey last saw."""
        found = []
        for asset in self.surveyed_assets or []:
            key = str(asset.get("key") or "") if isinstance(asset, dict) else ""
            if key:
                title = str(asset.get("title") or "")
                found.append((key, f"{key} — {title}" if title and title != key else key))
        return found

    @property
    def recheck_url(self):
        from django.urls import reverse

        return reverse("harvest_recheck_upstream", kwargs={"pk": self.pk}) if self.pk else None

    @property
    def collection(self):
        """The Collection this feed harvests into, or None before it has one."""
        link = self.collection_links.select_related("collection").first()
        return link.collection if link else None

    @property
    def collection_edit_url(self):
        """Where the feed's collection, and with it its Variables, is curated."""
        from django.urls import reverse

        from georiva.core.views.viewsets import CollectionViewSet

        collection = self.collection
        if collection is None:
            return None
        return reverse(CollectionViewSet().get_url_name("edit"), kwargs={"pk": collection.pk})

    @property
    def sorted_asset_hosts(self) -> list[str]:
        return sorted(self.allowed_asset_hosts)

    @property
    def mapping_rows(self):
        """The Variable mapping's rows with their Variables, for display."""
        return list(self.variable_mappings.select_related("variable"))

    @property
    def collection_slug(self) -> str:
        """The slug of the collection this feed harvests into when it has none yet:
        one already in its catalog under this slug is taken, else one is created."""
        return slugify(self.upstream_collection_id)[:100] or "harvest"

    def ensure_collection(self):
        """The Collection this feed harvests into, created in the feed's catalog —
        with the feed's Provenance — if it has none yet.

        Named after the upstream collection id until a harvest has read the
        Upstream's own title and description for it.
        """
        from georiva.core.models import Collection

        collection = self.collection
        if collection is not None:
            return collection
        if self.catalog_id is None:
            raise ValueError(f"Harvest feed {self.pk} has no catalog to harvest into.")

        collection, _created = Collection.objects.get_or_create(
            catalog_id=self.catalog_id,
            slug=self.collection_slug,
            defaults={"name": self.upstream_collection_id[:200], "harvested_by": self},
        )
        DataFeedCollectionLink.objects.get_or_create(data_feed=self, collection=collection)
        return collection

    def mapped_variables(self, collection) -> dict:
        """The Variable mapping as upstream asset key → Variable, for `collection`.

        A mapping to a Variable of some other collection maps nothing: an asset
        cannot sit on one collection's item and carry another's Variable.
        """
        mappings = self.variable_mappings.select_related("variable")
        return {m.asset_key: m.variable for m in mappings if m.variable.collection_id == collection.pk}

    # ── Runs ────────────────────────────────────────────────────────────────

    def run_now(self, collection=None, *, user=None, async_run: bool = True):
        """Harvest now. A harvest is not a LoaderJob: there are no files to show
        progress for, and it does not belong on the ingestion queue (ADR 0042 §11)."""
        if async_run:
            from .tasks import run_harvest

            return run_harvest.delay(self.pk, scheduled=False)

        from .harvester import harvest

        return [harvest(self)]

    def enqueue_retry(self, run):
        from .tasks import run_harvest

        run_harvest.delay(self.pk, resume_of_run_id=run.pk)


class VariableMapping(models.Model):
    """One row of a Harvest feed's Variable mapping: an upstream asset key and
    the curated Variable its assets are indexed under. An upstream key with no
    row is not indexed (ADR 0042 §3)."""

    ORGANISATION_LOOKUP = "feed__catalog__organisation"

    feed = ParentalKey(HarvestFeed, on_delete=models.CASCADE, related_name="variable_mappings")
    asset_key = models.CharField(
        max_length=255,
        verbose_name=_("Upstream asset key"),
        help_text=_(
            "The key of the asset in the upstream's items, exactly as the upstream spells it. For an upstream "
            "store: the array's full path in the store."
        ),
    )
    variable = models.ForeignKey(
        "georivacore.Variable",
        on_delete=models.CASCADE,
        related_name="+",
        help_text=_("The Variable this asset is indexed under. It must belong to the feed's collection."),
    )

    class Meta:
        ordering = ["id"]
        constraints = [
            models.UniqueConstraint(fields=["feed", "asset_key"], name="unique_asset_key_per_harvest_feed"),
            # An item holds one asset per Variable, so two keys cannot share one.
            models.UniqueConstraint(fields=["feed", "variable"], name="unique_variable_per_harvest_feed"),
        ]

    def __str__(self):
        return f"{self.asset_key} → {self.variable_id}"

    def clean(self):
        super().clean()
        if not self.feed_id or not self.variable_id:
            return
        collection = self.feed.collection
        if collection is not None and self.variable.collection_id != collection.pk:
            raise ValidationError({"variable": _("This Variable does not belong to the feed's collection.")})
