"""
The Harvest feed: a pull feed of the harvesting kind (ADR 0042 §2).

It is a `DataFeed` species, so it is still its catalog's one Feed (ADR 0034) and
its runs are `FetchRun`s on the shared run spine. What it does in a run is its
own: it copies an Upstream's index into Published rows and fetches no file.
"""

from urllib.parse import urlsplit

from django.contrib.postgres.fields import ArrayField
from django.core.exceptions import ValidationError
from django.db import models
from django.utils.text import slugify
from django.utils.translation import gettext_lazy as _
from modelcluster.fields import ParentalKey

from georiva.sources.models import DataFeed, DataFeedCollectionLink


class HarvestFeed(DataFeed):
    """An Upstream collection, a time window, a cadence (`interval_minutes`), a
    Variable mapping (`variable_mappings`) and an Asset host allowlist."""

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

    asset_hosts = ArrayField(
        models.CharField(max_length=255),
        default=list,
        blank=True,
        verbose_name=_("Additional asset hosts"),
        help_text=_(
            "Hosts asset links may name besides the upstream catalog's own. An asset on any other host is not indexed."
        ),
    )
    allow_http = models.BooleanField(
        default=False,
        verbose_name=_("Allow plain http assets"),
        help_text=_("Index assets whose link is http rather than https."),
    )

    run_task_name = "georiva.harvest.tasks.run_harvest"

    class Meta:
        verbose_name = _("Harvest feed")
        verbose_name_plural = _("Harvest feeds")

    # ── Configuration, as a harvest reads it ────────────────────────────────

    @property
    def window(self):
        return (self.window_start, self.window_end)

    @property
    def allowed_asset_hosts(self) -> set[str]:
        """The Asset host allowlist: the upstream catalog's own host, and those the operator added."""
        own = urlsplit(self.upstream_url).hostname
        return {host.lower() for host in [own, *self.asset_hosts] if host}

    def upstream_client(self):
        from .upstream import StacApiClient

        return StacApiClient(self.upstream_url)

    @property
    def collection(self):
        """The Collection this feed harvests into, or None before it has one."""
        link = self.collection_links.select_related("collection").first()
        return link.collection if link else None

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

        slug = slugify(self.upstream_collection_id)[:100] or "harvest"
        collection, _created = Collection.objects.get_or_create(
            catalog_id=self.catalog_id,
            slug=slug,
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
        help_text=_("The key of the asset in the upstream's items, exactly as the upstream spells it."),
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
