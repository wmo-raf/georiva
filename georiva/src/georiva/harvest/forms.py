"""
The Harvest feed's admin form: what an operator may save (ADR 0042 §2–3, §9).

Saving is where a feed is checked against the world it names: its Upstream must
answer without credentials and have the collection, its catalog must have no
other feed, and every row of its Variable mapping must name a Variable of the
collection it harvests into.
"""

from copy import deepcopy

from django.utils.translation import gettext_lazy as _

from georiva.sources.models import DataFeed, DataFeedForm

from .upstream import UpstreamUnreachable

#: The inline panel's relation: the rows of the Variable mapping.
MAPPINGS = "variable_mappings"

#: How many of an Upstream's collection ids an error offers as what was meant.
SUGGESTED_COLLECTIONS = 10


def _is_name(name: str) -> bool:
    """Whether `name` is a host's or a bucket's name alone: no scheme, port, path, credentials or wildcard."""
    return not any(char in name for char in "/:@*") and not any(char.isspace() for char in name)


class HarvestFeedForm(DataFeedForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.instance.pk:
            # Its collection, and every row harvested so far, is in this catalog.
            self.fields["catalog"].disabled = True
            self._offer_variables_of(self.instance.collection)

    def _offer_variables_of(self, collection):
        """Offer only `collection`'s Variables in the mapping's rows, the ones
        there and the one "add another" clones."""
        formset = self.formsets.get(MAPPINGS)
        if formset is None or collection is None:
            return
        for row in formset.forms:
            row.fields["variable"].queryset = row.fields["variable"].queryset.filter(collection=collection)
        # The clone is built from the class, which every request shares: narrow a copy.
        row_class = formset.form
        narrowed = type(row_class)(row_class.__name__, (row_class,), {})
        fields = {name: deepcopy(field) for name, field in row_class.base_fields.items()}
        fields["variable"].queryset = fields["variable"].queryset.filter(collection=collection)
        narrowed.base_fields = fields
        formset.form = narrowed

    # ── Fields ──────────────────────────────────────────────────────────────

    def clean_catalog(self):
        """A catalog has one feed (ADR 0034). Its push feed is refused by the model."""
        catalog = self.cleaned_data.get("catalog")
        if catalog is None:
            return catalog
        other = DataFeed.objects.non_polymorphic().filter(catalog=catalog).exclude(pk=self.instance.pk).first()
        if other:
            self.add_error(
                "catalog",
                _("This catalog already has a pull feed, “%(name)s”. A catalog has at most one feed.")
                % {"name": other.name},
            )
        return catalog

    def clean_asset_hosts(self):
        """Host names, as an asset link's host is compared: whole and lower case,
        with no scheme, port, path or wildcard."""
        hosts = []
        for raw in self.cleaned_data.get("asset_hosts") or []:
            host = raw.strip().lower().rstrip(".")
            if not host:
                continue
            if not _is_name(host):
                self.add_error(
                    "asset_hosts",
                    _("“%(host)s” is not a host name. Give the name alone, such as data.example.org.") % {"host": raw},
                )
                continue
            if host not in hosts:
                hosts.append(host)
        return hosts

    def clean_pinned_buckets(self):
        """Bucket → host, both as they are compared: a bucket's bare name, and a
        host name held to what an asset host is."""
        pins = self.cleaned_data.get("pinned_buckets") or {}
        if not isinstance(pins, dict):
            self.add_error("pinned_buckets", _('Give the pinned buckets as {"bucket": "host"}.'))
            return {}
        cleaned = {}
        for raw_bucket, raw_host in pins.items():
            bucket = str(raw_bucket).strip().lower()
            host = raw_host.strip().lower().rstrip(".") if isinstance(raw_host, str) else ""
            if not bucket or not _is_name(bucket) or not host or not _is_name(host):
                self.add_error(
                    "pinned_buckets",
                    _("“%(bucket)s” must be a bucket's name, pinned to a host name such as data.example.org.")
                    % {"bucket": raw_bucket},
                )
                continue
            cleaned[bucket] = host
        return cleaned

    # ── The feed as a whole ─────────────────────────────────────────────────

    def clean(self):
        cleaned = super().clean()
        start, end = cleaned.get("window_start"), cleaned.get("window_end")
        if start and end and end < start:
            self.add_error("window_end", _("The window ends before it starts."))
        self._check_upstream(cleaned)
        self._check_mapping(cleaned)
        return cleaned

    def _check_upstream(self, cleaned):
        """Refuse an Upstream that cannot be read without credentials, or that
        has no such collection (ADR 0042 §2).

        Asked when the feed is created and when what it names upstream changes:
        a feed whose Upstream is down for the day can still be paused or remapped.
        """
        url, collection_id = cleaned.get("upstream_url"), cleaned.get("upstream_collection_id")
        if not url or not collection_id:
            return
        if self.instance.pk and not {"upstream_url", "upstream_collection_id"} & set(self.changed_data):
            return

        # Through the feed's own client, so this asks what a harvest will ask.
        probe = type(self.instance)(upstream_url=url, upstream_collection_id=collection_id)
        try:
            found = [collection.id for collection in probe.upstream_client().list_collections()]
        except UpstreamUnreachable as unreachable:
            self.add_error(
                "upstream_url",
                _(
                    "This upstream catalog could not be read without credentials: %(why)s. "
                    "A harvest feed needs an upstream that answers anonymously."
                )
                % {"why": unreachable},
            )
            return

        if collection_id not in found:
            message = _("The upstream catalog has no collection “%(id)s”.") % {"id": collection_id}
            if found:
                offered = ", ".join(sorted(found)[:SUGGESTED_COLLECTIONS])
                more = "…" if len(found) > SUGGESTED_COLLECTIONS else ""
                message = _("%(message)s It has: %(offered)s%(more)s") % {
                    "message": message,
                    "offered": offered,
                    "more": more,
                }
            self.add_error("upstream_collection_id", message)

    def _harvested_collection(self, cleaned):
        """The collection this feed harvests into, as `ensure_collection` will
        find it: its own, or for a feed not saved yet, the one already in its
        catalog under the upstream collection's name. None when there is none yet."""
        from georiva.core.models import Collection

        if self.instance.pk:
            return self.instance.collection
        catalog, collection_id = cleaned.get("catalog"), cleaned.get("upstream_collection_id")
        if catalog is None or not collection_id:
            return None
        slug = type(self.instance)(upstream_collection_id=collection_id).collection_slug
        return Collection.objects.filter(catalog=catalog, slug=slug).first()

    def _check_mapping(self, cleaned):
        """Every row maps to a Variable of the collection this feed harvests into
        (ADR 0042 §3). That no key or Variable is mapped twice is the rows' own check."""
        formset = self.formsets.get(MAPPINGS)
        if formset is None:
            return
        formset.is_valid()
        collection = self._harvested_collection(cleaned)
        for row in formset.forms:
            row_cleaned = getattr(row, "cleaned_data", None) or {}
            variable = row_cleaned.get("variable")
            if variable is None or row_cleaned.get("DELETE"):
                continue
            if collection is None:
                row.add_error(
                    "variable",
                    _(
                        "This feed has no collection yet. Save it without a mapping, add Variables "
                        "to the collection it creates, then map them here."
                    ),
                )
            elif variable.collection_id != collection.pk:
                row.add_error("variable", _("This Variable does not belong to the feed's collection."))

    # ── Saving ──────────────────────────────────────────────────────────────

    def save(self, commit=True):
        feed = super().save(commit=commit)
        if commit:
            # The collection is the feed's setup, not the Upstream's content: it is
            # there from the first save, so its Variables can be curated and mapped.
            feed.ensure_collection()
        return feed
