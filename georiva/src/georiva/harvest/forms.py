"""
The Harvest feed's admin form: what an operator may change once a feed exists (ADR 0042 §2–3).

A harvest feed is created by the Harvest Feed Wizard and nowhere else, so this
form only edits: the feed's name, cadence and window, and its Variable mapping.
Its Upstream, upstream collection and the shape it was set up for are fixed —
another one is another feed — and where its files are read from is confirmed
on the feed's page, from what a re-check of the Upstream proposes, never typed
here.

A feed over a forecast store has a Rolling window in place of the fixed one
(ADR 0044 §7), and its mapping's rows name arrays of the store by their path.
"""

from copy import deepcopy

from django import forms
from django.utils.translation import gettext_lazy as _

from georiva.sources.models import DataFeed, DataFeedForm

#: The inline panel's relation: the rows of the Variable mapping.
MAPPINGS = "variable_mappings"


class HarvestFeedForm(DataFeedForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._keep_one_window()
        if self.instance.pk:
            # Its collection, and every row harvested so far, is in this catalog.
            self.fields["catalog"].disabled = True
            self._narrow_mapping_rows(self.instance.collection)

    def _keep_one_window(self):
        """A feed has the Rolling window or the fixed one, never both: which,
        was settled when it was set up. The other's fields leave the form, and
        their panels with them."""
        if self.instance.rolling_window_days:
            for fixed in ("window_start", "window_end"):
                self.fields.pop(fixed, None)
            if "rolling_window_days" in self.fields:
                # Emptied, the feed would silently fall back to a fixed window that it never had.
                self.fields["rolling_window_days"].required = True
        else:
            self.fields.pop("rolling_window_days", None)

    def _narrow_mapping_rows(self, collection):
        """Offer, in the mapping's rows — the ones there and the one "add
        another" clones — only this collection's Variables, and only the asset
        keys the Upstream survey saw."""
        formset = self.formsets.get(MAPPINGS)
        if formset is None:
            return
        keys = self._asset_key_choices(formset)

        def narrow(fields):
            if collection is not None:
                fields["variable"].queryset = fields["variable"].queryset.filter(collection=collection)
            if keys:
                # Still a text field underneath: the choice is a help, and the rows' own checks are the rule.
                fields["asset_key"].widget = forms.Select(choices=[("", "—"), *keys])
            if self.instance.is_store:
                fields["asset_key"].label = _("Array in the store")
                fields["asset_key"].help_text = _("The array's full path in the upstream store.")

        for row in formset.forms:
            narrow(row.fields)
        # The clone is built from the class, which every request shares: narrow a copy.
        row_class = formset.form
        narrowed = type(row_class)(row_class.__name__, (row_class,), {})
        fields = {name: deepcopy(field) for name, field in row_class.base_fields.items()}
        narrow(fields)
        narrowed.base_fields = fields
        formset.form = narrowed

    def _asset_key_choices(self, formset):
        """The surveyed asset keys, and any key already mapped that the survey
        no longer sees. Empty for a feed nothing was surveyed for: its keys are typed."""
        keys = list(self.instance.surveyed_asset_keys)
        if not keys:
            return []
        seen = {key for key, _label in keys}
        for row in formset.forms:
            mapped = row.initial.get("asset_key")
            if mapped and mapped not in seen:
                seen.add(mapped)
                keys.append((mapped, mapped))
        return keys

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

    # ── The feed as a whole ─────────────────────────────────────────────────

    def clean(self):
        cleaned = super().clean()
        start, end = cleaned.get("window_start"), cleaned.get("window_end")
        if start and end and end < start:
            self.add_error("window_end", _("The window ends before it starts."))
        self._check_mapping()
        return cleaned

    def _check_mapping(self):
        """Every row maps to a Variable of the collection this feed harvests into
        (ADR 0042 §3). That no key or Variable is mapped twice is the rows' own check."""
        formset = self.formsets.get(MAPPINGS)
        if formset is None:
            return
        formset.is_valid()
        collection = self.instance.collection if self.instance.pk else None
        # For a store, the survey decided which arrays can be mapped at all (ADR 0044 §8):
        # one it listed as not yet supported is not mapped by typing its path.
        offered = {key for key, _label in self._asset_key_choices(formset)} if self.instance.is_store else None
        for row in formset.forms:
            row_cleaned = getattr(row, "cleaned_data", None) or {}
            variable = row_cleaned.get("variable")
            if variable is None or row_cleaned.get("DELETE"):
                continue
            if collection is None or variable.collection_id != collection.pk:
                row.add_error("variable", _("This Variable does not belong to the feed's collection."))
            if offered is not None and row_cleaned.get("asset_key") not in offered:
                row.add_error("asset_key", _("This is not an array the upstream store offers for mapping."))
