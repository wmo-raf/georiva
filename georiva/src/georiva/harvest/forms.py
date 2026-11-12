"""
The Harvest feed's admin form: what an operator may change once a feed exists (ADR 0042 §2–3).

A harvest feed is created by the Harvest Feed Wizard and nowhere else, so this
form only edits: the feed's name, cadence and window, and its Variable mapping.
Its Upstream and upstream collection are fixed — another one is another feed —
and where its files are read from is confirmed on the feed's page, from what a
re-check of the Upstream proposes, never typed here.
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
        if self.instance.pk:
            # Its collection, and every row harvested so far, is in this catalog.
            self.fields["catalog"].disabled = True
            self._narrow_mapping_rows(self.instance.collection)

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
        for row in formset.forms:
            row_cleaned = getattr(row, "cleaned_data", None) or {}
            variable = row_cleaned.get("variable")
            if variable is None or row_cleaned.get("DELETE"):
                continue
            if collection is None or variable.collection_id != collection.pk:
                row.add_error("variable", _("This Variable does not belong to the feed's collection."))
