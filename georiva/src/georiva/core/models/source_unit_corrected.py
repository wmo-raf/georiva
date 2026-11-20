"""``SourceUnitCorrected``: an operator said a Variable's bytes hold another unit (ADR 0046 §8).

Correct source unit converts nothing: every value of the Variable, its range,
its style stops and its stored statistics now mean a different physical value.
The event is the record of who decided that, and when, and what the label was.
"""

from django.db import models

from georiva.events.models import DomainEvent


class SourceUnitCorrected(DomainEvent):
    label = "Source unit corrected"

    variable = models.ForeignKey(
        "georivacore.Variable", null=True, blank=True, on_delete=models.SET_NULL, related_name="+"
    )
    collection = models.ForeignKey(
        "georivacore.Collection", null=True, blank=True, on_delete=models.SET_NULL, related_name="+"
    )
    catalog_slug = models.CharField(max_length=255)
    collection_slug = models.CharField(max_length=255)
    variable_slug = models.CharField(max_length=100)
    from_unit = models.CharField(max_length=255, help_text="The source unit's symbol before the correction.")
    to_unit = models.CharField(max_length=255, help_text="The source unit's symbol after it.")

    def subject_url(self):
        if self.collection_id is None:
            return None
        from django.urls import reverse

        return reverse("collection:edit", args=[self.collection_id])

    def summary(self):
        return (
            f"Source unit of {self.catalog_slug}/{self.collection_slug}/{self.variable_slug} "
            f"corrected from {self.from_unit} to {self.to_unit}"
        )
