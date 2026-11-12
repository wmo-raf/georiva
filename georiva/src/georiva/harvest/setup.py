"""
Setting a Harvest feed up: everything the Harvest Feed Wizard saves, together or not at all (ADR 0042 §2).

One call creates the Catalog (or takes an unclaimed one), the feed with its
Pinned locations, the collection it harvests into, a Variable per chosen asset
and the Variable mapping. A Variable that is already there is taken as it is:
what an operator has hand-tuned is never overwritten.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from django.db import IntegrityError, transaction
from django.utils.text import slugify
from django.utils.translation import gettext as _


class SetupRefused(Exception):
    """The feed could not be set up, and nothing was saved. Its message is for the operator."""


@dataclass(frozen=True)
class PlannedVariable:
    asset_key: str
    name: str
    unit_id: int
    value_min: float
    value_max: float


@dataclass(frozen=True)
class FeedPlan:
    name: str
    upstream_url: str
    upstream_collection_id: str
    variables: list[PlannedVariable]
    interval_minutes: int = 1440
    window_start: datetime | None = None
    window_end: datetime | None = None
    #: An unclaimed catalog of the organisation's to harvest into, by pk; or a new one's name and slug.
    catalog_id: int | None = None
    new_catalog_name: str = ""
    new_catalog_slug: str = ""
    pinned_buckets: dict[str, str] = field(default_factory=dict)
    asset_hosts: list[str] = field(default_factory=list)
    surveyed_assets: list[dict] = field(default_factory=list)


def provision_harvest_feed(organisation, plan: FeedPlan):
    """Create the feed `plan` describes, owned by `organisation`. Raises
    `SetupRefused` — having saved nothing — when it cannot."""
    from georiva.core.models import Variable

    from .models import HarvestFeed, VariableMapping

    if not plan.variables:
        raise SetupRefused(_("Choose at least one kind of image to show."))
    slugs = [slugify(variable.asset_key)[:100] for variable in plan.variables]
    if "" in slugs or len(set(slugs)) < len(slugs):
        raise SetupRefused(_("Two of the chosen images would get the same short name. Choose one of them."))

    try:
        with transaction.atomic():
            feed = HarvestFeed.objects.create(
                catalog=_catalog(organisation, plan),
                name=plan.name,
                upstream_url=plan.upstream_url,
                upstream_collection_id=plan.upstream_collection_id,
                window_start=plan.window_start,
                window_end=plan.window_end,
                interval_minutes=plan.interval_minutes,
                pinned_buckets=dict(plan.pinned_buckets),
                asset_hosts=list(plan.asset_hosts),
                surveyed_assets=list(plan.surveyed_assets),
            )
            collection = feed.ensure_collection()
            for slug, planned in zip(slugs, plan.variables, strict=True):
                # get_or_create: a Variable curated here before is the operator's, as it stands.
                variable, _created = Variable.objects.get_or_create(
                    collection=collection,
                    slug=slug,
                    defaults={
                        "name": planned.name[:255] or planned.asset_key,
                        "unit_id": planned.unit_id,
                        "value_min": planned.value_min,
                        "value_max": planned.value_max,
                    },
                )
                VariableMapping.objects.create(feed=feed, asset_key=planned.asset_key, variable=variable)
    except IntegrityError as clash:
        raise SetupRefused(_("This could not be saved: %(why)s") % {"why": clash}) from clash
    return feed


def _catalog(organisation, plan: FeedPlan):
    """The catalog to harvest into: the organisation's own and unclaimed, or a new one."""
    from georiva.core.models import Catalog
    from georiva.core.provisioning import catalog_slug_taken

    if plan.catalog_id is None:
        slug = plan.new_catalog_slug or slugify(plan.new_catalog_name)
        if not plan.new_catalog_name or not slug:
            raise SetupRefused(_("Please enter a name for the new catalog."))
        if catalog_slug_taken(organisation, slug):
            raise SetupRefused(_("A catalog with the short name “%(slug)s” already exists.") % {"slug": slug})
        return Catalog.objects.create(
            organisation=organisation,
            name=plan.new_catalog_name,
            slug=slug,
            file_format=Catalog.FileFormat.GEOTIFF,
        )

    # Scoped at the point of use: a wizard session outlives a move to another organisation's host.
    catalog = Catalog.objects.filter(pk=plan.catalog_id, organisation=organisation).first()
    if catalog is None:
        raise SetupRefused(_("Please choose a catalog."))
    if getattr(catalog, "data_feed", None) is not None or getattr(catalog, "push_feed", None) is not None:
        raise SetupRefused(_("This catalog already has a feed. A catalog has at most one feed."))
    return catalog
