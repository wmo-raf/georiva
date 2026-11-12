"""
The Harvest Feed Wizard: a Harvest feed from an Upstream's address, and nothing
else an operator must already know (ADR 0042 §2).

Five steps: the catalog's address; pick a collection; what the Upstream survey
found; choose which images to show and confirm where their files are read
from; schedule and save. The survey does the knowing — collection ids, asset
keys, hosts and buckets are found, never typed — and the wizard stops at step 3
for a collection that cannot be harvested, saying why.

What an operator reads here is in their words: no "STAC", "S3" or "COG".
"""

from dataclasses import asdict
from datetime import UTC, datetime
from urllib.parse import urlsplit

from django.contrib import messages
from django.db import transaction
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils.text import slugify
from django.utils.translation import gettext as _

# The Push Feed Wizard's Sample File needs the same two things of a scanned
# variable — a Unit for its unit string, and bounds rounded outward — and has them.
from georiva.ingestion.upload_wizard_views import _nice_bounds, _resolve_unit
from georiva.organisations.access import require_active_org, scoped_queryset

from .setup import FeedPlan, PlannedVariable, SetupRefused, provision_harvest_feed
from .survey import Shape, reason_for, survey_catalog, survey_collection
from .upstream import UpstreamUnreachable

_SESSION_KEY = "georiva_harvest_wizard"

STEP_LABELS = [
    _("Catalog address"),
    _("Collection"),
    _("What was found"),
    _("Images and files"),
    _("Schedule and save"),
]

#: How often a feed looks for what is new, as an operator chooses it.
CADENCES = [
    (360, _("Every 6 hours")),
    (720, _("Twice a day")),
    (1440, _("Once a day")),
    (10080, _("Once a week")),
]
DEFAULT_CADENCE = 1440


def _chrome(step):
    label = STEP_LABELS[step - 1]
    return {
        "breadcrumbs_items": [
            {"url": reverse("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse("data_feed_list"), "label": _("Pull feeds")},
            {"url": None, "label": _("Show data from an online catalog — %s") % label},
        ],
        "header_title": _("Show data from an online catalog — %s") % label,
        "header_icon": "globe",
        "step": step,
        "step_labels": STEP_LABELS,
    }


def _session(request):
    return request.session.get(_SESSION_KEY, {})


def _save(request, data):
    request.session[_SESSION_KEY] = data


def _client(url: str):
    from .models import HarvestFeed

    return HarvestFeed(upstream_url=url).survey_client()


def _unreadable(why) -> str:
    return _(
        "This catalog could not be read: %(why)s. GeoRiva can only show catalogs that can be read without an account."
    ) % {"why": why}


# ── Step 1: the catalog's address ───────────────────────────────────────────


def harvest_wizard_step1(request):
    require_active_org(request)
    session = _session(request)
    address = session.get("upstream_url", "")

    if request.method == "POST":
        address = request.POST.get("upstream_url", "").strip().rstrip("/")
        error, found = _survey_address(address)
        if error:
            messages.error(request, error)
        else:
            # A new address is a new start: nothing found at another one carries over.
            _save(
                request,
                {
                    "upstream_url": address,
                    "collections": [{"id": c.id, "title": c.title} for c in found.collections],
                    "described": found.described,
                },
            )
            return redirect("harvest_wizard_step2")

    return render(request, "georivaharvest/wizard_step1_address.html", {"address": address, **_chrome(1)})


def _survey_address(address: str):
    """`(error, None)` or `(None, the catalog's survey)` for the address an operator gave."""
    parts = urlsplit(address)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        return _("Please give the catalog's web address, starting with https://."), None
    try:
        found = survey_catalog(_client(address))
    except UpstreamUnreachable as why:
        return _unreadable(why), None
    if not found.collections:
        return (reason_for(Shape.NO_API) if not found.has_api else _("This catalog lists no collections.")), None
    return None, found


# ── Step 2: pick a collection ───────────────────────────────────────────────


def harvest_wizard_step2(request):
    require_active_org(request)
    session = _session(request)
    if not session.get("collections"):
        return redirect("harvest_wizard_step1")

    if request.method == "POST":
        collection_id = request.POST.get("collection_id", "")
        chosen = next((c for c in session["collections"] if c["id"] == collection_id), None)
        if chosen is None:
            messages.error(request, _("Please choose a collection."))
        else:
            try:
                found = survey_collection(_client(session["upstream_url"]), session["upstream_url"], collection_id)
            except UpstreamUnreachable as why:
                messages.error(request, _unreadable(why))
            else:
                # What was chosen for another collection does not carry over.
                for stale in ("chosen", "confirmed"):
                    session.pop(stale, None)
                session.update(
                    {
                        "collection_id": collection_id,
                        "collection_title": chosen["title"] or collection_id,
                        "survey": _as_session(found),
                    }
                )
                _save(request, session)
                return redirect("harvest_wizard_step3")

    return render(
        request,
        "georivaharvest/wizard_step2_collection.html",
        {"session": session, "collections": session["collections"], **_chrome(2)},
    )


def _as_session(found) -> dict:
    """A collection's survey as the session keeps it between steps."""
    return {
        "shape": found.shape.value,
        "harvestable": found.harvestable,
        "reason": found.reason,
        "items_sampled": found.items_sampled,
        "assets": [asdict(asset) for asset in found.assets],
        "locations": [{**asdict(location), "proposed": location.proposed} for location in found.locations],
    }


# ── Step 3: what the survey found ───────────────────────────────────────────


def harvest_wizard_step3(request):
    require_active_org(request)
    session = _session(request)
    survey = session.get("survey")
    if not survey:
        return redirect("harvest_wizard_step2" if session.get("collections") else "harvest_wizard_step1")

    return render(
        request,
        "georivaharvest/wizard_step3_found.html",
        {
            "session": session,
            "survey": survey,
            "readable": [asset for asset in survey["assets"] if asset["readable"]],
            "hosts": sorted({location["host"] for location in survey["locations"] if location["readable"]}),
            **_chrome(3),
        },
    )


# ── Step 4: choose images, confirm where their files are ────────────────────


def harvest_wizard_step4(request):
    from georiva.core.models import Unit

    require_active_org(request)
    session = _session(request)
    survey = session.get("survey")
    if not survey or not survey["harvestable"]:
        return redirect("harvest_wizard_step3" if survey else "harvest_wizard_step1")

    proposed = [location for location in survey["locations"] if location["proposed"]]
    posted = request.POST if request.method == "POST" else None

    if posted is not None:
        chosen, errors = _chosen_images(survey, posted)
        confirmed = [location for index, location in enumerate(proposed) if posted.get(f"allow_{index}")]
        if not chosen and not errors:
            errors.append(_("Choose at least one kind of image to show."))
        errors += _unconfirmed(chosen, survey, confirmed)
        if errors:
            for error in errors:
                messages.error(request, error)
        else:
            session.update({"chosen": chosen, "confirmed": confirmed})
            _save(request, session)
            return redirect("harvest_wizard_step5")

    return render(
        request,
        "georivaharvest/wizard_step4_images.html",
        {
            "session": session,
            "rows": _image_rows(survey, session.get("chosen"), posted),
            "locations": [
                {
                    **location,
                    "index": index,
                    "allowed": bool(posted.get(f"allow_{index}"))
                    if posted is not None
                    else location in session.get("confirmed", []),
                }
                for index, location in enumerate(proposed)
            ],
            "units": Unit.objects.order_by("name"),
            **_chrome(4),
        },
    )


def _place(asset: dict) -> tuple[str, str]:
    location = asset.get("location") or {}
    return location.get("host", ""), location.get("bucket", "")


def _image_rows(survey: dict, chosen, posted) -> list[dict]:
    """One row per raster asset the survey saw: what the form shows for it.

    What was posted wins, then what was chosen on an earlier visit, then what
    the Sample item suggests.
    """
    earlier = {image["key"]: image for image in chosen or []}
    rows = []
    for index, asset in enumerate(survey["assets"]):
        low, high = asset["value_min"], asset["value_max"]
        if low is not None and high is not None:
            low, high = _nice_bounds(low, high)
        row = {
            **asset,
            "index": index,
            "use": asset["readable"] and (not earlier or asset["key"] in earlier),
            "name": asset["title"] or asset["key"],
            "unit_id": _resolve_unit(asset["unit"])["unit_id"],
            "low": low,
            "high": high,
        }
        if asset["key"] in earlier:
            image = earlier[asset["key"]]
            row.update(name=image["name"], unit_id=image["unit_id"], low=image["value_min"], high=image["value_max"])
        if posted is not None:
            row.update(
                use=asset["readable"] and bool(posted.get(f"use_{index}")),
                name=posted.get(f"name_{index}", row["name"]),
                unit_id=_whole(posted.get(f"unit_{index}")),
                low=posted.get(f"low_{index}", ""),
                high=posted.get(f"high_{index}", ""),
            )
        rows.append(row)
    return rows


def _whole(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _number(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None


def _chosen_images(survey: dict, posted) -> tuple[list[dict], list[str]]:
    """The images an operator ticked, each with the name, unit and range they
    settled on, and what is wrong with any of them."""
    from georiva.core.models import Unit

    chosen, errors = [], []
    for index, asset in enumerate(survey["assets"]):
        if not asset["readable"] or not posted.get(f"use_{index}"):
            continue
        name = posted.get(f"name_{index}", "").strip() or asset["title"] or asset["key"]
        unit_id = _whole(posted.get(f"unit_{index}"))
        low, high = _number(posted.get(f"low_{index}")), _number(posted.get(f"high_{index}"))
        if unit_id is None or not Unit.objects.filter(pk=unit_id).exists():
            errors.append(_("Choose a unit for “%(name)s”.") % {"name": name})
        if low is None or high is None or low >= high:
            errors.append(
                _("Give “%(name)s” a lowest and a highest value to colour by, the lowest first.") % {"name": name}
            )
        host, bucket = _place(asset)
        chosen.append(
            {
                "key": asset["key"],
                "title": asset["title"],
                "name": name,
                "unit_id": unit_id,
                "value_min": low,
                "value_max": high,
                "host": host,
                "bucket": bucket,
            }
        )
    return chosen, errors


def _unconfirmed(chosen: list[dict], survey: dict, confirmed: list[dict]) -> list[str]:
    """An error for each place a chosen image's file lives that nobody agreed to read from."""
    agreed = {(location["host"], location["bucket"]) for location in confirmed}
    agreed |= {(location["host"], location["bucket"]) for location in survey["locations"] if location["own"]}
    missing = sorted({image["host"] for image in chosen if (image["host"], image["bucket"]) not in agreed})
    return [
        _("“%(host)s” holds files of the images you chose. Allow GeoRiva to read from it, or leave those images out.")
        % {"host": host}
        for host in missing
    ]


# ── Step 5: schedule and save ───────────────────────────────────────────────


def harvest_wizard_step5(request):
    from georiva.core.models import Catalog

    organisation = require_active_org(request)
    session = _session(request)
    if not session.get("chosen"):
        return redirect("harvest_wizard_step4" if session.get("survey") else "harvest_wizard_step1")

    # Only unclaimed catalogs of this organisation's: a catalog has one feed (ADR 0034).
    catalogs = Catalog.objects.filter(
        organisation=organisation, data_feed__isnull=True, push_feed__isnull=True
    ).order_by("name")
    title = session["collection_title"]
    form = {
        "catalog_mode": "create",
        "catalog_id": None,
        "new_catalog_name": title,
        "new_catalog_slug": slugify(title),
        "feed_name": title,
        "window_start": "",
        "window_end": "",
        "interval_minutes": DEFAULT_CADENCE,
    }

    if request.method == "POST":
        form.update({name: request.POST.get(name, "").strip() for name in form})
        plan, errors = _plan(form, session)
        if not errors:
            try:
                feed = provision_harvest_feed(organisation, plan)
            except SetupRefused as refused:
                errors = [str(refused)]
        if errors:
            for error in errors:
                messages.error(request, error)
        else:
            request.session.pop(_SESSION_KEY, None)
            # The first harvest, so the operator's next look at the feed shows data.
            transaction.on_commit(feed.run_now)
            messages.success(
                request,
                _("“%(name)s” is set up. GeoRiva is now listing what the catalog has; nothing is being copied.")
                % {"name": feed.name},
            )
            return redirect("data_feed_detail", pk=feed.pk)

    return render(
        request,
        "georivaharvest/wizard_step5_save.html",
        {
            "session": session,
            "form": form,
            "catalogs": catalogs,
            "cadences": CADENCES,
            "chosen": session["chosen"],
            "confirmed": session.get("confirmed", []),
            **_chrome(5),
        },
    )


def _day(value: str) -> tuple[datetime | None, bool]:
    """`(the day, True)` for a date as a browser sends it, `(None, True)` for none
    given, and `(None, False)` for anything else."""
    if not value:
        return None, True
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC), True
    except ValueError:
        return None, False


def _plan(form: dict, session: dict) -> tuple[FeedPlan | None, list[str]]:
    errors = []
    if not form["feed_name"]:
        errors.append(_("Please give this feed a name."))
    start, start_ok = _day(form["window_start"])
    end, end_ok = _day(form["window_end"])
    if not (start_ok and end_ok):
        errors.append(_("Please give dates as a day, month and year."))
    elif start and end and end < start:
        errors.append(_("The last date is before the first."))
    interval = _whole(form["interval_minutes"])
    if interval not in dict(CADENCES):
        errors.append(_("Please choose how often to look for new data."))
    catalog_id = _whole(form["catalog_id"]) if form["catalog_mode"] == "select" else None
    if form["catalog_mode"] == "select" and catalog_id is None:
        errors.append(_("Please choose a catalog."))
    if errors:
        return None, errors

    confirmed = session.get("confirmed", [])
    used = {(image["host"], image["bucket"]) for image in session["chosen"]}
    return (
        FeedPlan(
            name=form["feed_name"],
            upstream_url=session["upstream_url"],
            upstream_collection_id=session["collection_id"],
            variables=[
                PlannedVariable(
                    asset_key=image["key"],
                    name=image["name"],
                    unit_id=image["unit_id"],
                    value_min=image["value_min"],
                    value_max=image["value_max"],
                )
                for image in session["chosen"]
            ],
            interval_minutes=interval,
            window_start=start,
            window_end=end,
            catalog_id=catalog_id,
            new_catalog_name=form["new_catalog_name"],
            new_catalog_slug=form["new_catalog_slug"],
            # Only the confirmed places the chosen images are actually in.
            pinned_buckets={
                location["bucket"]: location["host"]
                for location in confirmed
                if location["bucket"] and (location["host"], location["bucket"]) in used
            },
            asset_hosts=sorted(
                location["host"] for location in confirmed if not location["bucket"] and (location["host"], "") in used
            ),
            surveyed_assets=_seen(session["survey"]["assets"]),
        ),
        [],
    )


def _seen(assets) -> list[dict]:
    """The readable raster asset keys a survey saw, as a feed remembers them."""
    seen = []
    for asset in assets:
        asset = asset if isinstance(asset, dict) else asdict(asset)
        if asset["readable"]:
            seen.append({"key": asset["key"], "title": asset["title"]})
    return seen


# ── Re-check upstream, on a feed that exists ────────────────────────────────


def harvest_recheck_upstream(request, pk):
    """Survey a feed's Upstream again: confirm where its files now are, and
    refresh the asset keys its mapping offers."""
    from .models import HarvestFeed

    feed = get_object_or_404(scoped_queryset(request, HarvestFeed.objects.all()), pk=pk)
    try:
        found = survey_collection(feed.survey_client(), feed.upstream_url, feed.upstream_collection_id)
    except UpstreamUnreachable as why:
        messages.error(request, _unreadable(why))
        return redirect("data_feed_detail", pk=feed.pk)

    allowed = feed.allowed_asset_hosts
    new = [
        location
        for location in found.proposed_locations
        if feed.pinned_buckets.get(location.bucket) != location.host
        if location.bucket or location.host not in allowed
    ]

    if request.method == "POST" and found.harvestable:
        # By place, not by position: the Upstream is asked again on the way in.
        ticked = set(request.POST.getlist("allow"))
        buckets, hosts = found.pins([location for location in new if f"{location.bucket}|{location.host}" in ticked])
        feed.pinned_buckets = {**feed.pinned_buckets, **buckets}
        feed.asset_hosts = sorted({*feed.asset_hosts, *hosts})
        feed.surveyed_assets = _seen(found.assets)
        feed.save()
        if buckets or hosts:
            messages.success(request, _("GeoRiva may now read this feed's files from the places you allowed."))
        else:
            messages.success(request, _("The catalog was checked again. Nothing about where its files are changed."))
        return redirect("data_feed_detail", pk=feed.pk)

    return render(
        request,
        "georivaharvest/recheck_upstream.html",
        {
            "feed": feed,
            "found": found,
            "new": [{"value": f"{location.bucket}|{location.host}", "host": location.host} for location in new],
            "readable": [asset for asset in found.assets if asset.readable],
            "breadcrumbs_items": [
                {"url": reverse("wagtailadmin_home"), "label": _("Home")},
                {"url": reverse("data_feed_list"), "label": _("Pull feeds")},
                {"url": reverse("data_feed_detail", kwargs={"pk": feed.pk}), "label": feed.name},
                {"url": None, "label": _("Check the catalog again")},
            ],
            "header_title": _("Check the catalog again — %s") % feed.name,
            "header_icon": "globe",
        },
    )
