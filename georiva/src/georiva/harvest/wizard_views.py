"""
The Harvest Feed Wizard: a Harvest feed from an Upstream's address, and nothing
else an operator must already know (ADR 0042 §2).

Five steps: the catalog's address; pick a collection; what the Upstream survey
found; choose which images to show and confirm where their files are read
from; schedule and save. The survey does the knowing — collection ids, asset
keys, hosts and buckets are found, never typed — and the wizard stops at step 3
for a collection that cannot be harvested, saying why.

The same five steps set a feed up from an Upstream store (ADR 0044 §1). Step 4
then offers the store's fields in place of images, asks about each place its
data lives in turn, and reads one map of each chosen field with the places
confirmed and no others: that read is where a Variable's value range comes
from and the proof that the store can be read at all (§11, §12). Step 5 gives
a forecast store a Rolling window (§7). Its first harvest starts as it is
saved, like any other feed's, and its maps can be shown as their steps are listed.

A collection's survey runs off the request, and step 3 waits for it, saying
what it is waiting on: opening a large store takes longer than a web request
should (#667). What a survey learnt is kept for the rest of the wizard
session, so going back a step or reloading one does not read the Upstream
again; "Re-check upstream" on a feed always reads afresh.

What an operator reads here is in their words: no "STAC", "S3" or "COG", and
of a store no "Zarr", "chunk" or "codec".
"""

import re
import time
from collections import Counter
from dataclasses import asdict, dataclass
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

from . import background_survey
from .asset_hosts import chunk_place
from .models import DEFAULT_ROLLING_WINDOW_DAYS, MAX_ROLLING_WINDOW_DAYS
from .setup import FeedPlan, PlannedVariable, SetupRefused, provision_harvest_feed
from .survey import Shape, reason_for, survey_catalog
from .upstream import StoreField, UpstreamTimedOut, UpstreamUnreachable

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
#: A store's: a forecast has several runs a day, each arriving step by step
#: over hours, and asking which steps it holds costs one small read (#685).
STORE_CADENCES = [
    (15, _("Every 15 minutes")),
    (30, _("Every 30 minutes")),
    (60, _("Every hour")),
    (180, _("Every 3 hours")),
    *CADENCES,
]
DEFAULT_STORE_CADENCE = 60


def _cadences(store) -> tuple[list, int]:
    """The cadences an operator may choose from for this feed, and the one chosen for them."""
    return (STORE_CADENCES, DEFAULT_STORE_CADENCE) if store else (CADENCES, DEFAULT_CADENCE)


def _chrome(step, *, store=False):
    labels = list(STEP_LABELS)
    if store:
        labels[3] = _("Maps and places")
    label = labels[step - 1]
    return {
        "breadcrumbs_items": [
            {"url": reverse("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse("data_feed_list"), "label": _("Pull feeds")},
            {"url": None, "label": _("Show data from an online catalog — %s") % label},
        ],
        "header_title": _("Show data from an online catalog — %s") % label,
        "header_icon": "globe",
        "step": step,
        "step_labels": labels,
    }


def _session(request):
    return request.session.get(_SESSION_KEY, {})


def _save(request, data):
    request.session[_SESSION_KEY] = data


def _client(url: str):
    from .models import HarvestFeed

    return HarvestFeed(upstream_url=url).survey_client()


def _unreadable(why) -> str:
    if isinstance(why, UpstreamTimedOut):
        return _too_slow(why)
    return _(
        "This catalog could not be read: %(why)s. GeoRiva can only show catalogs that can be read without an account."
    ) % {"why": why}


def _too_slow(why) -> str:
    return _(
        "The catalog took too long to answer (%(why)s). Nothing was set up. A busy catalog often answers when "
        "asked again a moment later: try again."
    ) % {"why": why}


# ── Step 1: the catalog's address ───────────────────────────────────────────


def harvest_wizard_step1(request):
    require_active_org(request)
    session = _session(request)
    address = session.get("upstream_url", "")

    retry = False
    if request.method == "POST":
        address = request.POST.get("upstream_url", "").strip().rstrip("/")
        if address == session.get("upstream_url") and session.get("collections"):
            # Read already in this wizard session: what was found there still stands.
            return redirect("harvest_wizard_step2")
        error, found, retry = _survey_address(address)
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

    return render(
        request, "georivaharvest/wizard_step1_address.html", {"address": address, "retry": retry, **_chrome(1)}
    )


def _survey_address(address: str):
    """`(error, None, whether trying again may help)` or `(None, the catalog's
    survey, False)` for the address an operator gave."""
    parts = urlsplit(address)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        return _("Please give the catalog's web address, starting with https://."), None, False
    try:
        found = survey_catalog(_client(address))
    except UpstreamUnreachable as why:
        return _unreadable(why), None, isinstance(why, UpstreamTimedOut)
    if not found.collections:
        return (reason_for(Shape.NO_API) if not found.has_api else _("This catalog lists no collections.")), None, False
    return None, found, False


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
            title = chosen["title"] or collection_id
            known = session.get("surveys", {}).get(collection_id)
            if session.get("checking", {}).get("collection_id") == collection_id:
                # Already being surveyed: the wait goes on, and nothing is asked twice.
                pass
            elif known is not None:
                # Surveyed already in this wizard session: not read again.
                _choose(session, collection_id, title, known)
            else:
                _forget_choices_unless(session, collection_id)
                session.pop("survey", None)
                session["checking"] = {
                    "token": background_survey.start(session["upstream_url"], collection_id),
                    "collection_id": collection_id,
                    "collection_title": title,
                    "since": time.time(),
                }
            _save(request, session)
            return redirect("harvest_wizard_step3")

    return render(
        request,
        "georivaharvest/wizard_step2_collection.html",
        {"session": session, "collections": session["collections"], **_chrome(2)},
    )


def _forget_choices_unless(session: dict, collection_id: str) -> None:
    """What was chosen for another collection does not carry over."""
    if session.get("collection_id") != collection_id:
        for stale in ("chosen", "confirmed", "samples", "chunk_places", "declined_places"):
            session.pop(stale, None)


def _choose(session: dict, collection_id: str, title: str, survey: dict) -> None:
    """Make `survey` the one the wizard goes on from."""
    _forget_choices_unless(session, collection_id)
    session.pop("checking", None)
    session.setdefault("surveys", {})[collection_id] = survey
    session.update({"collection_id": collection_id, "collection_title": title, "survey": survey})


# ── Step 3: what the survey found ───────────────────────────────────────────


def harvest_wizard_step3(request):
    require_active_org(request)
    session = _session(request)
    if session.get("checking"):
        waiting = _collect(request, session)
        if waiting is not None:
            return waiting
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
            "offered": len(_offered(survey["store"])) if survey["store"] else 0,
            **_chrome(3, store=bool(survey["store"])),
        },
    )


@dataclass(frozen=True)
class _Wait:
    """Where a survey running off the request has got to, for a page waiting on it."""

    #: What it found, once it has: a `CollectionSurvey`.
    found: object = None
    #: What it is waiting on, in an operator's words, while it runs.
    waiting_on: str = ""
    #: Why it came to nothing, for the operator; empty unless it did.
    error: str = ""
    waited: int = 0

    @property
    def running(self) -> bool:
        return self.found is None and not self.error


def _wait_for(token: str, since: float) -> _Wait:
    """Where the survey under `token`, started at `since`, has got to. One that
    never answered, or was lost, came to nothing once waited on long enough."""
    answer = background_survey.collect(token)
    waited = time.time() - since
    state = answer["state"] if answer else None
    if state == background_survey.FOUND:
        return _Wait(found=answer["found"], waited=int(waited))
    if state == background_survey.CHECKING and waited <= background_survey.GIVE_UP_SECONDS:
        return _Wait(waiting_on=answer.get("waiting_on", ""), waited=int(waited))
    if state == background_survey.FAILED:
        error = _too_slow(answer["why"]) if answer.get("timed_out") else _unreadable(answer["why"])
    elif state == background_survey.CHECKING:
        error = _too_slow(_("GeoRiva stopped waiting after %(seconds)d seconds") % {"seconds": int(waited)})
    else:
        error = _("GeoRiva lost track of this check of the catalog before it finished. Nothing was changed: try again.")
    return _Wait(error=error, waited=int(waited))


def _collect(request, session: dict):
    """The page to show while a collection's survey runs, or when it came to
    nothing; `None` once it has found something, which is then the survey the
    wizard goes on from."""
    checking = session["checking"]
    wait = _wait_for(checking["token"], checking["since"])
    if wait.found is not None:
        survey = background_survey.as_session(wait.found)
        _choose(session, checking["collection_id"], checking["collection_title"], survey)
        _save(request, session)
        return None
    if wait.error:
        # It came to nothing: nothing is kept, and trying again starts afresh.
        session.pop("checking", None)
        _save(request, session)
        messages.error(request, wait.error)
    return render(
        request,
        "georivaharvest/wizard_step3_checking.html",
        {"session": session, "checking": checking, "wait": wait, **_chrome(3)},
    )


# ── Step 4: choose images, confirm where their files are ────────────────────


def harvest_wizard_step4(request):
    from georiva.core.models import Unit

    require_active_org(request)
    session = _session(request)
    survey = session.get("survey")
    if not survey or not survey["harvestable"]:
        return redirect("harvest_wizard_step3" if survey or session.get("checking") else "harvest_wizard_step1")
    if survey["store"]:
        return _step4_store(request, session)

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


# ── Step 4, for an Upstream store: choose fields, confirm where its data is ──

#: How many fields one message names before it says how many more there are.
_NAMED_IN_A_MESSAGE = 5


def _offered(store: dict) -> list[dict]:
    """The store's arrays that can become Variables, each with its place in the
    survey's list and the name it is shown by.

    A store with levels names dozens of its fields alike — "Temperature",
    "Pressure" — and tells them apart only by their paths (#685). A field whose
    own name another offered field shares is shown by its path, spelt as words.
    """
    offered = [{**array, "index": index} for index, array in enumerate(store["arrays"]) if array["offered"]]
    titles = Counter(array["title"] for array in offered if array["title"])
    for array in offered:
        title = array["title"]
        array["label"] = title if title and titles[title] == 1 else _spelt(array["path"]) or array["path"]
    return offered


def _spelt(path: str) -> str:
    """A field's path as words: `temperature_max_2m` is "Temperature max 2m"."""
    words = re.sub(r"[_/.\-]+", " ", path).strip()
    return words[:1].upper() + words[1:]


def _chunk_places(store: dict) -> list[dict]:
    """Each place the store declares its data lives in, as an operator is asked
    about it: named by its own name, never by an address (ADR 0044 §11)."""
    clouds = {"s3": _("Amazon's cloud storage"), "gs": _("Google's cloud storage")}
    places = []
    for index, declared in enumerate(store["chunk_locations"]):
        place = chunk_place(declared)
        region = store["chunk_regions"].get(declared, "")
        places.append(
            {
                "index": index,
                "declared": declared,
                "name": declared.partition("://")[2].rstrip("/"),
                "cloud": clouds.get(place.kind if place else "", ""),
                "region": region,
                # A place read by region can be read only once its region was found.
                "usable": place is not None and (place.kind != "s3" or bool(region)),
            }
        )
    return places


def _step4_store(request, session):
    from georiva.core.models import Unit

    survey = session["survey"]
    store = survey["store"]
    own = survey["locations"][0]
    places = _chunk_places(store)
    offered = _offered(store)
    posted = request.POST if request.method == "POST" else None

    if posted is not None:
        ticked = [array for array in offered if posted.get(f"use_{array['index']}")]
        allowed = [place for place in places if place["usable"] and posted.get(f"allow_{place['index']}")]
        errors, unseen = [], False
        if not ticked:
            errors.append(_("Choose at least one kind of map to show."))
        if own["proposed"] and not posted.get("allow_store"):
            errors.append(
                _("“%(host)s” is where this collection is kept. Allow this feed to read from it, or nothing is shown.")
                % {"host": own["host"]}
            )
        if not errors:
            errors = _read_samples(session, ticked, allowed, places)
            # What was read is kept whatever comes next: the next try reads only what it lacks.
            _save(request, session)
        if not errors:
            chosen, errors, unseen = _chosen_fields(ticked, posted, session["samples"])
        if errors:
            for error in errors:
                messages.error(request, error)
        elif unseen:
            messages.info(
                request,
                _(
                    "GeoRiva read one map of each kind you chose, from the provider's newest complete data, and "
                    "worked out the values below from it. Check them, then continue."
                ),
            )
        else:
            session.update(
                {
                    "chosen": chosen,
                    "confirmed": [own] if own["proposed"] else [],
                    "chunk_places": {place["declared"]: place["region"] for place in allowed},
                    "declined_places": [place for place in places if place not in allowed],
                }
            )
            _save(request, session)
            return redirect("harvest_wizard_step5")

    earlier = {field["key"]: field for field in session.get("chosen") or []}
    rows = _field_rows(offered, earlier, session.get("samples", {}), posted)
    return render(
        request,
        "georivaharvest/wizard_step4_fields.html",
        {
            "session": session,
            "rows": rows,
            "details": [row for row in rows if row["detailed"]],
            "unoffered": [array for array in store["arrays"] if not array["offered"]],
            "own": {
                **own,
                "allowed": bool(posted.get("allow_store")) if posted is not None else bool(session.get("confirmed")),
            },
            "places": [
                {
                    **place,
                    "allowed": bool(posted.get(f"allow_{place['index']}"))
                    if posted is not None
                    else place["declared"] in session.get("chunk_places", {}),
                }
                for place in places
            ],
            "units": Unit.objects.order_by("name"),
            **_chrome(4, store=True),
        },
    )


def _read_samples(session: dict, ticked: list[dict], allowed: list[dict], places: list[dict]) -> list[str]:
    """Read one map of each ticked field that has no sample yet, following the
    store's data into the places allowed and nowhere else, and keep what was
    read in the session. Returns what went wrong, for the operator.

    A sample stands for as long as every place it was read with is still
    allowed: taking a place away reads again without it (ADR 0044 §11).
    """
    store = session["survey"]["store"]
    following = {place["declared"]: place["region"] for place in allowed}
    samples = session.setdefault("samples", {})
    wanted = [
        array
        for array in ticked
        if array["path"] not in samples or not set(samples[array["path"]]["places"]) <= set(following)
    ]
    if not wanted:
        return []

    read = _client(session["upstream_url"]).sample_store(
        store["href"], [StoreField(array["path"], tuple(array["axes"])) for array in wanted], following
    )
    elsewhere: dict[str, list[str]] = {}
    errors = []
    for array in wanted:
        sample, name = read.get(array["path"]), array["label"]
        if sample is not None and sample.readable:
            samples[array["path"]] = {"low": sample.value_min, "high": sample.value_max, "places": sorted(following)}
            continue
        samples.pop(array["path"], None)
        if sample is not None and sample.elsewhere:
            elsewhere.setdefault(_where(sample.elsewhere, places), []).append(name)
        else:
            why = sample.problem if sample is not None else _("it was not read")
            errors.append(_("“%(name)s” could not be read: %(why)s.") % {"name": name, "why": why})
    by_name = {place["declared"]: place for place in places}
    for where, names in elsewhere.items():
        said = {"names": _named(names), "place": by_name[where]["name"] if where in by_name else where}
        if where not in by_name:
            errors.append(
                _(
                    "%(names)s: the data is kept somewhere this collection does not mention (%(place)s), so "
                    "GeoRiva will not read it. Leave it out."
                )
                % said
            )
        elif by_name[where]["usable"]:
            errors.append(
                _(
                    "%(names)s: the data is kept at “%(place)s”, which you have not allowed this feed to read "
                    "from. Allow it, or leave it out."
                )
                % said
            )
        else:
            errors.append(
                _("%(names)s: the data is kept at “%(place)s”, which GeoRiva could not find. Leave it out.") % said
            )
    return errors


def _where(address: str, places: list[dict]) -> str:
    """The declared place the data at `address` is in, or — when it is in none
    of them — the address cut back to the place it names."""
    for place in places:
        if address.startswith(place["declared"]):
            return place["declared"]
    scheme, _separator, rest = address.partition("://")
    return f"{scheme}://{rest.partition('/')[0]}/"


def _named(names: list[str]) -> str:
    """A few names, quoted, and how many more there are."""
    said = ", ".join(f"“{name}”" for name in names[:_NAMED_IN_A_MESSAGE])
    more = len(names) - _NAMED_IN_A_MESSAGE
    return said if more <= 0 else _("%(names)s and %(count)d more") % {"names": said, "count": more}


def _field_rows(offered: list[dict], earlier: dict, samples: dict, posted) -> list[dict]:
    """One row per field on offer. A field that is chosen and has been read
    also carries what its Variable will be — name, unit, description and value
    range: what was posted wins, then what was chosen on an earlier visit,
    then what the collection says of it and the map that was read shows."""
    rows = []
    for array in offered:
        index = array["index"]
        use = bool(posted.get(f"use_{index}")) if posted is not None else array["path"] in earlier
        row = {**array, "use": use, "detailed": False}
        sample = samples.get(array["path"])
        if use and sample:
            low, high = _field_bounds(array, sample["low"], sample["high"])
            row.update(
                detailed=True,
                name=row["label"],
                unit_id=_resolve_unit(array["unit"])["unit_id"],
                low=low,
                high=high,
                about=array["description"],
            )
            if array["path"] in earlier:
                field = earlier[array["path"]]
                row.update(
                    name=field["name"],
                    unit_id=field["unit_id"],
                    low=field["value_min"],
                    high=field["value_max"],
                    about=field["description"],
                )
            if posted is not None and f"name_{index}" in posted:
                row.update(
                    name=posted.get(f"name_{index}", ""),
                    unit_id=_whole(posted.get(f"unit_{index}")),
                    low=posted.get(f"low_{index}", ""),
                    high=posted.get(f"high_{index}", ""),
                    about=posted.get(f"about_{index}", ""),
                )
        rows.append(row)
    return rows


def _field_bounds(array: dict, low: float, high: float) -> tuple[float, float]:
    """The range a field is coloured by, from the one map of it that was read:
    rounded outward, and for one component of a vector — a wind's U or V — the
    same either side of zero. One map of U may blow mostly east and one of V
    mostly south; read alone each would lean its own way (#685)."""
    low, high = _nice_bounds(low, high)
    if _is_component(array) and low < 0 < high:
        widest = max(-low, high)
        return -widest, widest
    return low, high


def _is_component(array: dict) -> bool:
    """Whether a field is one component of a vector: so its name says, or its path has a lone `u` or `v`."""
    if "component" in f"{array['title']} {array['description']}".lower():
        return True
    return bool({"u", "v"} & set(re.split(r"[_/.\-]+", array["path"].lower())))


def _chosen_fields(ticked: list[dict], posted, samples: dict) -> tuple[list[dict], list[str], bool]:
    """`(the fields chosen, what is wrong with any of them, whether one has
    not been put to the operator yet)`. A field ticked for the first time comes
    back with nothing typed for it: its values are shown before they are taken."""
    from georiva.core.models import Unit

    if any(f"name_{array['index']}" not in posted for array in ticked):
        return [], [], True
    chosen, errors = [], []
    for array in ticked:
        index = array["index"]
        name = posted.get(f"name_{index}", "").strip() or array["label"]
        unit_id = _whole(posted.get(f"unit_{index}"))
        low, high = _number(posted.get(f"low_{index}")), _number(posted.get(f"high_{index}"))
        if unit_id is None or not Unit.objects.filter(pk=unit_id).exists():
            errors.append(_("Choose a unit for “%(name)s”.") % {"name": name})
        if low is None or high is None or low >= high:
            errors.append(
                _("Give “%(name)s” a lowest and a highest value to colour by, the lowest first.") % {"name": name}
            )
        chosen.append(
            {
                "key": array["path"],
                "title": array["title"],
                "name": name,
                "unit_id": unit_id,
                "value_min": low,
                "value_max": high,
                "description": posted.get(f"about_{index}", "").strip(),
            }
        )
    return chosen, errors, False


# ── Step 5: schedule and save ───────────────────────────────────────────────


def harvest_wizard_step5(request):
    from georiva.core.models import Catalog

    organisation = require_active_org(request)
    session = _session(request)
    if not session.get("chosen") or not session.get("survey"):
        earlier = session.get("survey") or session.get("checking")
        return redirect("harvest_wizard_step4" if earlier else "harvest_wizard_step1")

    # Only unclaimed catalogs of this organisation's: a catalog has one feed (ADR 0034).
    catalogs = Catalog.objects.filter(
        organisation=organisation, data_feed__isnull=True, push_feed__isnull=True
    ).order_by("name")
    title = session["collection_title"]
    store = session["survey"]["store"]
    # A forecast store's window rolls on run time; any other feed's is fixed (ADR 0044 §7).
    rolling = bool(store) and store["has_runs"]
    cadences, default_cadence = _cadences(store)
    form = {
        "catalog_mode": "create",
        "catalog_id": None,
        "new_catalog_name": title,
        # A catalog's short name is letters, digits and hyphens; an upstream id often has underscores.
        "new_catalog_slug": slugify(title).replace("_", "-"),
        "feed_name": title,
        "window_start": "",
        "window_end": "",
        "rolling_window_days": str(DEFAULT_ROLLING_WINDOW_DAYS),
        "interval_minutes": default_cadence,
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
            if feed.is_store:
                # Its steps are listed from now on, and the tile server draws each from the store (ADR 0044 §9).
                messages.success(
                    request,
                    _(
                        "“%(name)s” is set up. GeoRiva is now listing what the data store holds; nothing is being "
                        "copied. Its maps can be shown as soon as their steps are listed."
                    )
                    % {"name": feed.name},
                )
            else:
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
            "cadences": cadences,
            "chosen": session["chosen"],
            "confirmed": session.get("confirmed", []),
            "store": store,
            "rolling": rolling,
            "max_rolling_days": MAX_ROLLING_WINDOW_DAYS,
            "places": [place for place in _chunk_places(store) if place["declared"] in session.get("chunk_places", {})]
            if store
            else [],
            "declined_places": session.get("declined_places", []) if store else [],
            **_chrome(5, store=bool(store)),
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
    store = session["survey"]["store"]
    rolling = bool(store) and store["has_runs"]
    errors = []
    if not form["feed_name"]:
        errors.append(_("Please give this feed a name."))
    start = end = days = None
    if rolling:
        days = _whole(form["rolling_window_days"])
        if days is None or not 1 <= days <= MAX_ROLLING_WINDOW_DAYS:
            errors.append(
                _("Please say how many days of forecasts to keep: a whole number from 1 to %(most)d.")
                % {"most": MAX_ROLLING_WINDOW_DAYS}
            )
    else:
        start, start_ok = _day(form["window_start"])
        end, end_ok = _day(form["window_end"])
        if not (start_ok and end_ok):
            errors.append(_("Please give dates as a day, month and year."))
        elif start and end and end < start:
            errors.append(_("The last date is before the first."))
    interval = _whole(form["interval_minutes"])
    if interval not in dict(_cadences(store)[0]):
        errors.append(_("Please choose how often to look for new data."))
    catalog_id = _whole(form["catalog_id"]) if form["catalog_mode"] == "select" else None
    if form["catalog_mode"] == "select" and catalog_id is None:
        errors.append(_("Please choose a catalog."))
    if errors:
        return None, errors

    confirmed = session.get("confirmed", [])
    if store:
        return _store_plan(form, session, interval, catalog_id, start, end, days), []
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


def _store_plan(form: dict, session: dict, interval, catalog_id, start, end, days) -> FeedPlan:
    """The plan for a feed over an Upstream store: its Variables are mapped
    from arrays by their full path, and its Pinned locations are the store's
    own place and each place its data lives that the operator confirmed."""
    store, confirmed = session["survey"]["store"], session.get("confirmed", [])
    return FeedPlan(
        name=form["feed_name"],
        upstream_url=session["upstream_url"],
        upstream_collection_id=session["collection_id"],
        variables=[
            PlannedVariable(
                asset_key=field["key"],
                name=field["name"],
                unit_id=field["unit_id"],
                value_min=field["value_min"],
                value_max=field["value_max"],
                description=field["description"],
            )
            for field in session["chosen"]
        ],
        interval_minutes=interval,
        window_start=start,
        window_end=end,
        catalog_id=catalog_id,
        new_catalog_name=form["new_catalog_name"],
        new_catalog_slug=form["new_catalog_slug"],
        pinned_buckets={location["bucket"]: location["host"] for location in confirmed if location["bucket"]},
        asset_hosts=sorted(location["host"] for location in confirmed if not location["bucket"]),
        surveyed_assets=_seen_arrays(store["arrays"]),
        store_href=store["href"],
        rolling_window_days=days,
        pinned_chunk_locations=dict(session.get("chunk_places", {})),
    )


def _seen_arrays(arrays) -> list[dict]:
    """The arrays of a store a survey offered, as a feed remembers them: by
    path, where an items feed remembers asset keys."""
    seen = []
    for array in arrays:
        array = array if isinstance(array, dict) else {**asdict(array), "offered": array.offered}
        if array["offered"]:
            seen.append({"key": array["path"], "title": array["title"]})
    return seen


def _seen(assets) -> list[dict]:
    """The readable raster asset keys a survey saw, as a feed remembers them."""
    seen = []
    for asset in assets:
        asset = asset if isinstance(asset, dict) else asdict(asset)
        if asset["readable"]:
            seen.append({"key": asset["key"], "title": asset["title"]})
    return seen


# ── Re-check upstream, on a feed that exists ────────────────────────────────


_RECHECK_KEY = "georiva_harvest_recheck"

#: How many re-checks of one feed a session keeps open at once: one per tab, near enough.
_OPEN_CHECKS = 5


def harvest_recheck_upstream(request, pk):
    """Survey a feed's Upstream again and put to the operator what changed.
    Nothing is taken from it without their yes (ADR 0042 §9).

    The survey runs off the request (#695). Opening the page starts one —
    always afresh — and sends the operator to it by its token (`?check=`);
    that page waits for it, and reloading it starts nothing. Saving acts on
    the survey the page showed, kept server-side under the token: the Upstream
    is not read again, and only what that survey proposed can be taken. A
    check this session did not open is never saved.
    """
    from .models import HarvestFeed

    feed = get_object_or_404(scoped_queryset(request, HarvestFeed.objects.all()), pk=pk)
    here = reverse("harvest_recheck_upstream", kwargs={"pk": feed.pk})
    checks = request.session.get(_RECHECK_KEY, {})
    opened = checks.get(str(feed.pk), {})
    token = request.POST.get("check") or request.GET.get("check")

    if request.method == "GET" and not token:
        token = background_survey.start(feed.upstream_url, feed.upstream_collection_id)
        opened = {**opened, token: time.time()}
        checks[str(feed.pk)] = dict(sorted(opened.items(), key=lambda check: check[1])[-_OPEN_CHECKS:])
        request.session[_RECHECK_KEY] = checks
        return redirect(f"{here}?check={token}")
    if token not in opened:
        messages.error(request, _("That check of the catalog was not started here. GeoRiva is checking it again."))
        return redirect(here)

    wait = _wait_for(token, opened[token])
    if wait.found is None:
        if request.method == "POST":
            # Never saved blind: a save acts on a survey the operator was shown.
            if wait.running:
                messages.error(request, _("The catalog is still being checked, so nothing was saved yet."))
                return redirect(f"{here}?check={token}")
            gone = _("That check of the catalog is no longer kept, so nothing was saved. GeoRiva will check again.")
            messages.error(request, gone)
            return redirect(here)
        if wait.error:
            messages.error(request, wait.error)
        return render(
            request,
            "georivaharvest/recheck_checking.html",
            {"feed": feed, "wait": wait, "again": here, "check": token, **_recheck_chrome(feed)},
        )
    found = wait.found

    # Still the shape the feed was set up for: a feed's shape is fixed (ADR 0044 §1).
    fits = found.harvestable and found.shape.value == feed.upstream_shape
    reason = found.reason or _("This collection is no longer published the way it was when this feed was set up.")
    if feed.is_store:
        return _recheck_store(request, feed, found, fits, reason, token)

    allowed = feed.allowed_asset_hosts
    new = [
        location
        for location in found.proposed_locations
        if feed.pinned_buckets.get(location.bucket) != location.host
        if location.bucket or location.host not in allowed
    ]

    if request.method == "POST" and fits:
        # By place, not by position, and only those the survey shown proposed.
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
        _close_checks(request, feed)
        return redirect("data_feed_detail", pk=feed.pk)

    return render(
        request,
        "georivaharvest/recheck_upstream.html",
        {
            "feed": feed,
            "found": found,
            "fits": fits,
            "reason": reason,
            "new": [{"value": f"{location.bucket}|{location.host}", "host": location.host} for location in new],
            "readable": [asset for asset in found.assets if asset.readable],
            "check": token,
            **_recheck_chrome(feed),
        },
    )


def _close_checks(request, feed) -> None:
    """A save closes every re-check of the feed this session has open: each
    was a survey of the feed as it stood before, and is not saved over it."""
    checks = request.session.get(_RECHECK_KEY, {})
    if checks.pop(str(feed.pk), None) is not None:
        request.session[_RECHECK_KEY] = checks


def _recheck_chrome(feed) -> dict:
    return {
        "breadcrumbs_items": [
            {"url": reverse("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse("data_feed_list"), "label": _("Pull feeds")},
            {"url": reverse("data_feed_detail", kwargs={"pk": feed.pk}), "label": feed.name},
            {"url": None, "label": _("Check the catalog again")},
        ],
        "header_title": _("Check the catalog again — %s") % feed.name,
        "header_icon": "globe",
    }


def _recheck_store(request, feed, found, fits: bool, reason: str, token: str):
    """The re-check of a feed over an Upstream store (ADR 0044 §1, §8, §11).

    What it can find, each put to the operator separately: arrays newly seen
    (or gone), offered to the mapping; places the store's data is newly said to
    live in, pinned, and pins it no longer mentions, removed; a new address
    for the store, adopted — its rows follow at the next harvest, which
    rewrites their hrefs. A verdict that turned into a refusal is recorded on
    the feed: its harvests stop with that reason, and its rows stand, until a
    re-check that finds the store usable again is saved.
    """
    from .models import HarvestFeed

    if not fits:
        if feed.upstream_refusal != reason:
            HarvestFeed.objects.filter(pk=feed.pk).update(upstream_refusal=reason)
        return render(
            request,
            "georivaharvest/recheck_store.html",
            {"feed": feed, "found": found, "fits": False, "reason": reason, "check": token, **_recheck_chrome(feed)},
        )

    store = found.store
    surveyed = {key for key, _label in feed.surveyed_asset_keys}
    offered = {array.path for array in store.offered}
    new_arrays = [array for array in store.offered if array.path not in surveyed]
    gone_arrays = sorted(surveyed - offered)
    places = _chunk_places({"chunk_locations": store.chunk_locations, "chunk_regions": store.chunk_regions})
    new_places = [place for place in places if place["declared"] not in feed.pinned_chunk_locations]
    declared = {place["declared"] for place in places}
    gone_places = [
        {"declared": pinned, "name": pinned.partition("://")[2].rstrip("/")}
        for pinned in feed.pinned_chunk_locations
        if pinned not in declared
    ]
    moved_to = store.href if store.href != feed.store_href else ""
    own = found.locations[0] if found.locations else None
    # A store at a new host is read there only once that host is allowed: adopting the address allows it.
    own_to_pin = None
    if moved_to and own is not None and own.proposed:
        pinned = feed.pinned_buckets.get(own.bucket) == own.host if own.bucket else own.host in feed.allowed_asset_hosts
        own_to_pin = None if pinned else own

    if request.method == "POST":
        posted = request.POST
        changed = []
        allow, forget = set(posted.getlist("allow_place")), set(posted.getlist("forget_place"))
        pins = dict(feed.pinned_chunk_locations)
        for place in new_places:
            if place["usable"] and place["declared"] in allow:
                pins[place["declared"]] = place["region"]
        for place in gone_places:
            if place["declared"] in forget:
                pins.pop(place["declared"], None)
        if pins != feed.pinned_chunk_locations:
            feed.pinned_chunk_locations = pins
            changed.append("pinned_chunk_locations")
        if moved_to and posted.get("adopt_store") == moved_to:
            feed.store_href = moved_to
            changed.append("store_href")
            if own_to_pin is not None:
                buckets, hosts = found.pins([own_to_pin])
                feed.pinned_buckets = {**feed.pinned_buckets, **buckets}
                feed.asset_hosts = sorted({*feed.asset_hosts, *hosts})
                changed += ["pinned_buckets", "asset_hosts"]
        if (new_arrays or gone_arrays) and posted.get("offer_arrays"):
            feed.surveyed_assets = _seen_arrays(store.arrays)
            changed.append("surveyed_assets")
        if feed.upstream_refusal:
            # Seen usable again, and saved by the operator: its harvests may run.
            feed.upstream_refusal = ""
            changed.append("upstream_refusal")
        if changed:
            feed.save(update_fields=changed)
        if "pinned_chunk_locations" in changed:
            _republish(feed)
        # The full pass over every run in the window, closed ones too: a store
        # rewritten upstream is noticed here, and an adopted address followed.
        transaction.on_commit(feed.run_now)
        if set(changed) - {"upstream_refusal"}:
            messages.success(request, _("The feed now follows what you confirmed. GeoRiva is looking at it again."))
        else:
            messages.success(request, _("The catalog was checked again. GeoRiva is looking at its data again."))
        _close_checks(request, feed)
        return redirect("data_feed_detail", pk=feed.pk)

    return render(
        request,
        "georivaharvest/recheck_store.html",
        {
            "feed": feed,
            "found": found,
            "fits": True,
            "reason": reason,
            "new_arrays": new_arrays,
            "gone_arrays": gone_arrays,
            "unoffered": [array for array in store.arrays if not array.offered],
            "new_places": new_places,
            "gone_places": gone_places,
            "moved_to": moved_to,
            "new_host": own_to_pin.host if own_to_pin is not None else "",
            "check": token,
            **_recheck_chrome(feed),
        },
    )


def _republish(feed) -> None:
    """Tell the tile server again what it reads each of the feed's Store assets
    with, now that its Pinned locations changed (ADR 0044 §9)."""
    from georiva.core.machine_plane.remote_assets import publish_variable

    collection = feed.collection
    if collection is None:
        return
    for variable in feed.mapped_variables(collection).values():
        publish_variable(variable)
