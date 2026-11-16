"""
A collection's Upstream survey, run off the request (#667, #695).

Opening a large Upstream store to see what it is packed with downloads its
whole description first — GFS's took ~16 s cold, and its verdict ~30 s in all —
which is too close to a web worker's timeout to make inside a request, and no
concurrency shortens one download. So the Harvest Feed Wizard and "Re-check
upstream" start the survey here, on `georiva-default`, and wait for it,
saying what it is waiting on.

What a survey finds — the `CollectionSurvey` itself — is kept in the cache
under a token only the operator's session holds, and nowhere else: a survey
still writes nothing — no feed, no row (ADR 0042 §2). A re-check saves
against that same survey, so what is saved is what the operator was shown.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import asdict

from django.core.cache import cache
from django.utils.translation import gettext as _

from .survey import WAITING_TO_START, survey_collection
from .upstream import UpstreamTimedOut, UpstreamUnreachable

logger = logging.getLogger(__name__)

#: How long what a survey found is kept for the wizard to collect.
KEEP_SECONDS = 60 * 60

#: How long the wizard waits for a survey before it stops waiting: past the
#: slowest one measured, and the timeouts of every read it makes.
GIVE_UP_SECONDS = 180

CHECKING = "checking"
FOUND = "found"
FAILED = "failed"


def _key(token: str) -> str:
    return f"georiva:harvest:survey:{token}"


def _put(token: str, **state) -> None:
    cache.set(_key(token), state, timeout=KEEP_SECONDS)


def start(upstream_url: str, collection_id: str) -> str:
    """Start surveying `collection_id` of the Upstream at `upstream_url`, and
    return the token its answer will be kept under."""
    from .tasks import survey_upstream_collection

    token = uuid.uuid4().hex
    _put(token, state=CHECKING, waiting_on=WAITING_TO_START)
    survey_upstream_collection.delay(token, upstream_url, collection_id)
    return token


def collect(token: str) -> dict | None:
    """What the survey under `token` has come to: `state` is `CHECKING` (with
    `waiting_on`), `FOUND` (with `found`, the `CollectionSurvey`) or
    `FAILED` (with `why`, and `timed_out` for a wait worth trying again).
    `None` when nothing usable is kept under it: never started, long gone, or
    kept by an older GeoRiva in a shape this one cannot read back."""
    try:
        return cache.get(_key(token))
    except Exception:  # an answer that cannot be read back is a lost one: the operator checks again
        logger.warning("Survey answer under %s could not be read back", token, exc_info=True)
        return None


def run(token: str, upstream_url: str, collection_id: str) -> None:
    """Survey the collection and keep what was found, or why nothing was."""
    from .models import HarvestFeed

    def progress(text: str) -> None:
        _put(token, state=CHECKING, waiting_on=text)

    client = HarvestFeed(upstream_url=upstream_url).survey_client()
    try:
        found = survey_collection(client, upstream_url, collection_id, progress=progress)
    except UpstreamTimedOut as late:
        _put(token, state=FAILED, why=str(late), timed_out=True)
    except UpstreamUnreachable as why:
        _put(token, state=FAILED, why=str(why), timed_out=False)
    except Exception:  # an answer the survey could not make sense of is still an answer for the operator
        logger.exception("Survey of %s at %s failed", collection_id, upstream_url)
        _put(token, state=FAILED, why=_("it answered in a way GeoRiva could not make sense of"), timed_out=False)
    else:
        _put(token, state=FOUND, found=found)


def as_session(found) -> dict:
    """A collection's survey as the wizard's session keeps it between steps."""
    store = None
    if found.store:
        store = {**asdict(found.store), "has_runs": found.store.has_runs}
        store["arrays"] = [{**asdict(array), "offered": array.offered} for array in found.store.arrays]
    return {
        "shape": found.shape.value,
        "harvestable": found.harvestable,
        "store": store,
        "reason": found.reason,
        "items_sampled": found.items_sampled,
        "assets": [asdict(asset) for asset in found.assets],
        "locations": [{**asdict(location), "proposed": location.proposed} for location in found.locations],
    }
