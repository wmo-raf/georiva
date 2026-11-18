import logging

from georiva.config.celery import app
from georiva.events.tasks import ChainedTask, EntryTask

logger = logging.getLogger(__name__)


@app.task(
    name="georiva.harvest.tasks.run_harvest",
    base=EntryTask,
    queue="georiva-default",
    bind=True,
    acks_late=True,
    max_retries=0,
)
def run_harvest(self, data_feed_id, resume_of_run_id=None, scheduled=True):
    """
    Harvest one Harvest feed's window. `scheduled=False` is an operator's
    own run, which re-checks every run of a store's window, closed ones too.

    On `georiva-default`, never the ingestion queue: a harvest writes rows, and
    no file waits on it (ADR 0042 §11). Not retried by Celery — a harvest that
    stops is due its retry on the run itself, and the recovery sweep dispatches
    it here with `resume_of_run_id` (ADR 0031).

    One harvest of a feed at a time: while another runs, this one does not
    start — a retry stays due, for the sweep to dispatch again. A retry is not
    run at all once a later harvest has succeeded: a retry harvests the whole
    window again, which that harvest has already done (#683).
    """
    from georiva.events.context import update_context
    from georiva.harvest.harvester import harvest
    from georiva.harvest.models import HarvestFeed
    from georiva.sources.fetch_run_spine import FeedAlreadyRunning
    from georiva.sources.models import FetchRun

    try:
        feed = HarvestFeed.objects.get(pk=data_feed_id)
    except HarvestFeed.DoesNotExist:
        logger.warning("run_harvest: harvest feed %s gone", data_feed_id)
        return None

    if feed.catalog_id is None:
        logger.warning("run_harvest: harvest feed %s has no catalog to harvest into", data_feed_id)
        return None

    # A paused feed's schedule does not fire; a retry or an operator's own run still may.
    if scheduled and resume_of_run_id is None and not feed.is_active:
        return None

    resumed_from = None
    if resume_of_run_id is not None:
        resumed_from = FetchRun.objects.filter(pk=resume_of_run_id, data_feed_id=feed.pk).first()
        # Dispatched twice, or the run is gone: there is nothing left to retry.
        if resumed_from is None or resumed_from.resumes.exists():
            logger.info("run_harvest: run %s already retried or gone — not retrying", resume_of_run_id)
            return None
        if resumed_from.superseded():
            # Not due any more: the sweep would otherwise dispatch it again at every pass.
            resumed_from.drop_retry()
            logger.info("run_harvest: run %s superseded by a later successful harvest — not retrying", resumed_from.pk)
            return None

    # An entry point: the chain this harvest starts belongs to the feed's organisation.
    update_context(organisation_id=feed.catalog.organisation_id)

    # An operator's own run re-checks a store's closed runs too: how a step gone
    # from one is found (ADR 0044 §6). A schedule and a retry ask only open ones.
    recheck = not scheduled and resume_of_run_id is None
    try:
        return harvest(feed, resumed_from=resumed_from, recheck=recheck).to_dict()
    except FeedAlreadyRunning as busy:
        logger.info("run_harvest: harvest feed %s not harvested — run %s is still running", feed.pk, busy.running.pk)
        return None


@app.task(
    name="georiva.harvest.tasks.survey_upstream_collection",
    base=EntryTask,
    queue="georiva-default",
    bind=True,
    acks_late=True,
    max_retries=0,
)
def survey_upstream_collection(self, token, upstream_url, collection_id):
    """
    Survey one collection of an Upstream for the Harvest Feed Wizard, which
    waits on `token` for the answer (#667). It writes nothing but that answer,
    to the cache. Not retried: an operator waiting on it tries again.

    On `georiva-default`: it reads an Upstream, and no file waits on it.
    """
    from georiva.harvest.background_survey import run

    run(token, upstream_url, collection_id)


@app.task(
    name="georiva.harvest.tasks.materialize_harvested_asset",
    base=ChainedTask,
    queue="georiva-ingestion",
    bind=True,
    acks_late=True,
    max_retries=0,
)
def materialize_harvested_asset(self, asset_id, lane=None):
    """
    Fetch one Remote asset of a Harvest feed's materialization window and
    ingest it into our storage, in place (ADR 0042 §8), then send the feed's
    next asset due in its `lane` (#727).

    On `georiva-ingestion`, like any fetch and extraction: until it runs, the
    file is not ours. One per feed at a time, each sending the next to the back
    of the queue, so a deep window never sits in front of a fresh file. The
    harvest that opens the lane stays on `georiva-default`. Not retried by
    Celery: an asset that could not be had stays Remote and due, and is sent
    again once its claim has lapsed.
    """
    from georiva.harvest.materialize import materialize_in_lane

    return materialize_in_lane(asset_id, lane)
