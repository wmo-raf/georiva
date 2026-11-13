import logging

from georiva.config.celery import app
from georiva.events.tasks import EntryTask

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
    """
    from georiva.events.context import update_context
    from georiva.harvest.harvester import harvest
    from georiva.harvest.models import HarvestFeed
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

    # An entry point: the chain this harvest starts belongs to the feed's organisation.
    update_context(organisation_id=feed.catalog.organisation_id)

    # An operator's own run re-checks a store's closed runs too: how a step gone
    # from one is found (ADR 0044 §6). A schedule and a retry ask only open ones.
    recheck = not scheduled and resume_of_run_id is None
    return harvest(feed, resumed_from=resumed_from, recheck=recheck).to_dict()
