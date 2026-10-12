"""Task base classes that carry the event context across the queue (ADR 0030).

Three kinds, and every core task declares one:

* :class:`EntryTask` — always starts a fresh chain (scheduled feed fetches,
  async-job API tasks). Actor ``celery``.
* :class:`SweepTask` — starts a ``sweep:<name>`` context; the sweep adopts the
  context of each run it revives.
* :class:`ChainedTask` — the default for work an arrival set in motion. Reads the
  four context values from task headers on entry; anything it enqueues is
  published with the same headers (see :func:`inject_headers`).

Plus :class:`ReadSideTask` for artefact tasks (virtual Zarr manifests, zonal
statistics): they inherit a context when one arrives and never require one, and
write no Domain events — a traceability gap must not become a serving outage.

Every class resets the context when the task ends, however it ends, so one
worker process never attributes a task's events to the previous task's
organisation or user. A task declared with a plain ``@app.task`` keeps working
under both strictness modes — it never checks for context — but it silently
drops the chain; a plugin that adopts Domain events moves its tasks onto these
classes.

Part of the supported plugin API.
"""

import logging
import uuid

from celery import Task
from celery.signals import before_task_publish
from django.conf import settings

from .context import EventContext, current_context, new_context, reset_context, set_context
from .metrics import increment_unpropagated

logger = logging.getLogger(__name__)

CORRELATION_HEADER = "georiva_correlation_id"
CAUSED_BY_HEADER = "georiva_caused_by"
ACTOR_HEADER = "georiva_actor"
ORGANISATION_HEADER = "georiva_organisation_id"
HEADER_NAMES = (CORRELATION_HEADER, CAUSED_BY_HEADER, ACTOR_HEADER, ORGANISATION_HEADER)

UNPROPAGATED_ACTOR = "system:unpropagated"
WARN = "warn"
STRICT = "strict"


class MissingTaskContext(RuntimeError):
    """A chained task ran with no propagated context, under the strict setting."""


def context_mode() -> str:
    return getattr(settings, "GEORIVA_TASK_CONTEXT_MODE", WARN)


def context_to_headers(context: EventContext) -> dict:
    headers = {
        CORRELATION_HEADER: str(context.correlation_id),
        ACTOR_HEADER: context.actor,
    }
    if context.caused_by:
        headers[CAUSED_BY_HEADER] = str(context.caused_by)
    if context.organisation_id is not None:
        headers[ORGANISATION_HEADER] = str(context.organisation_id)
    return headers


def context_from_headers(headers) -> EventContext | None:
    """The context carried by ``headers``, or ``None`` when it carries none."""
    correlation = headers.get(CORRELATION_HEADER)
    actor = headers.get(ACTOR_HEADER)
    if not correlation or not actor:
        return None
    caused_by = headers.get(CAUSED_BY_HEADER)
    organisation = headers.get(ORGANISATION_HEADER)
    return EventContext(
        correlation_id=uuid.UUID(str(correlation)),
        actor=actor,
        organisation_id=int(organisation) if organisation not in (None, "") else None,
        caused_by=uuid.UUID(str(caused_by)) if caused_by else None,
    )


def _request_headers(request) -> dict:
    """Custom headers as the worker (flattened onto the request) or eager mode (``headers``) holds them."""
    found = dict(getattr(request, "headers", None) or {})
    for name in HEADER_NAMES:
        value = getattr(request, name, None)
        if value is not None:
            found.setdefault(name, value)
    return found


@before_task_publish.connect
def inject_headers(headers=None, **kwargs):
    """Stamp the active context onto every task published from inside it.

    Connected to the publish signal rather than to a base class so that dispatch
    sites need no change: ``task.delay(...)`` from within a context carries it.
    Headers already set explicitly win.
    """
    context = current_context()
    if context is None or headers is None:
        return
    for name, value in context_to_headers(context).items():
        headers.setdefault(name, value)


class _ContextTask(Task):
    abstract = True

    def initial_context(self) -> EventContext | None:
        raise NotImplementedError

    def __call__(self, *args, **kwargs):
        token = set_context(self.initial_context())
        try:
            return super().__call__(*args, **kwargs)
        finally:
            reset_context(token)


class EntryTask(_ContextTask):
    """Always starts a fresh chain, whatever context the caller was in."""

    abstract = True

    def initial_context(self):
        return new_context(actor="celery")


class SweepTask(_ContextTask):
    """Starts a ``sweep:<name>`` chain; declare ``sweep_name`` on the task."""

    abstract = True
    sweep_name = None

    def initial_context(self):
        return new_context(actor=f"sweep:{self.sweep_name or self.name.rsplit('.', 1)[-1]}")


class ChainedTask(_ContextTask):
    """Continues the chain its dispatcher was in. The default for arrival-driven work."""

    abstract = True

    def initial_context(self):
        carried = context_from_headers(_request_headers(self.request))
        if carried is not None:
            return carried
        inherited = current_context()
        if inherited is not None:
            # Run inline (eager, or called directly) inside a context: nothing was lost.
            return inherited
        return self._unpropagated()

    def _unpropagated(self):
        message = f"Task {self.name} ran without propagated context"
        if context_mode() == STRICT:
            raise MissingTaskContext(message)
        logger.error("%s; continuing as %s", message, UNPROPAGATED_ACTOR)
        increment_unpropagated()
        return new_context(actor=UNPROPAGATED_ACTOR)


class ReadSideTask(_ContextTask):
    """Inherits a context when one arrives; never requires one and never fails for lack of it."""

    abstract = True

    def initial_context(self):
        return context_from_headers(_request_headers(self.request)) or current_context()
