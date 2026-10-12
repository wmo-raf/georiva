"""The context every Domain event is recorded under (ADR 0030).

Four values travel together: the Correlation shared by one chain, the Caused-by
pointer to the event that directly triggered the work, the Actor, and the owning
organisation. They live in a context variable so a writer deep inside a model
method can read them without every caller threading them through — and so the
Derivation engine can carry them as an opaque object without ever resolving a
tenancy of its own.

A missing context is loud (:class:`NoEventContext`); a *stale* one is silent and
misattributes tenancy, which is why every entry point sets it through
:func:`use_context` and the task base classes reset it however a task ends.

This module is part of the supported plugin API.
"""

import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace


class NoEventContext(RuntimeError):
    """A Domain event was recorded with no context set."""


@dataclass(frozen=True)
class EventContext:
    correlation_id: uuid.UUID
    actor: str
    organisation_id: int | None = None
    caused_by: uuid.UUID | None = None

    def caused_by_event(self, event) -> "EventContext":
        """The same chain, with ``event`` as the direct cause of what comes next."""
        return replace(self, caused_by=event.pk)

    def with_actor(self, actor: str) -> "EventContext":
        return replace(self, actor=actor)


_current: ContextVar[EventContext | None] = ContextVar("georiva_event_context", default=None)


def new_context(*, actor: str, organisation_id=None, correlation_id=None, caused_by=None) -> EventContext:
    """A context for a brand-new chain unless ``correlation_id`` continues one."""
    return EventContext(
        correlation_id=correlation_id or uuid.uuid4(),
        actor=actor,
        organisation_id=organisation_id,
        caused_by=caused_by,
    )


def starting_correlation(*, continuing=None) -> uuid.UUID:
    """The Correlation a new tracking row should carry: ``continuing`` if given,
    else the active chain's, else a new one."""
    if continuing is not None:
        return continuing
    context = _current.get()
    return context.correlation_id if context else uuid.uuid4()


def current_context() -> EventContext | None:
    return _current.get()


def require_context() -> EventContext:
    context = _current.get()
    if context is None:
        raise NoEventContext(
            "A Domain event was recorded with no context set. Entry points set one with "
            "georiva.events.context.use_context(); see ADR 0030."
        )
    return context


def set_context(context: EventContext | None):
    """Set the context; returns the token :func:`reset_context` needs."""
    return _current.set(context)


def reset_context(token) -> None:
    _current.reset(token)


def update_context(**changes) -> EventContext:
    """Replace fields of the active context in place (it is reset when the task ends).

    For entry points that learn the organisation only after they start — a feed's
    fetch task knows it once it has loaded the feed.
    """
    context = require_context()
    updated = replace(context, **changes)
    _current.set(updated)
    return updated


@contextmanager
def use_context(context: EventContext):
    """Run a block under ``context``, restoring whatever was there however it ends."""
    token = _current.set(context)
    try:
        yield context
    finally:
        _current.reset(token)


@contextmanager
def ensure_context(*, actor: str, organisation_id=None):
    """Keep the active context, or start a fresh chain for the duration of the block.

    For transition sites reachable both from a context-setting entry point and
    from code that has none (tests, shells). An existing context always wins.
    """
    existing = _current.get()
    if existing is not None:
        yield existing
        return
    with use_context(new_context(actor=actor, organisation_id=organisation_id)) as context:
        yield context


@contextmanager
def new_chain(*, organisation_id, actor=None):
    """A fresh chain for one subject, keeping the surrounding Actor.

    For an entry or sweep task that handles subjects of several organisations in
    one run: a chain is single-organisation, so each subject gets its own.
    """
    current = _current.get()
    actor = actor or (current.actor if current else "celery")
    with use_context(new_context(actor=actor, organisation_id=organisation_id)) as context:
        yield context


@contextmanager
def caused_by(event):
    """Run a block with ``event`` as the cause of whatever it records."""
    with use_context(require_context().caused_by_event(event)) as context:
        yield context
