"""Helpers shared by tests that record Domain events."""

from georiva.events.context import new_context, use_context


def event_context(*, actor="system:test", organisation_id=None, **kwargs):
    """``with event_context(...):`` — a fresh chain for the block."""
    return use_context(new_context(actor=actor, organisation_id=organisation_id, **kwargs))
