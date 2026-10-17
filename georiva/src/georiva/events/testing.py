"""Helpers for tests that record or run under Domain events. Part of the plugin API."""

from .context import new_context, use_context


def event_context(*, actor="system:test", organisation_id=None, **kwargs):
    """``with event_context(...):`` -- a fresh chain for the block."""
    return use_context(new_context(actor=actor, organisation_id=organisation_id, **kwargs))
