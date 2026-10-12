"""The event-type registry, populated at app-ready (ADR 0030).

Every concrete subclass of :class:`~georiva.events.models.DomainEvent` in an
installed app is an event type. The Timeline and failures panel read the index
table and then fetch details *per type* from this registry, so a deployment
without a given plugin never queries that plugin's tables — and a type whose
table is absent is skipped rather than fatal.

Part of the supported plugin API.
"""

from django.apps import apps

_types: dict[str, type] = {}


def type_key(model) -> str:
    return model._meta.label_lower


def populate() -> None:
    from .models import DomainEvent

    _types.clear()
    for model in apps.get_models():
        if issubclass(model, DomainEvent) and not model._meta.abstract:
            _types[type_key(model)] = model


def installed_tables() -> set[str]:
    """Tables that exist right now — a registered type may belong to a plugin that was removed."""
    from django.db import connection

    return set(connection.introspection.table_names())


def event_types() -> list[type]:
    return list(_types.values())


def model_for(key: str):
    return _types.get(key)
