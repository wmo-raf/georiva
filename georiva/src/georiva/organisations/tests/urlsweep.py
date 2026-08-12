"""Walking the registered URL tree, for the guard tests that sweep it.

Both fail-closed sweeps — the admin one and the public-API one — generate their
targets from the URLs the project actually registers rather than from a list
somebody maintains. That is the whole point of them: a view added next month is
covered on the day it is registered. This is the one piece they share.
"""

from django.urls import URLPattern, URLResolver


def flatten_url_patterns(patterns, prefix=""):
    """Yield ``(pattern, prefix)`` for every leaf URL under ``patterns``."""
    for entry in patterns:
        if isinstance(entry, URLResolver):
            yield from flatten_url_patterns(entry.url_patterns, prefix + str(entry.pattern))
        elif isinstance(entry, URLPattern):
            yield entry, prefix
