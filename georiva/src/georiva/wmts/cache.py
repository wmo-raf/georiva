"""The shared cache of the anonymous capabilities document (#354, slice #361).

Capabilities is what a legacy client asks for first and asks for again on every
connection, and the document is the expensive one to build: it walks every
visible collection and enumerates every valid time and every run each of them
holds, deliberately uncapped. Since every anonymous caller of an organisation
receives the same bytes, they are built once and held in Redis for a few
minutes rather than rebuilt per connection.

*Only* the anonymous one. A credentialed document is widened by ``visible_to``
to the caller's private layers and may carry that caller's own key in every URL
(#360): personal, and never into an entry somebody else reads. That is the same
line the response's ``Cache-Control: private`` draws, so :func:`is_personal`
draws it once for both — a cache that disagreed with the header would be a
shared entry the header had already promised was nobody else's.

Keys are org-first, as every machine-plane key is (``georiva:palette:{org}:…``):
catalog slugs are unique only within an organisation, and each document's URLs
are absolute against that organisation's own host, so two institutions meeting
in one entry would serve each other's layers under each other's hostname.

What the cache costs is a document that lags: an item ingested a minute ago
appears when the entry expires and no sooner. #354 accepts that explicitly —
the TTL is minutes, and the newest run moving a few minutes late is invisible
to a human loading a client. An operator who needs it gone sets the TTL to 0.

The organisation's *hostname* lags by the same TTL and for the same reason: the
key names the organisation, not the Site row whose ``root_url`` every URL in
the document is absolute against. Renaming a host therefore keeps handing out
the old one for a few minutes, which is the smaller half of a change that
already breaks in-flight clients outright.
"""
import logging

from django.conf import settings
from django.core.cache import cache

from .capabilities import build_capabilities

logger = logging.getLogger(__name__)

#: Cache-framework key prefix, and therefore what the tests sweep. Django's own
#: ``KEY_PREFIX``/``VERSION`` are prepended underneath: nothing outside Django
#: reads this entry, unlike the palette keys Titiler goes to Redis for itself.
CAPABILITIES_KEY_PREFIX = "wmts:capabilities"


def is_personal(request) -> bool:
    """Whether this document belongs to its caller alone.

    Any credential makes it so, whatever the transport: an API key and a
    session widen the listing through the same ``visible_to``, and a query-borne
    key is written into the document's URLs as well. Anonymous is the one case
    where the bytes are the same for everybody.
    """
    return request.user.is_authenticated


def capabilities_cache_key(organisation) -> str:
    return f"{CAPABILITIES_KEY_PREFIX}:{organisation.slug}"


def capabilities_document(request, organisation) -> bytes:
    """The document to answer ``request`` with, shared when it may be shared."""
    if is_personal(request):
        return build_capabilities(request, organisation)

    timeout = settings.GEORIVA_WMTS_CAPABILITIES_CACHE_SECONDS
    if timeout <= 0:
        # Off, rather than stored and expired in the same breath: an operator
        # chasing a wrong-looking document wants no entry to exist at all.
        return build_capabilities(request, organisation)

    key = capabilities_cache_key(organisation)
    document = _cached_document(key)
    if document is None:
        document = build_capabilities(request, organisation)
        _store_document(key, document, timeout)
    return document


# Both halves are best-effort, in the pattern of the palette cache: this cache
# is a saving, and a saving that is unavailable must cost a slow document, not
# a failed one. Discovery worked without an entry before #361 and still does —
# a legacy client left unable to list any layer because Redis blinked would be
# a far worse outage than the rebuild it was meant to spare.


def _cached_document(key):
    try:
        return cache.get(key)
    except Exception as e:
        logger.warning("Failed to read the cached capabilities at %s: %s", key, e)
        return None


def _store_document(key, document, timeout):
    try:
        cache.set(key, document, timeout=timeout)
    except Exception as e:
        logger.warning("Failed to cache the capabilities at %s: %s", key, e)
