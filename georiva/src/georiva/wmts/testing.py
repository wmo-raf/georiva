"""Shared arrangements for the capabilities suites.

Three of them, all here for the reason ``core.testing`` and
``organisations.testing`` exist: an arrangement two suites build separately is
an arrangement that can drift between them.

*Cache isolation.* The anonymous document is held per organisation for minutes
at a time (#361), and the test runner rolls back the database between tests but
not Redis. Every test in this app dials the same organisation, so without a
sweep the second test of a run reads the document the first test's fixtures
produced — and the first test of the next run reads one from the run before it.
A class that fetches capabilities therefore mixes in
:class:`IsolatedCapabilitiesCache`, and a guard test holds the package to it.
The sweep is by key prefix rather than a flush, following the palette-cache
tests: these suites run against the developer's own Redis, which is also
holding a live stack's broker.

*The three tiers*, which every credential-facing test needs, and
*the reader*, which turns a response back into the layers it advertises.

Not a test module itself — it lives beside the app, in the pattern of
``organisations.testing``, so it is importable without reaching into a
``tests`` package.
"""

from xml.etree import ElementTree as ET

from django.core.cache import cache
from django.urls import reverse

from .cache import CAPABILITIES_KEY_PREFIX

NS = {
    "wmts": "http://www.opengis.net/wmts/1.0",
    "ows": "http://www.opengis.net/ows/1.1",
    "xlink": "http://www.w3.org/1999/xlink",
}

#: The layer identifiers :func:`make_tiered_catalog` produces, for the two
#: tiers that are ever served. The internal one has no constant on purpose:
#: no test should ever be asserting that it appears.
PUBLIC_LAYER = "forecast:temperature:t2m"
PRIVATE_LAYER = "forecast:members-only:t2m"


def clear_capabilities_cache():
    """Drop every organisation's cached capabilities document."""
    cache.delete_pattern(f"{CAPABILITIES_KEY_PREFIX}:*")


class IsolatedCapabilitiesCache:
    """Give each test an empty capabilities cache. Mix in before ``TestCase``.

    A subclass writing its own ``setUp`` must call ``super().setUp()``, or its
    tests inherit whatever the previous one left cached.
    """

    def setUp(self):
        super().setUp()
        clear_capabilities_cache()
        self.addCleanup(clear_capabilities_cache)


class CapabilitiesReader(IsolatedCapabilitiesCache):
    """Fetch ``self.organisation``'s document and read its layers back.

    For the suites that ask *which* layers a caller is shown rather than how
    one layer is described — visibility, credentials, and what a shared cache
    entry may hold.
    """

    PUBLIC = PUBLIC_LAYER
    PRIVATE = PRIVATE_LAYER

    def fetch(self, params=None, **extra):
        response = self.client.get(
            reverse("wmts:capabilities", args=[self.organisation.slug]),
            params or {},
            **extra,
        )
        self.assertEqual(response.status_code, 200)
        return response

    def layers(self, response):
        document = ET.fromstring(response.content)
        return document.findall("wmts:Contents/wmts:Layer", NS)

    def identifiers(self, response):
        return [layer.findtext("ows:Identifier", namespaces=NS) for layer in self.layers(response)]

    def layer(self, response, identifier):
        for layer in self.layers(response):
            if layer.findtext("ows:Identifier", namespaces=NS) == identifier:
                return layer
        self.fail(f"No layer {identifier!r} in the document")

    def operations(self, response):
        """Advertised KVP endpoint per operation name (#362)."""
        document = ET.fromstring(response.content)
        return {
            operation.get("name"): operation.find("ows:DCP/ows:HTTP/ows:Get", NS).get(f"{{{NS['xlink']}}}href")
            for operation in document.findall(
                "ows:OperationsMetadata/ows:Operation",
                NS,
            )
        }

    def templates(self, response):
        """Advertised tile-URL template per layer identifier."""
        return {
            layer.findtext("ows:Identifier", namespaces=NS): layer.find("wmts:ResourceURL", NS).get("template")
            for layer in self.layers(response)
        }


def make_tiered_catalog(organisation, slug="forecast"):
    """One catalog under ``organisation`` holding a collection at each
    visibility tier, each carrying a ``t2m`` variable.

    The arrangement every credential-facing capabilities test needs: the
    document lists the public collection for anybody, adds the private one for
    a member of this organisation, and never mentions the internal one (ADR
    0014). Built once here so the three tiers cannot drift between the suite
    guarding what a key reveals (#360) and the suite guarding what a shared
    cache entry may hold (#361).

    Returns the pieces by name, in the manner of ``make_org_tree``.
    """
    from georiva.core.models import Catalog, Collection, Unit, Variable

    catalog = Catalog.objects.create(
        organisation=organisation,
        name="Forecast",
        slug=slug,
        file_format="geotiff",
    )
    unit, _ = Unit.objects.get_or_create(name="Celsius", symbol="C")
    tiers = {}
    for visibility, name, collection_slug, variable_name in (
        (Collection.Visibility.PUBLIC, "Temperature", "temperature", "2m Temperature"),
        (Collection.Visibility.PRIVATE, "Members only", "members-only", "private t2m"),
        (Collection.Visibility.INTERNAL, "Intermediate", "intermediate", "internal t2m"),
    ):
        collection = Collection.objects.create(
            catalog=catalog,
            name=name,
            slug=collection_slug,
            visibility=visibility,
        )
        Variable.objects.create(
            collection=collection,
            slug="t2m",
            name=variable_name,
            unit=unit,
            value_min=0,
            value_max=50,
        )
        tiers[visibility.value] = collection
    return {"catalog": catalog, "unit": unit, **tiers}
