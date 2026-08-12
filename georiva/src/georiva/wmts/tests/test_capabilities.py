"""The org-wide capabilities document, held to what discovery may reveal.

WMTS exists to be pasted into clients GeoRiva does not control, so everything
here asserts at the HTTP boundary — status, parsed XML, headers — following the
STAC visibility tests: the document lists exactly the collections ``visible_to``
would serve this caller, and every URL in it is the machine-plane grammar read
back verbatim.
"""
from datetime import datetime, timezone
from urllib.parse import urlsplit

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from xml.etree import ElementTree as ET

from georiva.accounts.models import ApiKey
from georiva.core.machine_plane import (
    MachineScope,
    scope_of,
    wmts_capabilities_url,
    wmts_kvp_endpoint,
    wmts_rest_featureinfo_template,
    wmts_rest_tile_template,
)
from georiva.core.models import (
    Catalog, Collection, Item, Unit, Variable, VariableStyle,
)
from georiva.organisations.testing import (
    dial_org, join_org, make_organisation, org_host,
)
from georiva.wmts.testing import (
    NS, PUBLIC_LAYER, CapabilitiesReader, IsolatedCapabilitiesCache,
    make_tiered_catalog,
)


class WMTSCapabilitiesTests(IsolatedCapabilitiesCache, TestCase):
    def setUp(self):
        super().setUp()
        dial_org(self.client)
        self.organisation = make_organisation()
        self.catalog = Catalog.objects.create(
            organisation=self.organisation,
            name="Forecast", slug="forecast", file_format="geotiff",
        )
        unit = Unit.objects.create(name="Celsius", symbol="C")

        self.public = Collection.objects.create(
            catalog=self.catalog, name="Temperature", slug="temperature",
            visibility=Collection.Visibility.PUBLIC,
            bounds=[20.0, -12.0, 52.0, 23.0],
        )
        self.variable = Variable.objects.create(
            collection=self.public, slug="t2m", name="2m Temperature",
            unit=unit, value_min=0, value_max=50,
        )
        Item.objects.create(
            collection=self.public,
            time=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc),
        )
        Item.objects.create(
            collection=self.public,
            time=datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc),
        )

        private = Collection.objects.create(
            catalog=self.catalog, name="Members only", slug="members-only",
            visibility=Collection.Visibility.PRIVATE,
        )
        Variable.objects.create(
            collection=private, slug="t2m", name="private t2m",
            unit=unit, value_min=0, value_max=50,
        )
        internal = Collection.objects.create(
            catalog=self.catalog, name="Intermediate", slug="intermediate",
            visibility=Collection.Visibility.INTERNAL,
        )
        Variable.objects.create(
            collection=internal, slug="t2m", name="internal t2m",
            unit=unit, value_min=0, value_max=50,
        )

    def fetch(self):
        response = self.client.get(
            reverse("wmts:capabilities", args=[self.organisation.slug])
        )
        self.assertEqual(response.status_code, 200)
        return ET.fromstring(response.content)

    def layers(self, document):
        return document.findall("wmts:Contents/wmts:Layer", NS)

    def layer_identifiers(self, document):
        return [
            layer.findtext("ows:Identifier", namespaces=NS)
            for layer in self.layers(document)
        ]

    def test_the_route_spells_what_the_machine_plane_builder_writes(self):
        self.assertEqual(
            reverse("wmts:capabilities", args=[self.organisation.slug]),
            wmts_capabilities_url(self.organisation),
        )

    def test_public_variables_appear_private_and_internal_do_not(self):
        identifiers = self.layer_identifiers(self.fetch())
        self.assertEqual(identifiers, ["forecast:temperature:t2m"])

    def test_a_private_collection_widens_nothing_for_an_anonymous_caller(self):
        """Same fixture, flipped the other way: making the public collection
        private empties the document rather than 404ing it."""
        Collection.objects.filter(pk=self.public.pk).update(
            visibility=Collection.Visibility.PRIVATE,
        )
        self.assertEqual(self.layer_identifiers(self.fetch()), [])

    def test_layers_advertise_json_as_their_only_info_format(self):
        """What an identify tool is promised it may ask for (#363).

        The KVP endpoint answers GetFeatureInfo as ``application/json`` and
        nothing else, so the layer offers exactly that: a client shown a
        format the endpoint refuses would send its identify into an exception
        it had been told would work.
        """
        (layer,) = self.layers(self.fetch())
        self.assertEqual(
            [f.text for f in layer.findall("wmts:InfoFormat", NS)],
            ["application/json"],
        )

    def test_layers_advertise_webmercatorquad_and_png_only(self):
        document = self.fetch()
        (layer,) = self.layers(document)
        self.assertEqual(
            [f.text for f in layer.findall("wmts:Format", NS)], ["image/png"],
        )
        self.assertEqual(
            [
                link.findtext("wmts:TileMatrixSet", namespaces=NS)
                for link in layer.findall("wmts:TileMatrixSetLink", NS)
            ],
            ["WebMercatorQuad"],
        )
        matrix_sets = document.findall("wmts:Contents/wmts:TileMatrixSet", NS)
        self.assertEqual(
            [t.findtext("ows:Identifier", namespaces=NS) for t in matrix_sets],
            ["WebMercatorQuad"],
        )

    def test_the_resource_url_is_the_machine_plane_template_made_absolute(self):
        (layer,) = self.layers(self.fetch())
        resource = layer.find("wmts:ResourceURL", NS)
        self.assertEqual(resource.get("resourceType"), "tile")
        self.assertEqual(resource.get("format"), "image/png")
        self.assertEqual(
            resource.get("template"),
            f"http://{org_host()}"
            + wmts_rest_tile_template(self.variable, ("Time",)),
        )

    def test_a_filled_template_scopes_back_to_the_advertised_collection(self):
        """The templates must resolve against the routes the gateway actually
        authorises: substituting the placeholders — dimensions included — yields
        a URI ``scope_of`` reads as this collection, so a client following the
        document lands on a working tile route."""
        (layer,) = self.layers(self.fetch())
        template = layer.find("wmts:ResourceURL", NS).get("template")
        split = urlsplit(template.format(
            TileMatrix=6, TileCol=38, TileRow=32, Time="2026-03-01T12:00:00Z",
        ))
        self.assertEqual(
            scope_of(f"{split.path}?{split.query}"),
            MachineScope("test-org", "forecast", "temperature"),
        )

    def resource_urls(self, layer):
        return {
            resource.get("resourceType"): resource
            for resource in layer.findall("wmts:ResourceURL", NS)
        }

    def test_the_layer_carries_an_identify_template_beside_the_tile_one(self):
        """#379: the InfoFormat above promises an identify, and until this
        template existed a client reading only ``ResourceURL`` had nowhere to
        send the click — identify was reachable through ``OperationsMetadata``
        and therefore by KVP speakers alone."""
        (layer,) = self.layers(self.fetch())
        resources = self.resource_urls(layer)
        self.assertEqual(sorted(resources), ["FeatureInfo", "tile"])
        info = resources["FeatureInfo"]
        self.assertEqual(info.get("format"), "application/json")
        self.assertEqual(
            info.get("template"),
            f"http://{org_host()}"
            + wmts_rest_featureinfo_template(self.variable, ("Time",)),
        )

    def test_the_identify_template_is_the_tile_template_plus_a_pixel(self):
        """The two must address one layer at one instant in one style, or a
        client identifies a pixel of a tile it is not looking at. Asserted as a
        relationship between the two strings rather than against a literal,
        because what matters is that they cannot drift apart."""
        resources = self.resource_urls(self.layers(self.fetch())[0])
        tile, query = resources["tile"].get("template").split("?")
        info, info_query = resources["FeatureInfo"].get("template").split("?")
        self.assertEqual(info_query, query)
        self.assertEqual(info, tile.removesuffix(".png") + "/{J}/{I}.json")

    def test_a_filled_identify_template_scopes_back_to_the_collection(self):
        """The gate reads an identify request by the same four segments it
        reads a tile by, so the deeper path must still name this collection —
        otherwise the document advertises an address nginx denies."""
        resources = self.resource_urls(self.layers(self.fetch())[0])
        split = urlsplit(resources["FeatureInfo"].get("template").format(
            TileMatrix=6, TileCol=38, TileRow=32, J=128, I=64,
            Time="2026-03-01T12:00:00Z",
        ))
        self.assertEqual(
            scope_of(f"{split.path}?{split.query}"),
            MachineScope("test-org", "forecast", "temperature"),
        )

    def test_a_layer_without_items_still_appears_with_a_bare_template(self):
        Item.objects.all().delete()
        (layer,) = self.layers(self.fetch())
        self.assertNotIn("?", layer.find("wmts:ResourceURL", NS).get("template"))

    def test_the_document_carries_the_collections_extent(self):
        (layer,) = self.layers(self.fetch())
        bbox = layer.find("ows:WGS84BoundingBox", NS)
        self.assertEqual(bbox.findtext("ows:LowerCorner", namespaces=NS), "20.0 -12.0")
        self.assertEqual(bbox.findtext("ows:UpperCorner", namespaces=NS), "52.0 23.0")

    def test_the_document_is_served_as_xml(self):
        response = self.client.get(
            reverse("wmts:capabilities", args=[self.organisation.slug])
        )
        self.assertEqual(response["Content-Type"], "application/xml")

    def test_anothers_org_path_on_this_host_is_absent(self):
        """The host is the authority and the path may only agree with it —
        the same rule the tile-config callback enforces (#354, story 25)."""
        make_organisation("other-org")
        response = self.client.get(
            reverse("wmts:capabilities", args=["other-org"])
        )
        self.assertEqual(response.status_code, 404)

    def test_an_unknown_org_segment_is_absent_too(self):
        response = self.client.get(reverse("wmts:capabilities", args=["nowhere"]))
        self.assertEqual(response.status_code, 404)


class WMTSKVPBindingTests(CapabilitiesReader, TestCase):
    """The KVP binding a legacy client reads out of the document (#362).

    A modern client follows the per-layer REST templates; a KVP-only one reads
    ``OperationsMetadata`` and learns the single org-scoped endpoint under the
    Titiler prefix, which proxies GetCapabilities back to this very view. Both
    together are what "one pasted URL and nothing else" means — a document
    without this section advertises layers a KVP client has no address to
    fetch.
    """

    def setUp(self):
        super().setUp()
        dial_org(self.client)
        self.organisation = make_organisation()
        make_tiered_catalog(self.organisation)

    def test_the_kvp_operations_point_at_the_org_scoped_endpoint(self):
        operations = self.operations(self.fetch())
        expected = (
            f"http://{org_host()}{wmts_kvp_endpoint(self.organisation)}?"
        )
        self.assertEqual(
            operations,
            {
                "GetCapabilities": expected,
                "GetTile": expected,
                "GetFeatureInfo": expected,
            },
        )

    def test_the_href_carries_the_separator_a_client_appends_after(self):
        """OWS clients concatenate their parameters onto this href rather than
        parse it, so the separator has to already be there."""
        for href in self.operations(self.fetch()).values():
            self.assertTrue(href.endswith("?"), href)

    def test_each_operation_declares_the_kvp_encoding(self):
        document = ET.fromstring(self.fetch().content)
        gets = document.findall(
            "ows:OperationsMetadata/ows:Operation/ows:DCP/ows:HTTP/ows:Get", NS,
        )
        self.assertEqual(len(gets), 3)
        for get in gets:
            constraint = get.find("ows:Constraint", NS)
            self.assertEqual(constraint.get("name"), "GetEncoding")
            self.assertEqual(
                [v.text for v in constraint.findall(
                    "ows:AllowedValues/ows:Value", NS,
                )],
                ["KVP"],
            )

    def test_a_getfeatureinfo_built_on_the_advertised_endpoint_scopes_to_its_layer(self):
        """The identify address a client builds from this document is one the
        gate reads as the layer's own collection (#363) — the same arm of
        ``scope_of`` GetTile travels, so identify opens no side door."""
        href = self.operations(self.fetch())["GetFeatureInfo"]
        split = urlsplit(
            f"{href}SERVICE=WMTS&REQUEST=GetFeatureInfo"
            f"&LAYER={PUBLIC_LAYER}&INFOFORMAT=application/json"
        )
        self.assertEqual(
            scope_of(f"{split.path}?{split.query}"),
            MachineScope("test-org", "forecast", "temperature"),
        )

    def test_a_request_built_on_the_advertised_endpoint_scopes_to_the_org(self):
        """What the gate makes of a client that follows this document: the
        href with a GetCapabilities query concatenated on is an address
        ``scope_of`` reads as this organisation and no collection."""
        href = self.operations(self.fetch())["GetCapabilities"]
        split = urlsplit(f"{href}SERVICE=WMTS&REQUEST=GetCapabilities")
        self.assertEqual(
            scope_of(f"{split.path}?{split.query}"),
            MachineScope("test-org", None, None),
        )


class WMTSDimensionTests(IsolatedCapabilitiesCache, TestCase):
    """Time and Reftime dimensions, enumerated from the organisation's Items (#358).

    One observation collection and one forecast collection under the same org:
    the forecast advertises both axes with the newest run as the default and
    that run's valid times as the Time list, the observation advertises Time
    alone — so a dimension-ignorant client substituting defaults into the
    ResourceURL template lands on coherent latest tiles for either kind.
    """

    def setUp(self):
        super().setUp()
        dial_org(self.client)
        self.organisation = make_organisation()
        catalog = Catalog.objects.create(
            organisation=self.organisation,
            name="Weather", slug="weather", file_format="geotiff",
        )
        unit = Unit.objects.create(name="Celsius", symbol="C")

        observations = Collection.objects.create(
            catalog=catalog, name="Station Temperature", slug="obs-temperature",
            visibility=Collection.Visibility.PUBLIC,
        )
        Variable.objects.create(
            collection=observations, slug="t2m", name="2m Temperature",
            unit=unit, value_min=0, value_max=50,
        )
        for hour in (0, 6, 12):
            Item.objects.create(
                collection=observations,
                time=datetime(2026, 3, 1, hour, 0, tzinfo=timezone.utc),
            )

        forecast = Collection.objects.create(
            catalog=catalog, name="Model Temperature", slug="fc-temperature",
            visibility=Collection.Visibility.PUBLIC,
        )
        Variable.objects.create(
            collection=forecast, slug="t2m", name="Forecast 2m Temperature",
            unit=unit, value_min=0, value_max=50,
        )
        for day, hours in ((1, (0, 6, 12)), (2, (0, 6))):
            run = datetime(2026, 3, day, 0, 0, tzinfo=timezone.utc)
            for hour in hours:
                Item.objects.create(
                    collection=forecast,
                    time=run.replace(hour=hour),
                    reference_time=run,
                )

    def layer(self, identifier):
        response = self.client.get(
            reverse("wmts:capabilities", args=[self.organisation.slug])
        )
        self.assertEqual(response.status_code, 200)
        document = ET.fromstring(response.content)
        for layer in document.findall("wmts:Contents/wmts:Layer", NS):
            if layer.findtext("ows:Identifier", namespaces=NS) == identifier:
                return layer
        self.fail(f"No layer {identifier!r} in the document")

    def dimensions(self, layer):
        return {
            dimension.findtext("ows:Identifier", namespaces=NS): dimension
            for dimension in layer.findall("wmts:Dimension", NS)
        }

    def values(self, dimension):
        return [value.text for value in dimension.findall("wmts:Value", NS)]

    def test_a_forecast_layer_carries_time_and_reftime(self):
        dimensions = self.dimensions(self.layer("weather:fc-temperature:t2m"))
        self.assertEqual(list(dimensions), ["Time", "Reftime"])

    def test_an_observation_layer_carries_time_only(self):
        dimensions = self.dimensions(self.layer("weather:obs-temperature:t2m"))
        self.assertEqual(list(dimensions), ["Time"])

    def test_reftime_lists_every_run_and_defaults_to_the_newest(self):
        dimensions = self.dimensions(self.layer("weather:fc-temperature:t2m"))
        reftime = dimensions["Reftime"]
        self.assertEqual(
            self.values(reftime),
            ["2026-03-02T00:00:00Z", "2026-03-01T00:00:00Z"],
        )
        self.assertEqual(
            reftime.findtext("wmts:Default", namespaces=NS),
            "2026-03-02T00:00:00Z",
        )

    def test_forecast_time_lists_the_default_runs_valid_times(self):
        """Only the newest run's valid times — a Time from one run against
        another run's Reftime would advertise combinations that do not exist."""
        dimensions = self.dimensions(self.layer("weather:fc-temperature:t2m"))
        time = dimensions["Time"]
        self.assertEqual(
            self.values(time),
            ["2026-03-02T00:00:00Z", "2026-03-02T06:00:00Z"],
        )
        self.assertEqual(
            time.findtext("wmts:Default", namespaces=NS), "2026-03-02T00:00:00Z",
        )

    def test_observation_time_lists_everything_and_defaults_to_the_newest(self):
        dimensions = self.dimensions(self.layer("weather:obs-temperature:t2m"))
        time = dimensions["Time"]
        self.assertEqual(
            self.values(time),
            [
                "2026-03-01T00:00:00Z",
                "2026-03-01T06:00:00Z",
                "2026-03-01T12:00:00Z",
            ],
        )
        self.assertEqual(
            time.findtext("wmts:Default", namespaces=NS), "2026-03-01T12:00:00Z",
        )

    def test_templates_carry_exactly_the_advertised_dimension_placeholders(self):
        forecast = self.layer("weather:fc-temperature:t2m")
        self.assertTrue(
            forecast.find("wmts:ResourceURL", NS).get("template")
            .endswith("?time={Time}&reftime={Reftime}")
        )
        observation = self.layer("weather:obs-temperature:t2m")
        self.assertTrue(
            observation.find("wmts:ResourceURL", NS).get("template")
            .endswith("?time={Time}")
        )

    def test_default_substitution_names_an_item_that_exists(self):
        """The dimension-ignorant client's path: filling the template with the
        advertised defaults must address a (time, reftime) pair the collection
        actually holds, or the no-dimensions client 404s by construction."""
        forecast = self.layer("weather:fc-temperature:t2m")
        dimensions = self.dimensions(forecast)
        template = forecast.find("wmts:ResourceURL", NS).get("template")
        url = template.format(
            TileMatrix=0, TileCol=0, TileRow=0,
            Time=dimensions["Time"].findtext("wmts:Default", namespaces=NS),
            Reftime=dimensions["Reftime"].findtext("wmts:Default", namespaces=NS),
        )
        self.assertIn("time=2026-03-02T00:00:00Z", url)
        self.assertTrue(
            Item.objects.filter(
                collection__slug="fc-temperature",
                time=datetime(2026, 3, 2, 0, 0, tzinfo=timezone.utc),
                reference_time=datetime(2026, 3, 2, 0, 0, tzinfo=timezone.utc),
            ).exists()
        )


class WMTSStyleTests(IsolatedCapabilitiesCache, TestCase):
    """Named styles in the capabilities document (#359).

    Following the styling-surface pattern: which styles exist is Django's
    knowledge (ADR 0023), so the document lists a variable's real named styles
    — identifiers being the style slugs the tile route's ``?style=`` reads —
    with the actual default marked, and the REST template carries a
    ``{Style}`` placeholder wired to that same parameter. A variable with no
    named styles keeps a valid placeholder default entry and no style
    parameter, mirroring the no-items/no-dimensions precedent.
    """

    def setUp(self):
        super().setUp()
        dial_org(self.client)
        self.organisation = make_organisation()
        catalog = Catalog.objects.create(
            organisation=self.organisation,
            name="Weather", slug="weather", file_format="geotiff",
        )
        unit = Unit.objects.create(name="Celsius", symbol="C")
        collection = Collection.objects.create(
            catalog=catalog, name="Temperature", slug="temperature",
            visibility=Collection.Visibility.PUBLIC,
        )
        self.styled = Variable.objects.create(
            collection=collection, slug="t2m", name="2m Temperature",
            unit=unit, value_min=0, value_max=50,
        )
        VariableStyle.objects.create(
            variable=self.styled, name="Analyst", slug="analyst",
            stops=[{"value": 0.0, "color": "#000000"},
                   {"value": 50.0, "color": "#ffffff"}],
        )
        VariableStyle.objects.create(
            variable=self.styled, name="Official", slug="official",
            is_default=True,
            stops=[{"value": 0.0, "color": "#0000ff"},
                   {"value": 50.0, "color": "#ff0000"}],
        )
        self.styleless = Variable.objects.create(
            collection=collection, slug="rh", name="Relative Humidity",
            unit=unit, value_min=0, value_max=100,
        )
        Item.objects.create(
            collection=collection,
            time=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc),
        )

    def layer(self, identifier):
        response = self.client.get(
            reverse("wmts:capabilities", args=[self.organisation.slug])
        )
        self.assertEqual(response.status_code, 200)
        document = ET.fromstring(response.content)
        for layer in document.findall("wmts:Contents/wmts:Layer", NS):
            if layer.findtext("ows:Identifier", namespaces=NS) == identifier:
                return layer
        self.fail(f"No layer {identifier!r} in the document")

    def styles(self, layer):
        return layer.findall("wmts:Style", NS)

    def test_every_named_style_appears_under_its_layer(self):
        styles = self.styles(self.layer("weather:temperature:t2m"))
        self.assertEqual(
            [s.findtext("ows:Identifier", namespaces=NS) for s in styles],
            ["official", "analyst"],
        )
        self.assertEqual(
            [s.findtext("ows:Title", namespaces=NS) for s in styles],
            ["Official", "Analyst"],
        )

    def test_the_actual_default_is_the_one_marked_default(self):
        styles = self.styles(self.layer("weather:temperature:t2m"))
        marked = {
            s.findtext("ows:Identifier", namespaces=NS): s.get("isDefault")
            for s in styles
        }
        self.assertEqual(marked, {"official": "true", "analyst": None})

    def test_identifiers_are_style_slugs_not_the_literal_default(self):
        styles = self.styles(self.layer("weather:temperature:t2m"))
        self.assertNotIn(
            "default",
            [s.findtext("ows:Identifier", namespaces=NS) for s in styles],
        )

    def test_a_styled_template_carries_the_style_placeholder(self):
        layer = self.layer("weather:temperature:t2m")
        template = layer.find("wmts:ResourceURL", NS).get("template")
        self.assertIn("style={Style}", template)

    def test_a_filled_styled_template_scopes_back_to_the_collection(self):
        """Substituting every placeholder — style included — must yield a URI
        the gate reads as this collection: the picker's choice lands on the
        same route the gateway authorises."""
        layer = self.layer("weather:temperature:t2m")
        template = layer.find("wmts:ResourceURL", NS).get("template")
        split = urlsplit(template.format(
            TileMatrix=6, TileCol=38, TileRow=32,
            Style="analyst", Time="2026-03-01T00:00:00Z",
        ))
        self.assertEqual(
            scope_of(f"{split.path}?{split.query}"),
            MachineScope("test-org", "weather", "temperature"),
        )

    def test_styles_with_no_default_flagged_mark_nothing(self):
        """The database forbids a second default, not a missing one. Then no
        entry is marked: the styleless route renders grayscale, so marking a
        named style would advertise a default it is not (ADR 0023)."""
        VariableStyle.objects.filter(variable=self.styled).update(is_default=False)
        styles = self.styles(self.layer("weather:temperature:t2m"))
        self.assertEqual(len(styles), 2)
        self.assertEqual([s.get("isDefault") for s in styles], [None, None])

    def test_a_variable_without_styles_keeps_a_valid_default_entry(self):
        styles = self.styles(self.layer("weather:temperature:rh"))
        (placeholder,) = styles
        self.assertEqual(placeholder.get("isDefault"), "true")
        self.assertEqual(
            placeholder.findtext("ows:Identifier", namespaces=NS), "default",
        )

    def test_a_styleless_template_carries_no_style_parameter(self):
        """The placeholder identifier is not a slug the tile route knows, so
        the template must not invite a client to substitute it — the styleless
        request already renders the default (ADR 0023)."""
        layer = self.layer("weather:temperature:rh")
        template = layer.find("wmts:ResourceURL", NS).get("template")
        self.assertNotIn("{Style}", template)
        self.assertNotIn("style=", template)


class WMTSCredentialTests(CapabilitiesReader, TestCase):
    """Private layers and API-key propagation (#360).

    A valid key widens the document through the collection manager's
    ``visible_to`` and nothing looser (ADR 0014); a key that travelled as
    ``?api_key=`` is additionally written into every advertised URL, so a
    legacy client — which can fill placeholders but cannot append parameters —
    reaches private tiles from one paste. Keyed documents are personal and say
    so to caches.
    """

    def setUp(self):
        super().setUp()
        dial_org(self.client)
        self.organisation = make_organisation()
        tiers = make_tiered_catalog(self.organisation)
        Item.objects.create(
            collection=tiers["private"],
            time=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc),
        )

        User = get_user_model()
        member = User.objects.create_user("member", password="x")
        join_org(member)
        _, self.member_key = ApiKey.objects.mint(user=member, name="qgis")
        outsider = User.objects.create_user("outsider", password="x")
        join_org(outsider, "other-org")
        _, self.outsider_key = ApiKey.objects.mint(user=outsider, name="qgis")
        admin = User.objects.create_superuser("admin", password="x")
        _, self.admin_key = ApiKey.objects.mint(user=admin, name="ops")

    def test_anonymous_discovery_stays_public_only(self):
        self.assertEqual(self.identifiers(self.fetch()), [self.PUBLIC])

    def test_a_members_key_adds_the_orgs_private_layers_and_never_internal(self):
        response = self.fetch({"api_key": self.member_key})
        self.assertEqual(
            self.identifiers(response), [self.PRIVATE, self.PUBLIC],
        )

    def test_another_organisations_member_sees_public_only(self):
        response = self.fetch({"api_key": self.outsider_key})
        self.assertEqual(self.identifiers(response), [self.PUBLIC])

    def test_the_instance_admin_sees_private_layers(self):
        response = self.fetch({"api_key": self.admin_key})
        self.assertEqual(
            self.identifiers(response), [self.PRIVATE, self.PUBLIC],
        )

    def test_a_broken_key_is_unauthorised_rather_than_public_only(self):
        """The same 401 the tile gate answers (ADR 0014): a silently
        public-only document would send a key holder debugging a dataset that
        appears to have vanished."""
        response = self.client.get(
            reverse("wmts:capabilities", args=[self.organisation.slug]),
            {"api_key": "grv_not-a-real-key"},
        )
        self.assertEqual(response.status_code, 401)

    def test_a_query_key_is_written_into_every_tile_template(self):
        templates = self.templates(self.fetch({"api_key": self.member_key}))
        self.assertEqual(len(templates), 2)
        for template in templates.values():
            self.assertIn(f"api_key={self.member_key}", template)

    def test_the_service_metadata_url_keeps_the_key(self):
        """The document advertises its own address; without the key a client
        refreshing capabilities through it would silently drop to the
        public-only view."""
        response = self.fetch({"api_key": self.member_key})
        document = ET.fromstring(response.content)
        href = document.find("wmts:ServiceMetadataURL", NS).get(
            f"{{{NS['xlink']}}}href"
        )
        self.assertIn(f"api_key={self.member_key}", href)

    def test_the_kvp_endpoint_keeps_the_key_and_its_separator(self):
        """The paste-able URL is one of the advertised URLs (#362): a KVP
        client appends its parameters to this href on every later request, so
        a bare spelling would drop it to public-only tiles."""
        for href in self.operations(self.fetch({"api_key": self.member_key})).values():
            self.assertIn(f"api_key={self.member_key}", href)
            self.assertTrue(href.endswith("&"), href)

    def test_an_anonymous_document_carries_no_key_anywhere(self):
        self.assertNotIn(b"api_key", self.fetch().content)

    def test_a_header_key_widens_visibility_but_marks_no_urls(self):
        """A caller who can send an Authorization header can send it on tile
        requests too — only the query transport needs propagating, and a
        credential should not move to a weaker transport uninvited."""
        response = self.fetch(
            HTTP_AUTHORIZATION=f"Bearer {self.member_key}",
        )
        self.assertIn(self.PRIVATE, self.identifiers(response))
        self.assertNotIn(b"api_key", response.content)

    def test_a_query_string_the_authenticator_ignored_is_not_propagated(self):
        """With a Bearer header presented, the header is the judged credential
        and the query value was never validated — writing it into the document
        would advertise a string nobody checked."""
        response = self.fetch(
            {"api_key": "grv_never-checked"},
            HTTP_AUTHORIZATION=f"Bearer {self.member_key}",
        )
        self.assertIn(self.PRIVATE, self.identifiers(response))
        self.assertNotIn(b"api_key=grv_never-checked", response.content)

    def test_a_keyed_document_is_private_to_caches(self):
        response = self.fetch({"api_key": self.member_key})
        self.assertIn("private", response.get("Cache-Control", ""))

    def test_a_session_document_is_private_to_caches_too(self):
        """Any credentialed document may list private layers; none of them may
        enter a shared cache, whatever the transport."""
        self.client.login(username="member", password="x")
        response = self.fetch()
        self.assertIn("private", response.get("Cache-Control", ""))

    def test_an_anonymous_document_is_not_marked_private(self):
        """The shareable document stays shareable — the shared cache of #361
        must be allowed to hold it."""
        self.assertNotIn("private", self.fetch().get("Cache-Control", ""))

    def test_legacy_accept_headers_are_not_negotiated_away(self):
        """A legacy client asking for XML (or anything else) gets the one
        representation this document has, never a 406."""
        response = self.fetch(
            {"api_key": self.member_key}, HTTP_ACCEPT="application/xml",
        )
        self.assertEqual(response["Content-Type"], "application/xml")

    def test_private_tiles_pass_the_gate_using_only_document_urls(self):
        """The end-to-end promise of #360: fill the advertised template with
        the advertised defaults and the resulting URL — key included, exactly
        as nginx forwards the original query onto the auth subrequest — is
        authorised for the private layer."""
        layer = self.layer(self.fetch({"api_key": self.member_key}), self.PRIVATE)
        template = layer.find("wmts:ResourceURL", NS).get("template")
        time = layer.find("wmts:Dimension/wmts:Default", NS).text
        split = urlsplit(template.format(TileMatrix=1, TileCol=0, TileRow=1, Time=time))

        gate = self.client.get(
            f"{reverse('tile_auth')}?{split.query}",
            HTTP_X_ORIGINAL_URI=f"{split.path}?{split.query}",
        )
        self.assertEqual(gate.status_code, 204)

        anonymous = self.client.get(
            reverse("tile_auth"), HTTP_X_ORIGINAL_URI=split.path,
        )
        self.assertEqual(anonymous.status_code, 403)
