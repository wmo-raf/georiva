"""Proof that the tile servers are told which organisation, and told correctly.

Titiler and Martin are the two services on this instance that answer without a
resolvable Host: the browser reaches them through nginx on the portal's own
hostname, and Titiler dials Django back on an internal container name that
belongs to no organisation at all. Neither can therefore do what
``OrganisationMiddleware`` does, so the organisation has to travel in the
address instead — and everything below exists to hold that arrangement to two
promises.

*Django decides, the tile servers concatenate.* Every machine-plane URL is built
from a row Django already fetched under a host-scoped query, so an org segment
can never disagree with the catalog beside it. The tests read the org out of the
generated URL rather than checking a format, because the failure that matters is
not a malformed path — it is a well-formed path naming the wrong tenant.

*Colliding slugs stay apart.* Two institutions may each run a ``forecast``
catalog (#267), so every fixture here is duplicated across two organisations
under the same slug. A key, a path or a lookup that drops the org segment stops
distinguishing them, and each test below is written so that dropping it fails.
"""
from datetime import datetime, timezone

from django.test import TestCase, override_settings
from django.urls import reverse

from georiva.core.machine_plane import (
    MARTIN_PREFIX,
    martin_boundary_stats_url,
    org_slug_of,
    render_config_token,
    style_version_token,
    titiler_encoded_preview_url,
    titiler_preview_url,
    titiler_variable_root,
    wmts_capabilities_url,
    wmts_kvp_endpoint,
    wmts_layer_identifier,
    wmts_rest_featureinfo_template,
    wmts_rest_tile_template,
)
from georiva.core.models import Item, Variable
from georiva.core.testing import ANALYST_STOPS, make_style
from georiva.core.machine_plane.palette_cache import (
    build_colormap_256,
    build_variable_payload,
    get_palette_cache_key,
    variable_cache_key,
    warm_all,
)
from georiva.organisations.testing import (
    SHARED_TREE_SLUG as SHARED_SLUG,
    make_org_tree,
    make_organisation,
)


def build_tree(organisation, *, variable_name):
    """A data chain under ``SHARED_SLUG`` — the same slug for every organisation."""
    return make_org_tree(organisation, name=variable_name)


class MachinePlaneAddressTests(TestCase):
    """The URLs and cache keys Django emits for Titiler and Martin."""

    @classmethod
    def setUpTestData(cls):
        cls.kenya = make_organisation("kenya")
        cls.uganda = make_organisation("uganda")
        cls.kenya_tree = build_tree(cls.kenya, variable_name="Kenya Forecast")
        cls.uganda_tree = build_tree(cls.uganda, variable_name="Uganda Forecast")

    def test_org_is_read_from_the_row_not_the_caller(self):
        self.assertEqual(org_slug_of(self.kenya_tree["collection"]), "kenya")
        self.assertEqual(org_slug_of(self.uganda_tree["item"].collection), "uganda")

    def test_a_titiler_url_opens_with_its_own_organisation(self):
        url = titiler_preview_url(self.kenya_tree["item"], SHARED_SLUG)
        self.assertTrue(
            url.startswith(f"/titiler/kenya/{SHARED_SLUG}/{SHARED_SLUG}/{SHARED_SLUG}/"),
            url,
        )
        self.assertIn("time=2026-03-01T12", url)

    def test_two_organisations_sharing_a_slug_get_different_titiler_urls(self):
        self.assertNotEqual(
            titiler_preview_url(self.kenya_tree["item"], SHARED_SLUG),
            titiler_preview_url(self.uganda_tree["item"], SHARED_SLUG),
        )

    def test_an_encoded_preview_url_carries_the_render_config_token(self):
        """The immutable cache header on encoded-preview.png is only safe
        because a render-range edit is a *different URL* (ADR 0021)."""
        item = self.kenya_tree["item"]
        variable = self.kenya_tree["variable"]
        url = titiler_encoded_preview_url(item, variable)
        self.assertTrue(
            url.startswith(
                f"/titiler/kenya/{SHARED_SLUG}/{SHARED_SLUG}/{SHARED_SLUG}/"
                "encoded-preview.png?"
            ),
            url,
        )
        self.assertIn(f"v={render_config_token(variable)}", url)

    def test_changing_the_render_range_changes_the_url(self):
        variable = self.kenya_tree["variable"]
        before = render_config_token(variable)
        variable.value_max = (variable.value_max or 0) + 1
        self.assertNotEqual(before, render_config_token(variable))

    def test_identical_render_config_yields_the_same_token(self):
        """A hash, not a counter: no version state to migrate or drift."""
        self.assertEqual(
            render_config_token(self.kenya_tree["variable"]),
            render_config_token(self.kenya_tree["variable"]),
        )

    def test_a_forecast_preview_carries_its_reference_time(self):
        item = Item.objects.create(
            collection=self.kenya_tree["collection"],
            time=datetime(2026, 3, 2, tzinfo=timezone.utc),
            reference_time=datetime(2026, 3, 1, tzinfo=timezone.utc),
        )
        url = titiler_preview_url(item, SHARED_SLUG)
        self.assertIn("reftime=2026-03-01T00", url)

    def test_the_asset_preview_url_is_the_same_address(self):
        """One grammar, not two: the model property delegates rather than repeats."""
        self.assertEqual(
            self.kenya_tree["asset"].preview_url,
            titiler_preview_url(self.kenya_tree["item"], self.kenya_tree["variable"]),
        )

    def test_the_template_tag_ignores_slugs_it_is_not_given(self):
        """The tag reads catalog and org from the item, so a page cannot mix them."""
        from django.template import Context, Template

        rendered = Template(
            "{% load georiva_tags %}{% titiler_preview_url item slug %}"
        ).render(Context({"item": self.uganda_tree["item"], "slug": SHARED_SLUG}))
        self.assertTrue(rendered.startswith("/titiler/uganda/"), rendered)

    def test_a_martin_url_pins_the_tile_to_one_organisations_rows(self):
        url = martin_boundary_stats_url(self.uganda_tree["collection"], MARTIN_PREFIX)
        self.assertIn("/martin/boundary_stats/{z}/{x}/{y}?", url)
        self.assertIn("org=uganda", url)
        self.assertIn(f"catalog={SHARED_SLUG}", url)
        self.assertIn(f"collection={SHARED_SLUG}", url)

    def test_a_martin_url_can_be_absolute_without_losing_the_triple(self):
        url = martin_boundary_stats_url(
            self.kenya_tree["collection"], base="https://kenya.example/martin/",
        )
        self.assertTrue(url.startswith("https://kenya.example/martin/boundary_stats/"), url)
        self.assertIn("org=kenya", url)

    def test_palette_cache_keys_of_colliding_catalogs_differ(self):
        keys = {
            get_palette_cache_key(org, SHARED_SLUG, SHARED_SLUG, SHARED_SLUG)
            for org in ("kenya", "uganda")
        }
        self.assertEqual(len(keys), 2, keys)
        self.assertIn("georiva:palette:kenya:forecast:forecast:forecast", keys)

    def test_a_variables_key_is_derived_from_its_own_row(self):
        self.assertEqual(
            variable_cache_key(self.uganda_tree["variable"]),
            "georiva:palette:uganda:forecast:forecast:forecast",
        )


class WmtsAddressTests(TestCase):
    """The two WMTS addresses Django writes (#355).

    Both are org-level rather than variable-level — the whole point of WMTS is
    one paste-able URL per organisation — so the org segment is the only
    varying part, and the tests hold it to the same promise as every other
    machine-plane address: read from the row, distinct across tenants.
    """

    @classmethod
    def setUpTestData(cls):
        cls.kenya = make_organisation("kenya")
        cls.uganda = make_organisation("uganda")

    def test_the_kvp_endpoint_sits_under_the_titiler_proxy_org_first(self):
        self.assertEqual(wmts_kvp_endpoint(self.kenya), "/titiler/kenya/wmts")

    def test_two_organisations_get_different_kvp_endpoints(self):
        self.assertNotEqual(
            wmts_kvp_endpoint(self.kenya), wmts_kvp_endpoint(self.uganda),
        )

    def test_a_keyed_kvp_endpoint_carries_the_credential_param(self):
        """#362: the keyed document advertises this endpoint as where its
        reader asks everything next, on the same ``api_key`` parameter the
        tile gate reads (ADR 0015)."""
        self.assertEqual(
            wmts_kvp_endpoint(self.kenya, api_key="grv_secret"),
            "/titiler/kenya/wmts?api_key=grv_secret",
        )

    def test_the_capabilities_url_lives_on_the_metadata_plane_org_first(self):
        self.assertEqual(
            wmts_capabilities_url(self.uganda),
            "/api/wmts/uganda/WMTSCapabilities.xml",
        )

    def test_two_organisations_get_different_capabilities_urls(self):
        self.assertNotEqual(
            wmts_capabilities_url(self.kenya), wmts_capabilities_url(self.uganda),
        )

    def test_a_keyed_capabilities_url_carries_the_credential_param(self):
        """#360: the keyed document advertises this as its own refresh URL, on
        the same ``api_key`` parameter the tile gate reads (ADR 0015)."""
        self.assertEqual(
            wmts_capabilities_url(self.kenya, api_key="grv_secret"),
            "/api/wmts/kenya/WMTSCapabilities.xml?api_key=grv_secret",
        )


class WmtsRestTileTemplateTests(TestCase):
    """The ResourceURL template a capabilities Layer advertises (#356).

    Unlike the two endpoints above it is variable-level — it names one
    variable's existing tile route — so it is held to the same promises as
    every other variable-level address: org read from the row, colliding slugs
    across organisations kept apart, and readable back by ``scope_of`` as the
    collection it addresses.
    """

    @classmethod
    def setUpTestData(cls):
        cls.kenya_tree = build_tree(make_organisation("kenya"), variable_name="Kenya Forecast")
        cls.uganda_tree = build_tree(make_organisation("uganda"), variable_name="Uganda Forecast")

    def test_the_template_is_the_existing_tile_route_org_first(self):
        template = wmts_rest_tile_template(self.kenya_tree["variable"], ("Time",))
        self.assertEqual(
            template,
            f"/titiler/kenya/{SHARED_SLUG}/{SHARED_SLUG}/{SHARED_SLUG}"
            "/tiles/WebMercatorQuad/{TileMatrix}/{TileCol}/{TileRow}.png"
            "?time={Time}",
        )

    def test_a_forecast_layer_carries_the_reftime_placeholder_too(self):
        template = wmts_rest_tile_template(
            self.kenya_tree["variable"], ("Time", "Reftime"),
        )
        self.assertTrue(template.endswith("?time={Time}&reftime={Reftime}"))

    def test_a_variable_without_dimensions_gets_a_bare_template(self):
        template = wmts_rest_tile_template(self.kenya_tree["variable"])
        self.assertNotIn("?", template)
        self.assertTrue(template.endswith("/{TileMatrix}/{TileCol}/{TileRow}.png"))

    def test_a_styled_variable_gets_the_style_placeholder_on_the_style_param(self):
        """#359: the ``{Style}`` placeholder rides the same ``style`` query
        param every other styled URL uses (ADR 0023) — one spelling, wired
        beside the dimension params so a client fills them all alike."""
        template = wmts_rest_tile_template(
            self.kenya_tree["variable"], ("Time",), styled=True,
        )
        self.assertTrue(template.endswith("?style={Style}&time={Time}"))

    def test_a_styled_variable_without_dimensions_still_gets_the_placeholder(self):
        template = wmts_rest_tile_template(self.kenya_tree["variable"], styled=True)
        self.assertTrue(template.endswith(".png?style={Style}"))

    def test_a_keyed_template_appends_the_credential_after_the_placeholders(self):
        """#360: the key rides the same ``api_key`` parameter the auth
        subrequest reads, last so the placeholder grammar stays untouched —
        and it is written encoded, unlike the literal ``{…}`` placeholders."""
        template = wmts_rest_tile_template(
            self.kenya_tree["variable"], ("Time",), styled=True,
            api_key="grv_secret",
        )
        self.assertTrue(
            template.endswith("?style={Style}&time={Time}&api_key=grv_secret")
        )

    def test_a_keyed_template_without_dimensions_still_carries_the_key(self):
        template = wmts_rest_tile_template(
            self.kenya_tree["variable"], api_key="grv_secret",
        )
        self.assertTrue(template.endswith(".png?api_key=grv_secret"))

    def test_two_organisations_sharing_a_slug_get_different_templates(self):
        self.assertNotEqual(
            wmts_rest_tile_template(self.kenya_tree["variable"], ("Time",)),
            wmts_rest_tile_template(self.uganda_tree["variable"], ("Time",)),
        )

    def test_the_identify_template_is_the_tile_template_plus_the_pixel(self):
        """#379: the two templates a Layer advertises are one address written
        twice, so they are asserted against each other rather than against a
        second literal — a builder that drifted would still satisfy a literal
        copied from it, and a client would then identify a pixel of a tile it
        is not looking at."""
        variable = self.kenya_tree["variable"]
        for dimensions, styled, key in (
                (("Time",), False, None),
                (("Time", "Reftime"), True, None),
                ((), False, None),
                (("Time",), True, "grv_secret"),
        ):
            with self.subTest(dimensions=dimensions, styled=styled, keyed=bool(key)):
                tile = wmts_rest_tile_template(variable, dimensions, styled, key)
                info = wmts_rest_featureinfo_template(variable, dimensions, styled, key)
                path, _, query = tile.partition("?")
                info_path, _, info_query = info.partition("?")
                self.assertEqual(info_query, query)
                self.assertEqual(
                    info_path, path.removesuffix(".png") + "/{J}/{I}.json",
                )

    def test_the_identify_template_is_the_identify_route_org_first(self):
        self.assertEqual(
            wmts_rest_featureinfo_template(self.kenya_tree["variable"], ("Time",)),
            f"/titiler/kenya/{SHARED_SLUG}/{SHARED_SLUG}/{SHARED_SLUG}"
            "/tiles/WebMercatorQuad/{TileMatrix}/{TileCol}/{TileRow}/{J}/{I}.json"
            "?time={Time}",
        )

    def test_a_filled_identify_template_scopes_back_to_its_own_collection(self):
        """Two segments deeper than a tile, and the gate must still read the
        same collection out of it — otherwise the document advertises an
        identify address nginx denies (ADR 0015)."""
        from georiva.core.machine_plane import MachineScope, scope_of

        filled = wmts_rest_featureinfo_template(
            self.kenya_tree["variable"], ("Time",),
        ).format(
            TileMatrix=6, TileCol=38, TileRow=32, J=128, I=64,
            Time="2026-03-01T12:00:00Z",
        )
        self.assertEqual(
            scope_of(filled), MachineScope("kenya", SHARED_SLUG, SHARED_SLUG),
        )

    def test_a_layer_identifier_is_the_triple_a_kvp_layer_param_carries(self):
        """Writer and reader of the ``LAYER`` grammar live side by side: what
        this writes, ``scope_of`` splits back into the same collection."""
        from georiva.core.machine_plane import MachineScope, scope_of

        identifier = wmts_layer_identifier(self.kenya_tree["variable"])
        self.assertEqual(identifier, f"{SHARED_SLUG}:{SHARED_SLUG}:{SHARED_SLUG}")
        self.assertEqual(
            scope_of(f"/titiler/kenya/wmts?REQUEST=GetTile&LAYER={identifier}"),
            MachineScope("kenya", SHARED_SLUG, SHARED_SLUG),
        )

    def test_a_filled_template_scopes_back_to_its_own_collection(self):
        """The gate reads what this writes: substituting the placeholders must
        yield a URI ``scope_of`` resolves to the advertising collection."""
        from georiva.core.machine_plane import MachineScope, scope_of

        template = wmts_rest_tile_template(
            self.uganda_tree["variable"], ("Time", "Reftime"),
        )
        uri = template.format(
            TileMatrix=6, TileCol=38, TileRow=32,
            Time="2026-03-01T12:00:00Z", Reftime="2026-03-01T00:00:00Z",
        )
        self.assertEqual(
            scope_of(uri), MachineScope("uganda", SHARED_SLUG, SHARED_SLUG),
        )


class PaletteCacheSweepTests(TestCase):
    """Warming leaves the cache a mirror of the database, not a history of it.

    Titiler reads these keys straight out of Redis and nothing expires them, so
    a key whose variable was renamed, deactivated or deleted would otherwise
    outlive it — indefinitely, and invisibly, because a stale palette renders
    perfectly well. It just renders the wrong thing.
    """

    @classmethod
    def setUpTestData(cls):
        cls.organisation = make_organisation("kenya")
        cls.tree = build_tree(cls.organisation, variable_name="Kenya Forecast")

    def setUp(self):
        from django_redis import get_redis_connection

        self.redis = get_redis_connection("default")
        self.addCleanup(self._clear)
        self._clear()

    def _clear(self):
        keys = list(self.redis.scan_iter(match="georiva:palette:*"))
        if keys:
            self.redis.delete(*keys)

    def _keys(self):
        return {
            key.decode() if isinstance(key, bytes) else key
            for key in self.redis.scan_iter(match="georiva:palette:*")
        }

    def test_warming_writes_the_active_variables_key(self):
        warm_all()
        self.assertIn(variable_cache_key(self.tree["variable"]), self._keys())

    def test_a_key_left_by_the_pre_org_format_is_swept(self):
        """The migration case: keys written before the org segment existed."""
        legacy = f"georiva:palette:{SHARED_SLUG}:{SHARED_SLUG}:{SHARED_SLUG}"
        self.redis.set(legacy, "{}")
        warm_all()
        self.assertNotIn(legacy, self._keys())

    def test_a_deactivated_variables_key_does_not_outlive_it(self):
        warm_all()
        key = variable_cache_key(self.tree["variable"])
        self.assertIn(key, self._keys())

        Variable.objects.filter(pk=self.tree["variable"].pk).update(is_active=False)
        warm_all()
        self.assertNotIn(key, self._keys())

    def test_the_sweep_leaves_other_cache_namespaces_alone(self):
        self.redis.set("georiva:something-else", "{}")
        self.addCleanup(self.redis.delete, "georiva:something-else")
        warm_all()
        self.assertTrue(self.redis.exists("georiva:something-else"))


class StyledPayloadTests(TestCase):
    """The rendering payload under the two-layer model (ADR 0022): resolving
    the default style must be invisible from Titiler's side of the key.

    The stops a legacy palette held and the stops its migrated default style
    holds are the same rows, so the payload built from either must be
    byte-equivalent — that equivalence is what let the palette models retire
    without a client noticing.
    """

    @classmethod
    def setUpTestData(cls):
        cls.tree = build_tree(make_organisation("kenya"), variable_name="Kenya Forecast")
        cls.variable = cls.tree["variable"]

    def _style(self, **overrides):
        from georiva.core.models import VariableStyle

        fields = dict(
            variable=self.variable, name="Official", slug="official",
            is_default=True,
            stops=[{"value": 0.0, "color": "#000000"},
                   {"value": 50.0, "color": "#ff0000"}],
        )
        fields.update(overrides)
        return VariableStyle.objects.create(**fields)

    def test_a_styled_variables_payload_is_byte_equivalent_to_the_palette_eras(self):
        import json

        self._style()
        # What the retired builder produced for the same stops: range and
        # scale from the variable, the colormap interpolated from the
        # absolute value→color pairs.
        expected = {
            "vmin": self.variable.value_min,
            "vmax": self.variable.value_max,
            "scale_type": "linear",
            "colormap": build_colormap_256(
                [[0.0, [0, 0, 0]], [50.0, [255, 0, 0]]],
                self.variable.value_min, self.variable.value_max,
            ),
        }
        payload = build_variable_payload(self.variable)
        self.assertEqual(
            json.dumps(payload, sort_keys=True),
            json.dumps(expected, sort_keys=True),
        )

    def test_a_styleless_variables_payload_still_omits_the_colormap(self):
        self.assertEqual(
            build_variable_payload(self.variable),
            {"vmin": self.variable.value_min, "vmax": self.variable.value_max,
             "scale_type": "linear"},
        )

    def test_a_non_default_style_does_not_color_the_payload(self):
        self._style(is_default=False)
        self.assertNotIn("colormap", build_variable_payload(self.variable))


class StyleSignalTests(TestCase):
    """Style edits are live on the next tile: save and delete both re-warm,
    and a deleted variable's key does not wait for the next sweep."""

    @classmethod
    def setUpTestData(cls):
        cls.tree = build_tree(make_organisation("kenya"), variable_name="Kenya Forecast")
        cls.variable = cls.tree["variable"]

    def setUp(self):
        from django_redis import get_redis_connection

        self.redis = get_redis_connection("default")
        self.addCleanup(self._clear)
        self._clear()

    def _clear(self):
        keys = list(self.redis.scan_iter(match="georiva:palette:*"))
        if keys:
            self.redis.delete(*keys)

    def _payload(self):
        import json

        raw = self.redis.get(variable_cache_key(self.variable))
        return json.loads(raw) if raw else None

    def _make_style(self):
        from georiva.core.models import VariableStyle

        return VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official",
            is_default=True,
            stops=[{"value": 0.0, "color": "#000000"},
                   {"value": 50.0, "color": "#ff0000"}],
        )

    def test_saving_a_style_warms_its_variables_key(self):
        style = self._make_style()
        self.assertIn("colormap", self._payload())

        style.stops = [{"value": 0.0, "color": "#0000ff"},
                       {"value": 50.0, "color": "#00ff00"}]
        style.save()
        self.assertEqual(self._payload()["colormap"]["0"], [0, 0, 255, 255])

    def test_deleting_a_style_rewarms_the_key_without_it(self):
        self._make_style().delete()
        payload = self._payload()
        self.assertIsNotNone(payload)
        self.assertNotIn("colormap", payload)

    def test_a_deleted_variables_key_does_not_wait_for_the_sweep(self):
        warm_all()
        key = variable_cache_key(self.variable)
        self.assertTrue(self.redis.exists(key))
        Variable.objects.get(pk=self.variable.pk).delete()
        self.assertFalse(self.redis.exists(key))


@override_settings(ALLOWED_HOSTS=["*"])
class TileConfigOrgSegmentTests(TestCase):
    """The one lookup that resolves its organisation from the path, not the Host.

    Titiler dials it server-to-server on an internal container name. That host
    belongs to no organisation, so the endpoint is exempt from host resolution —
    which makes its own filtering the only thing standing between a palette
    request and the wrong tenant's rendering config.
    """

    @classmethod
    def setUpTestData(cls):
        cls.kenya = make_organisation("kenya")
        cls.uganda = make_organisation("uganda")
        build_tree(cls.kenya, variable_name="Kenya Forecast")
        build_tree(cls.uganda, variable_name="Uganda Forecast")

    def _url(self, org_slug):
        return reverse(
            "tile_config", args=[org_slug, SHARED_SLUG, SHARED_SLUG, SHARED_SLUG],
        )

    def test_it_answers_on_a_hostname_belonging_to_no_organisation(self):
        """Titiler's internal address. Host resolution would 404 this outright."""
        response = self.client.get(self._url("kenya"), HTTP_HOST="georiva:8000")
        self.assertEqual(response.status_code, 200)

    def test_each_organisations_config_comes_back_under_its_own_segment(self):
        for org_slug, expected in (("kenya", 10.0), ("uganda", 20.0)):
            Variable.objects.filter(
                collection__catalog__organisation__slug=org_slug,
            ).update(value_max=expected)

        for org_slug, expected in (("kenya", 10.0), ("uganda", 20.0)):
            with self.subTest(org=org_slug):
                response = self.client.get(self._url(org_slug), HTTP_HOST="georiva:8000")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["vmax"], expected)

    def test_an_unknown_organisation_is_not_found(self):
        response = self.client.get(self._url("nowhere"), HTTP_HOST="georiva:8000")
        self.assertEqual(response.status_code, 404)

    def test_a_catalog_under_the_wrong_organisation_is_not_found(self):
        """Both halves must agree — a real org and a real catalog is not enough."""
        lonely = make_organisation("lonely")
        self.assertEqual(
            self.client.get(self._url(lonely.slug), HTTP_HOST="georiva:8000").status_code,
            404,
        )

    # -- the public half ---------------------------------------------------
    #
    # `/api/` is publicly proxied, so this URL is reachable by anyone. The path
    # segment is trusted only where the reason for trusting it holds — no
    # organisation answers the host. On a tenant's host the Host wins.

    def test_a_portals_own_host_still_reads_its_own_config(self):
        response = self.client.get(self._url("kenya"), HTTP_HOST=self.kenya.hostname)
        self.assertEqual(response.status_code, 200)

    def test_one_hosts_caller_cannot_read_another_organisations_config(self):
        """The disclosure this endpoint would otherwise reopen (#278)."""
        response = self.client.get(self._url("uganda"), HTTP_HOST=self.kenya.hostname)
        self.assertEqual(response.status_code, 404)

    def test_the_answer_does_not_reveal_whether_the_other_org_exists(self):
        """A stranger's real org and an invented one must look identical."""
        real = self.client.get(self._url("uganda"), HTTP_HOST=self.kenya.hostname)
        invented = self.client.get(self._url("nowhere"), HTTP_HOST=self.kenya.hostname)
        self.assertEqual(real.status_code, invented.status_code)
        self.assertEqual(real.content, invented.content)


class StyleSelectorAddressTests(TestCase):
    """The ``?style=`` selector and the per-style version token (ADR 0023).

    Style is a rendering parameter, not an addressable resource: the path never
    changes, so nothing about routes, the nginx location or the auth gate has
    to. What does change is the *query* — a pinned style names itself there,
    and every colorized URL carries a token hashed from what its pixels bake
    in, so an edited style is a different URL rather than a stale cache entry.
    """

    @classmethod
    def setUpTestData(cls):
        cls.tree = build_tree(make_organisation("kenya"), variable_name="Kenya Forecast")
        cls.variable = cls.tree["variable"]
        cls.official = make_style(cls.variable, "official")
        cls.analyst = make_style(
            cls.variable, "analyst", is_default=False, stops=ANALYST_STOPS,
        )

    def test_a_pinned_style_travels_as_a_query_param(self):
        url = titiler_preview_url(self.tree["item"], self.variable, style=self.analyst)
        self.assertIn("style=analyst", url)
        self.assertIn(f"v={style_version_token(self.variable, self.analyst)}", url)

    def test_omission_names_no_style(self):
        """The default is selected by saying nothing — a URL that named it
        would survive a default flip pointing at the old default."""
        url = titiler_preview_url(self.tree["item"], self.variable)
        self.assertNotIn("style=", url)

    def test_a_default_preview_carries_the_default_styles_token(self):
        url = titiler_preview_url(self.tree["item"], self.variable)
        self.assertIn(f"v={style_version_token(self.variable, self.official)}", url)

    def test_two_styles_of_one_variable_get_different_tokens(self):
        self.assertNotEqual(
            style_version_token(self.variable, self.official),
            style_version_token(self.variable, self.analyst),
        )

    def test_editing_a_styles_stops_changes_its_token(self):
        before = style_version_token(self.variable, self.analyst)
        self.analyst.stops = [{"value": 0.0, "color": "#123456"},
                              {"value": 50.0, "color": "#00ff00"}]
        self.assertNotEqual(before, style_version_token(self.variable, self.analyst))

    def test_editing_the_range_changes_every_styles_token(self):
        before = style_version_token(self.variable, self.official)
        self.variable.value_max = (self.variable.value_max or 0) + 1
        self.assertNotEqual(before, style_version_token(self.variable, self.official))

    def test_the_encoded_texture_token_ignores_style_edits(self):
        """Encoding is style-independent (ADR 0021): a palette edit must not
        churn every cached texture URL on the instance."""
        before = render_config_token(self.variable)
        self.official.stops = ANALYST_STOPS
        self.official.save()
        self.assertEqual(before, render_config_token(self.variable))
        self.assertNotIn("style=", titiler_encoded_preview_url(self.tree["item"], self.variable))

    def test_a_pinned_style_is_tokened_even_for_a_slug_only_caller(self):
        """The style row knows its own variable, so a styled URL never goes
        out untokened — styled-but-untokened would undo the honesty the
        token exists for."""
        url = titiler_preview_url(self.tree["item"], SHARED_SLUG, style=self.analyst)
        self.assertIn("style=analyst", url)
        self.assertIn(f"v={style_version_token(self.variable, self.analyst)}", url)

    def test_a_slug_only_caller_still_gets_the_plain_address(self):
        """The templatetag holds a slug, not a row; its thumbnails are never
        cached, so an untokened URL stays honest there."""
        url = titiler_preview_url(self.tree["item"], SHARED_SLUG)
        self.assertNotIn("v=", url)
        self.assertNotIn("style=", url)

    def test_a_style_segmented_cache_key_ends_with_the_style(self):
        self.assertEqual(
            get_palette_cache_key("kenya", SHARED_SLUG, SHARED_SLUG, SHARED_SLUG, "analyst"),
            "georiva:palette:kenya:forecast:forecast:forecast:analyst",
        )

    def test_a_styles_key_is_derived_from_its_own_rows(self):
        self.assertEqual(
            variable_cache_key(self.variable, self.analyst),
            "georiva:palette:kenya:forecast:forecast:forecast:analyst",
        )


class StyleMultiplicityCacheTests(TestCase):
    """Every style is warm, and the styleless key is the default's alias.

    Titiler resolves ``?style=analyst`` into the segmented key and a styleless
    request into the alias, so both must always exist and the alias must always
    mirror whichever style is currently default — including in the same
    request that flipped it. An alias that diverges is a bug in the
    default-flip signal, not a feature (ADR 0023).
    """

    @classmethod
    def setUpTestData(cls):
        cls.tree = build_tree(make_organisation("kenya"), variable_name="Kenya Forecast")
        cls.variable = cls.tree["variable"]

    def setUp(self):
        from django_redis import get_redis_connection

        self.redis = get_redis_connection("default")
        self.addCleanup(self._clear)
        self._clear()

    def _clear(self):
        keys = list(self.redis.scan_iter(match="georiva:palette:*"))
        if keys:
            self.redis.delete(*keys)

    def _payload(self, style=None):
        import json

        raw = self.redis.get(variable_cache_key(self.variable, style))
        return json.loads(raw) if raw else None

    def test_saving_a_style_warms_its_own_segmented_key(self):
        analyst = make_style(self.variable, "analyst", is_default=False,
                             stops=ANALYST_STOPS)
        self.assertEqual(self._payload(analyst)["colormap"]["0"], [0, 0, 255, 255])

    def test_the_styleless_key_is_the_defaults_alias(self):
        official = make_style(self.variable, "official")
        make_style(self.variable, "analyst", is_default=False, stops=ANALYST_STOPS)
        self.assertEqual(self._payload(), self._payload(official))
        self.assertEqual(self._payload()["colormap"]["0"], [0, 0, 0, 255])

    def test_a_default_flip_rewrites_the_alias_immediately(self):
        make_style(self.variable, "official")
        analyst = make_style(self.variable, "analyst", is_default=False,
                             stops=ANALYST_STOPS)
        analyst.promote_to_default()
        self.assertEqual(self._payload()["colormap"]["0"], [0, 0, 255, 255])

    def test_deleting_a_style_prunes_its_key(self):
        make_style(self.variable, "official")
        analyst = make_style(self.variable, "analyst", is_default=False,
                             stops=ANALYST_STOPS)
        key = variable_cache_key(self.variable, analyst)
        self.assertTrue(self.redis.exists(key))
        analyst.delete()
        self.assertFalse(self.redis.exists(key))
        # The alias still mirrors the surviving default.
        self.assertEqual(self._payload()["colormap"]["0"], [0, 0, 0, 255])

    def test_deleting_the_variable_prunes_its_style_keys_too(self):
        analyst = make_style(self.variable, "analyst", is_default=False,
                             stops=ANALYST_STOPS)
        key = variable_cache_key(self.variable, analyst)
        self.assertTrue(self.redis.exists(key))
        Variable.objects.get(pk=self.variable.pk).delete()
        self.assertFalse(self.redis.exists(key))

    def test_the_sweep_iterates_styles_not_just_variables(self):
        make_style(self.variable, "analyst", is_default=False, stops=ANALYST_STOPS)
        stale = variable_cache_key(self.variable) + ":retired"
        self.redis.set(stale, "{}")
        warm_all()
        self.assertFalse(self.redis.exists(stale))
        self.assertTrue(self.redis.exists(variable_cache_key(self.variable) + ":analyst"))

    def test_a_neighbouring_variables_key_survives_a_prune(self):
        """`forecast`'s per-style sweep must not eat `forecast2`'s keys: the
        style segment is colon-separated, not prefix-matched."""
        from georiva.core.models import Asset  # noqa: F401 — tree import side

        neighbour_key = variable_cache_key(self.variable) + "2"
        self.redis.set(neighbour_key, "{}")
        self.addCleanup(self.redis.delete, neighbour_key)
        Variable.objects.get(pk=self.variable.pk).delete()
        self.assertTrue(self.redis.exists(neighbour_key))


@override_settings(ALLOWED_HOSTS=["*"])
class TileConfigStyleParamTests(TestCase):
    """`?style=` on the tile-config endpoint: the seam Titiler resolves through.

    Omission serves the default; a named style serves that style; an unknown
    slug is a 404 and never a fallback — silently serving the wrong style
    would be worse than failing (ADR 0023).
    """

    @classmethod
    def setUpTestData(cls):
        cls.tree = build_tree(make_organisation("kenya"), variable_name="Kenya Forecast")
        cls.variable = cls.tree["variable"]
        cls.official = make_style(cls.variable, "official")
        cls.analyst = make_style(
            cls.variable, "analyst", is_default=False, stops=ANALYST_STOPS,
        )

    def _get(self, query=""):
        url = reverse("tile_config", args=["kenya", SHARED_SLUG, SHARED_SLUG, SHARED_SLUG])
        return self.client.get(f"{url}{query}", HTTP_HOST="georiva:8000")

    def test_omission_serves_the_default_style(self):
        response = self._get()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["colormap"]["0"], [0, 0, 0, 255])

    def test_a_named_style_serves_that_style(self):
        response = self._get("?style=analyst")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["colormap"]["0"], [0, 0, 255, 255])

    def test_an_unknown_style_is_not_found_never_a_fallback(self):
        self.assertEqual(self._get("?style=nowhere").status_code, 404)

    def test_an_empty_style_param_reads_as_omission(self):
        response = self._get("?style=")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["colormap"]["0"], [0, 0, 0, 255])


@override_settings(ALLOWED_HOSTS=["*"])
class TileConfigStylesIndexTests(TestCase):
    """Discovery rides the existing endpoint, not a new one (ADR 0023).

    The payload is the resolved style's config plus an index of every style
    the variable carries, so a client can build a style picker from the same
    response it renders with. The index is the same whichever style was asked
    for — it describes the variable, not the answer.
    """

    @classmethod
    def setUpTestData(cls):
        cls.kenya = make_organisation("kenya")
        cls.tree = build_tree(cls.kenya, variable_name="Kenya Forecast")
        cls.variable = cls.tree["variable"]
        cls.official = make_style(cls.variable, "official")
        cls.analyst = make_style(
            cls.variable, "analyst", is_default=False, stops=ANALYST_STOPS,
        )

    def _get(self, query=""):
        url = reverse("tile_config", args=["kenya", SHARED_SLUG, SHARED_SLUG, SHARED_SLUG])
        return self.client.get(f"{url}{query}", HTTP_HOST="georiva:8000")

    INDEX = [
        {"slug": "official", "title": "Official", "is_default": True},
        {"slug": "analyst", "title": "Analyst", "is_default": False},
    ]

    def test_the_default_config_carries_the_index_default_first(self):
        response = self._get()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["styles"], self.INDEX)

    def test_a_named_styles_config_carries_the_same_index(self):
        response = self._get("?style=analyst")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["colormap"]["0"], [0, 0, 255, 255])
        self.assertEqual(payload["styles"], self.INDEX)

    def test_a_styleless_variable_carries_an_empty_index(self):
        build_tree(make_organisation("uganda"), variable_name="Uganda Forecast")
        url = reverse("tile_config", args=["uganda", SHARED_SLUG, SHARED_SLUG, SHARED_SLUG])
        response = self.client.get(url, HTTP_HOST="georiva:8000")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["styles"], [])

    def test_the_index_stays_out_of_titilers_cache_payload(self):
        """The Redis value is Titiler's rendering config and nothing more —
        discovery is this endpoint's addition, not the cache's."""
        self.assertNotIn("styles", build_variable_payload(self.variable))

    def test_a_private_collections_styles_are_exactly_as_invisible_as_it_is(self):
        """Visibility is the collection's, inherited (ADR 0023): the
        public-only constraint on this callback applies to styles identically,
        named or not."""
        from georiva.core.models import Collection

        Collection.objects.filter(pk=self.tree["collection"].pk).update(
            visibility=Collection.Visibility.PRIVATE,
        )
        self.assertEqual(self._get().status_code, 404)
        self.assertEqual(self._get("?style=analyst").status_code, 404)


class CogKeyGrammarTests(TestCase):
    """The one place the storage grammar is restated outside Django.

    Titiler has no ORM and rebuilds the COG key from path segments
    (``titiler-app/app/dependencies.build_cog_url``). That reconstruction is a
    copy of ``Catalog.storage_prefix``'s grammar living in another process,
    where no import or type can keep the two in step — so a change to one and
    not the other yields tiles that 404 against storage, silently and only in
    production. This asserts the copy still matches the original.
    """

    @classmethod
    def setUpTestData(cls):
        cls.tree = build_tree(make_organisation("kenya"), variable_name="Kenya Forecast")

    @staticmethod
    def _titiler_cog_path(org, catalog, collection, variable, time_dt):
        """``build_cog_url``'s path half, transcribed from the Titiler service."""
        return (
            f"{org}/{catalog}/{collection}/{variable}"
            f"/{time_dt.strftime('%Y/%m/%d')}/{variable}_{time_dt.strftime('%H%M%S')}.tif"
        )

    def test_titilers_key_matches_the_prefix_django_writes_under(self):
        catalog = self.tree["catalog"]
        key = self._titiler_cog_path(
            "kenya", catalog.slug, SHARED_SLUG, SHARED_SLUG,
            datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc),
        )
        self.assertTrue(
            key.startswith(catalog.storage_prefix.rstrip("/") + "/"),
            f"{key} is not under {catalog.storage_prefix}",
        )

    def test_the_titiler_route_and_the_storage_key_share_their_leading_segments(self):
        """A tile URL and the object behind it differ only by prefix, by design."""
        route = titiler_variable_root("kenya", SHARED_SLUG, SHARED_SLUG, SHARED_SLUG)
        key = self._titiler_cog_path(
            "kenya", SHARED_SLUG, SHARED_SLUG, SHARED_SLUG,
            datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(route, f"/titiler/{'/'.join(key.split('/')[:4])}")
