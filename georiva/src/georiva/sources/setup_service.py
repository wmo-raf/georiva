"""
SourceSetupService — provisions Catalog → Collection → Variable records from
CollectionDefinition objects declared by a DataFeed plugin.

Idempotent: re-running updates existing records (keyed by slug) rather than
creating duplicates, so adding new collections to a plugin is safe to re-run.
"""
import logging
import math
from typing import Optional

from django.db import transaction
from django.utils.text import slugify

from georiva.core.provisioning import build_source_block, resolve_unit
from georiva.sources.collection_definitions import CollectionDefinition, CollectionVariable

logger = logging.getLogger("georiva.sources.setup_service")


def _validated_palette_stops(raw):
    """Validate a plugin's declared ``palette_stops`` into a stops snapshot.

    Returns ``(stops, error)``: on success, ``stops`` is the normalized
    ``[{"value": float, "color": "#..."} ...]`` list in ascending value order
    and ``error`` is None; on any malformation, ``stops`` is None and
    ``error`` says why — the caller degrades one tier with a warning
    (ADR 0022: provisioning never fails on styling).
    """
    from georiva.core.models.visualization import HEX_COLOR_VALIDATOR
    from django.core.exceptions import ValidationError

    if raw is None:
        return None, None
    if isinstance(raw, str) or not isinstance(raw, (list, tuple)):
        return None, f"not a list of (value, color) pairs: {raw!r}"
    if len(raw) < 2:
        return None, "fewer than two stops — cannot span a range"
    stops = []
    for entry in raw:
        if isinstance(entry, str) or not isinstance(entry, (list, tuple)) or len(entry) != 2:
            return None, f"entry is not a (value, color) pair: {entry!r}"
        value, color = entry
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            return None, f"stop value is not a finite number: {value!r}"
        if not isinstance(color, str):
            return None, f"stop color is not a string: {color!r}"
        try:
            HEX_COLOR_VALIDATOR(color)
        except ValidationError:
            return None, f"stop color is not a hex color: {color!r}"
        stops.append({"value": float(value), "color": color})
    stops.sort(key=lambda stop: stop["value"])
    if stops[0]["value"] == stops[-1]["value"]:
        return None, "all stop values are equal — cannot derive a range"
    return stops, None


class SourceSetupService:
    """
    Turns selected CollectionDefinitions (plus operator config values) into
    persisted DB records.

    Usage::

        service = SourceSetupService()
        data_feed, collections = service.provision(
            CHIRPSDataFeed,
            catalog=catalog,
            feed_name="CHIRPS Africa",
            feed_interval=7200,
            global_config={"head_timeout": 20},
            selected_definitions=[
                (monthly_def, {"default_start_date": date(1981, 1, 1)}),
                (dekadal_def, {"default_start_date": date(1981, 1, 1)}),
            ],
        )
    """
    
    def provision(
            self,
            data_feed_model_cls,
            *,
            catalog,
            feed_name: str,
            feed_interval: int = 360,
            global_config: Optional[dict] = None,
            selected_definitions: list[tuple[CollectionDefinition, dict]],
    ) -> tuple:
        """
        Create DataFeed + Collections + Variables + Links atomically.

        Parameters
        ----------
        data_feed_model_cls : DataFeed subclass
        catalog             : Catalog instance (already saved)
        feed_name           : Name for the new DataFeed
        feed_interval       : Global interval_minutes for the DataFeed
        global_config       : Extra fields applied to the DataFeed (e.g. head_timeout)
        selected_definitions: List of (CollectionDefinition, config_values) pairs

        Returns (data_feed, collections).
        """
        global_config = global_config or {}
        
        with transaction.atomic():
            data_feed = self._create_data_feed(
                model_cls=data_feed_model_cls,
                name=feed_name,
                interval_minutes=feed_interval,
                catalog=catalog,
                extra_data=global_config,
            )
            
            created_collections = []
            for definition, config_values in selected_definitions:
                collection = self._provision_collection(
                    catalog=catalog,
                    definition=definition,
                    data_feed=data_feed,
                    config_values=config_values,
                )
                created_collections.append(collection)
            
            return data_feed, created_collections
    
    def provision_collection(
            self,
            *,
            catalog,
            definition: CollectionDefinition,
            data_feed,
            config_values: dict,
    ):
        """
        Provision a single collection for an existing DataFeed (used for the
        "Add collection" action on the detail page).
        """
        with transaction.atomic():
            return self._provision_collection(
                catalog=catalog,
                definition=definition,
                data_feed=data_feed,
                config_values=config_values,
            )
    
    def provision_derived_products(self, data_feed, selected_products: list) -> list:
        """
        Provision DerivedProduct rows for a feed from its declared definitions
        plus operator config and per-product enablement (ADR-0008).

        ``selected_products`` is a list of ``(DerivedProductDefinition, config,
        enabled)`` triples — one per *declared* definition, whether the operator
        ticked it or not. Each config is validated/coerced against the
        definition's config_schema *before* any write, so an invalid option
        rejects the whole batch atomically (nothing is half-provisioned).

        Idempotent: re-running upserts on (data_feed, definition_key), so a
        wizard revisit edits config rather than duplicating. ``is_enabled`` is
        set via ``create_defaults`` — only on row creation — so a re-run never
        clobbers an enable/disable toggle an operator changed after setup.
        """
        from georiva.sources.models import DerivedProduct
        from georiva.sources.product_service import materialise_and_pin

        with transaction.atomic():
            products = []
            for definition, config, enabled in selected_products:
                cleaned = definition.validate_config(config or {})
                # ``defaults`` drives the update path (config edits on a wizard
                # re-run); ``create_defaults`` drives the create path and is a
                # superset that also seeds is_enabled — so is_enabled is written
                # once at creation and a re-run never flips an operator's toggle.
                shared = {"recipe_type": definition.recipe_type, "config": cleaned}
                product, _created = DerivedProduct.objects.update_or_create(
                    data_feed=data_feed,
                    definition_key=definition.key,
                    defaults=shared,
                    create_defaults={**shared, "is_enabled": bool(enabled)},
                )
                # An enabled product materialises its output collections now, so
                # they appear in the catalog with declared titles before any run
                # (keyed on the row's actual state, not the wizard arg, so a
                # re-run never materialises for a product the operator disabled),
                # then pins its input/output bindings to those collections.
                if product.is_enabled:
                    materialise_and_pin(product, definition, data_feed)
                products.append(product)
                logger.info(
                    "DerivedProduct %s: feed=%s product=%s enabled=%s",
                    "created" if _created else "updated",
                    data_feed.pk, definition.key, product.is_enabled,
                )
            return products

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------
    
    @staticmethod
    def _create_data_feed(*, model_cls, name: str, interval_minutes: int, catalog, extra_data: Optional[dict] = None):
        defaults = {**model_cls.get_wizard_defaults(), **(extra_data or {})}
        data_feed = model_cls(
            name=name,
            interval_minutes=interval_minutes,
            catalog=catalog,
            **defaults,
        )
        data_feed.save()
        logger.info("Created DataFeed: %s (%s)", name, model_cls.__name__)
        return data_feed
    
    def _provision_collection(self, *, catalog, definition: CollectionDefinition, data_feed, config_values: dict):
        """Create/update Collection + Variables + Link for one CollectionDefinition."""
        # Slug is the definition key alone — no catalog prefix (ADR-0010 §5) — so
        # it matches the key the derived-product InputRef/OutputRef declarations
        # reference and the output collections materialise under. The bucket path
        # already carries the catalog segment, so the prefix was redundant.
        slug = slugify(definition.key)
        
        # selected_variable_keys is a wizard-only field, not stored on the link
        config_for_link = dict(config_values)
        selected_var_keys = config_for_link.pop('selected_variable_keys', None)
        
        collection = self._upsert_collection(
            catalog=catalog,
            slug=slug,
            name=definition.name,
            time_resolution=definition.time_resolution,
            is_forecast=definition.is_forecast,
        )
        
        variables_to_create = [
            v for v in definition.variables
            if selected_var_keys is None or v.key in selected_var_keys
        ]
        for var_def in variables_to_create:
            self._upsert_variable(collection, var_def)
        
        self._upsert_link(
            data_feed=data_feed,
            collection=collection,
            definition=definition,
            config_values=config_for_link,
        )
        
        return collection
    
    @staticmethod
    def _upsert_collection(*, catalog, slug: str, name: str, time_resolution: str, is_forecast: bool):
        from georiva.core.models import Collection
        
        collection, created = Collection.objects.update_or_create(
            catalog=catalog,
            slug=slug,
            defaults={
                "name": name,
                "time_resolution": time_resolution,
                "is_forecast": is_forecast,
            },
        )
        action = "created" if created else "updated"
        logger.info("Collection %s: %s/%s", action, catalog.slug, slug)
        return collection
    
    def _upsert_variable(self, collection, var_def: CollectionVariable):
        from georiva.core.models import Variable
        
        slug = slugify(var_def.key)
        source_unit = resolve_unit(var_def.source_units)
        output_unit = (
            resolve_unit(var_def.output_units)
            if var_def.output_units
            else source_unit
        )

        base_defaults = {
            "name": var_def.name,
            "description": var_def.description,
            "unit": output_unit,
            "source_unit": source_unit,
        }
        # Seed-vs-tune (ADR 0022): the plugin's declared range and styling
        # only seed a new Variable — an operator-tuned range or style must
        # survive re-provisioning. Canonical stops outrank the declared range:
        # when valid palette_stops are present the range derives from them.
        seed_stops, stops_error = _validated_palette_stops(var_def.palette_stops)
        seed_warnings = []
        if stops_error:
            seed_warnings.append(
                "Variable %s/%s: ignoring palette_stops (%s) — falling back "
                "to %s" % (
                    collection.slug, slug, stops_error,
                    f"ramp {var_def.palette!r}" if var_def.palette else "grayscale",
                )
            )
        if seed_stops:
            seed_min = seed_stops[0]["value"]
            seed_max = seed_stops[-1]["value"]
            if var_def.value_range and not (
                math.isclose(seed_min, var_def.value_range[0])
                and math.isclose(seed_max, var_def.value_range[1])
            ):
                seed_warnings.append(
                    "Variable %s/%s: declared value_range %s disagrees with "
                    "palette_stops span (%s, %s) — the stops win" % (
                        collection.slug, slug, var_def.value_range,
                        seed_min, seed_max,
                    )
                )
        elif var_def.value_range:
            seed_min, seed_max = var_def.value_range
        else:
            seed_min, seed_max = 0.0, 1.0
        seed_defaults = {"value_min": seed_min, "value_max": seed_max}
        
        if var_def.transform == 'passthrough':
            transform = Variable.TransformType.PASSTHROUGH
            sources_data = [self._source_key_to_block("primary", var_def.source_variable)]
        elif var_def.transform == 'vector_magnitude':
            transform = Variable.TransformType.VECTOR_MAGNITUDE
            sources_data = [
                self._source_key_to_block("u_component", var_def.components["u"]),
                self._source_key_to_block("v_component", var_def.components["v"]),
            ]
        else:  # vector_direction
            transform = Variable.TransformType.VECTOR_DIRECTION
            sources_data = [
                self._source_key_to_block("u_component", var_def.components["u"]),
                self._source_key_to_block("v_component", var_def.components["v"]),
            ]
        
        defaults = {**base_defaults, "transform_type": transform, "sources": sources_data}

        variable, created = Variable.objects.update_or_create(
            collection=collection,
            slug=slug,
            defaults=defaults,
            create_defaults={**defaults, **seed_defaults},
        )
        if created:
            # Seeding (and its degradation warnings) is create-only: a
            # re-provision neither writes styles nor nags about a declaration
            # it is not going to apply.
            for message in seed_warnings:
                logger.warning(message)
            self._seed_default_style(variable, var_def, seed_stops)
        action = "created" if created else "updated"
        logger.info("Variable %s: %s/%s", action, collection.slug, slug)
        return variable

    @staticmethod
    def _seed_default_style(variable, var_def: CollectionVariable, seed_stops):
        """Materialize the plugin's styling seed as the new Variable's default
        style (ADR 0022), precedence ``palette_stops`` > ``palette`` >
        grayscale. Create-only by construction: only called for a variable
        this provisioning run just created. The grayscale tier writes no row —
        serving already falls back to grayscale for a style-less variable.
        """
        from georiva.core.models import ColorRamp, VariableStyle
        from georiva.core.models.visualization import generate_stops

        if seed_stops:
            VariableStyle.objects.create(
                variable=variable,
                name="Default",
                slug="default",
                is_default=True,
                stops=seed_stops,
            )
            return
        if not var_def.palette:
            return

        # A plugin provisions into one organisation's catalog, so its ramp
        # vocabulary is that organisation's tier plus the instance-wide
        # catalog — the org's own ramp wins a name collision.
        organisation = variable.collection.catalog.organisation
        ramp = (
            ColorRamp.objects.filter(
                organisation=organisation, name__iexact=var_def.palette
            ).first()
            or ColorRamp.objects.filter(
                organisation__isnull=True, name__iexact=var_def.palette
            ).first()
        )
        if ramp is None:
            logger.warning(
                "Variable %s/%s: unknown color ramp %r — grayscale fallback",
                variable.collection.slug, variable.slug, var_def.palette,
            )
            return
        VariableStyle.objects.create(
            variable=variable,
            name=ramp.name,
            slug=slugify(ramp.name) or "default",
            is_default=True,
            ramp=ramp,
            stops=generate_stops(ramp, variable.value_min, variable.value_max),
        )
    
    @staticmethod
    def _upsert_link(*, data_feed, collection, definition: CollectionDefinition, config_values: dict):
        """Create or update a DataFeedCollectionLink with definition_key and config_values."""
        link_model = type(data_feed).get_collection_link_model()
        
        # Baked-in config from the plugin (e.g. CHIRPS period derived from definition key)
        baked_config = type(data_feed).get_link_config_for_definition(definition)
        
        interval = definition.default_interval_minutes
        
        link, _created = link_model.objects.update_or_create(
            data_feed=data_feed,
            collection=collection,
            defaults={
                "definition_key": definition.key,
                **({"interval_minutes": interval} if interval is not None else {}),
                **baked_config,  # plugin-derived, not user-editable
                **config_values,  # user-provided (can override baked config)
            },
        )
        action = "created" if _created else "updated"
        logger.info("DataFeedCollectionLink %s: feed=%s collection=%s", action, data_feed.pk, collection.slug)
        return link
    
    @staticmethod
    def _source_key_to_block(block_type: str, source_key) -> dict:
        """Adapt a definition SourceKey to a canonical sources block."""
        level = source_key.level
        return build_source_block(
            block_type,
            source_key.name,
            vertical_dimension=level.dimension if level else "",
            vertical_value=level.value if level else None,
        )
