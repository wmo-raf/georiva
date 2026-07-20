"""
SourceSetupService — provisions Catalog → Collection → Variable records from
CollectionDefinition objects declared by a DataFeed plugin.

Idempotent: re-running updates existing records (keyed by slug) rather than
creating duplicates, so adding new collections to a plugin is safe to re-run.
"""
import logging
from typing import Optional

from django.db import transaction
from django.utils.text import slugify

from georiva.core.provisioning import build_source_block, resolve_unit
from georiva.sources.collection_definitions import CollectionDefinition, CollectionVariable

logger = logging.getLogger("georiva.sources.setup_service")


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
            "value_min": var_def.value_range[0] if var_def.value_range else 0.0,
            "value_max": var_def.value_range[1] if var_def.value_range else 1.0,
        }
        
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
        )
        action = "created" if created else "updated"
        logger.info("Variable %s: %s/%s", action, collection.slug, slug)
        return variable
    
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
