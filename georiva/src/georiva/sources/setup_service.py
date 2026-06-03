"""
SourceSetupService — provisions Catalog → Collection → Variable records from a
ParameterManifest declared by a source plugin's describe_parameters() method.

Idempotent: re-running updates existing records (keyed by slug) rather than
creating duplicates, so adding new parameters to a plugin is safe to re-run.

ONE DataFeed is created/linked for all provisioned collections (M2M).
"""
import logging
from typing import Optional

from django.db import transaction
from django.utils.text import slugify

from georiva.sources.parameters import (
    Parameter,
    DerivedParameter,
    ParameterManifest,
)

logger = logging.getLogger("georiva.sources.setup_service")


class SourceSetupService:
    """
    Turns a ParameterManifest (plus operator choices) into persisted DB records.

    Usage::

        service = SourceSetupService()
        collections, data_feed = service.provision(
            manifest,
            catalog=catalog,
            selected_keys=['2t', 'wind_speed_10m', 'wind_dir_10m'],
            new_feed_name="ECMWF AIFS Africa",
            new_feed_interval=360,
            model_cls=ECMWFAIFSDataFeed,
        )
    """

    def provision(
            self,
            manifest: ParameterManifest,
            *,
            catalog,
            selected_keys: list[str],
            data_feed=None,
            new_feed_name: Optional[str] = None,
            new_feed_interval: int = 360,
            model_cls=None,
            group_into_collections: bool = True,
    ) -> tuple:
        """
        Materialise Collections + Variables for the selected parameter keys.

        DataFeed creation (mutually exclusive with data_feed):
        - If new_feed_name + model_cls are given, a new DataFeed is created and
          linked to ALL provisioned collections via M2M.
        - If data_feed is given, it is linked to all provisioned collections.

        Returns (collections, data_feed) where data_feed may be None.
        """
        selected_set = set(selected_keys)

        with transaction.atomic():
            # Create DataFeed if requested
            if new_feed_name and model_cls and data_feed is None:
                data_feed = self._create_data_feed(
                    model_cls=model_cls,
                    name=new_feed_name,
                    interval_minutes=new_feed_interval,
                    setup_via_wizard=True,
                )

            created_collections = []

            if group_into_collections:
                for group in manifest.groups:
                    group_keys = [k for k in group.member_keys if k in selected_set]
                    if not group_keys:
                        continue
                    collection = self._upsert_collection(
                        catalog=catalog,
                        slug=slugify(group.key),
                        name=group.name,
                    )
                    for key in group_keys:
                        self._upsert_variable(collection, manifest.by_key(key))
                    if data_feed:
                        data_feed.collections.add(collection)
                    created_collections.append(collection)

            ungrouped = [k for k in manifest.ungrouped_keys() if k in selected_set]
            if ungrouped:
                collection = self._upsert_collection(
                    catalog=catalog,
                    slug=slugify(catalog.slug),
                    name=catalog.name,
                )
                for key in ungrouped:
                    self._upsert_variable(collection, manifest.by_key(key))
                if data_feed:
                    data_feed.collections.add(collection)
                created_collections.append(collection)

            return created_collections, data_feed

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _create_data_feed(*, model_cls, name: str, interval_minutes: int, setup_via_wizard: bool = False):
        """Create a new DataFeed subclass instance using wizard defaults."""
        defaults = model_cls.get_wizard_defaults()
        data_feed = model_cls(
            name=name,
            interval_minutes=interval_minutes,
            setup_via_wizard=setup_via_wizard,
            **defaults,
        )
        data_feed.save()
        logger.info("Created DataFeed: %s (%s)", name, model_cls.__name__)
        return data_feed

    @staticmethod
    def _upsert_collection(*, catalog, slug: str, name: str):
        from georiva.core.models import Collection

        collection, created = Collection.objects.get_or_create(
            catalog=catalog,
            slug=slug,
            defaults={"name": name},
        )
        action = "created" if created else "found existing"
        logger.info("Collection %s: %s/%s", action, catalog.slug, slug)
        return collection

    def _upsert_variable(self, collection, param: 'Parameter | DerivedParameter'):
        from georiva.core.models import Variable

        slug = slugify(param.key)
        unit = self._get_or_create_unit(param.units)

        base_defaults = {
            "name": param.name,
            "description": param.description,
            "unit": unit,
            "source_unit": unit,
            "value_min": param.value_range[0] if param.value_range else 0.0,
            "value_max": param.value_range[1] if param.value_range else 1.0,
        }

        if isinstance(param, Parameter):
            transform = Variable.TransformType.PASSTHROUGH
            sources_data = [self._source_key_to_block("primary", param.source)]
        else:
            transform = param.transform  # 'vector_magnitude' or 'vector_direction'
            sources_data = [
                self._source_key_to_block("u_component", param.components["u"]),
                self._source_key_to_block("v_component", param.components["v"]),
            ]

        defaults = {**base_defaults, "transform_type": transform, "sources": sources_data}

        variable, created = Variable.objects.get_or_create(
            collection=collection,
            slug=slug,
            defaults=defaults,
        )

        action = "created" if created else "found existing"
        logger.info("Variable %s: %s/%s", action, collection.slug, slug)
        return variable

    @staticmethod
    def _source_key_to_block(block_type: str, source_key) -> dict:
        level = source_key.level
        return {
            "type": block_type,
            "value": {
                "source_name": source_key.name,
                "vertical_dimension": level.dimension if level else "",
                "vertical_value": level.value if level else None,
            },
        }

    @staticmethod
    def _get_or_create_unit(symbol: str):
        from georiva.core.models import Unit

        unit, created = Unit.objects.get_or_create(
            symbol=symbol,
            defaults={"name": symbol},
        )
        if created:
            logger.info("Created Unit: %s", symbol)
        return unit
