"""
CollectionDefinition contract for GeoRiva source plugins.

Plugins declare the exact set of collections they can produce by implementing
DataFeed.get_collection_definitions() and returning a list of CollectionDefinition
objects. The wizard uses this list to build the Collections step UI; the setup
service uses it to provision Collection + Variable + DataFeedCollectionLink records.
"""
from dataclasses import dataclass, field
from typing import Optional

from georiva.sources.parameters import SourceKey


@dataclass(frozen=True)
class CollectionVariable:
    """
    One variable within a CollectionDefinition.

    Scalar variables use transform='passthrough' (default) and source_variable.
    Vector-derived variables (wind speed, wind direction) use
    transform='vector_magnitude' or 'vector_direction' and components instead
    of source_variable.

    source_units (required) is the raw unit of the data as it leaves the source
    file (or, for transforms, as it leaves the transform). output_units is the
    unit this variable is exposed in: when set and different from source_units,
    the ingestion pipeline converts source_units -> output_units via pint. When
    output_units is None it defaults to source_units, i.e. no conversion.
    """
    key: str
    name: str
    source_units: str
    output_units: Optional[str] = None
    source_variable: Optional[SourceKey] = None
    transform: str = 'passthrough'
    components: Optional[dict[str, SourceKey]] = None
    description: str = ''
    value_range: Optional[tuple[float, float]] = None
    palette: Optional[str] = None

    def __post_init__(self):
        if self.transform == 'passthrough' and self.source_variable is None:
            raise ValueError(f"CollectionVariable '{self.key}': passthrough transform requires 'source_variable'")
        if self.transform != 'passthrough' and self.components is None:
            raise ValueError(f"CollectionVariable '{self.key}': derived transform requires 'components'")

    @property
    def exposed_units(self) -> str:
        """Unit this variable is exposed in: output_units, or source_units when
        no conversion is declared."""
        return self.output_units or self.source_units


@dataclass(frozen=True)
class VariableGroup:
    """
    A purely UX grouping of variables within a CollectionDefinition.

    Groups are rendered in the wizard as collapsible sections with a
    group-level "check all" checkbox.  They have no effect on the data model.
    Variables not covered by any group are shown in an "Other" section.
    """
    key: str
    name: str
    variable_keys: tuple[str, ...]
    
    def __init__(self, key: str, name: str, variable_keys):
        object.__setattr__(self, 'key', key)
        object.__setattr__(self, 'name', name)
        object.__setattr__(self, 'variable_keys', tuple(variable_keys))


@dataclass(frozen=True)
class CollectionDefinition:
    """
    Describes one collection a DataFeed plugin can create.

    The wizard presents all definitions from get_collection_definitions() as a
    checklist. For each selected definition the setup service creates:
      - One Collection (slug derived from catalog.slug + definition.key)
      - One Variable per entry in variables
      - One DataFeedCollectionLink (definition_key stored for later reference)

    Per-collection configuration (e.g. default start date) is collected in the
    wizard via DataFeedCollectionLink.get_form_class() — declare fields there,
    not here.

    default_interval_minutes — if set, pre-fills the collection link's
    interval_minutes. Useful when different collections have different cadences
    (e.g. CHIRPS monthly vs pentadal).
    """
    key: str
    name: str
    time_resolution: str
    variables: tuple['CollectionVariable', ...]
    groups: tuple[VariableGroup, ...] = field(default_factory=tuple)
    description: str = ''
    is_forecast: bool = False
    default_interval_minutes: Optional[int] = None

    def __init__(
            self,
            key: str,
            name: str,
            time_resolution: str,
            variables,
            groups=(),
            description: str = '',
            is_forecast: bool = False,
            default_interval_minutes: Optional[int] = None,
    ):
        object.__setattr__(self, 'key', key)
        object.__setattr__(self, 'name', name)
        object.__setattr__(self, 'time_resolution', time_resolution)
        object.__setattr__(self, 'variables', tuple(variables))
        object.__setattr__(self, 'groups', tuple(groups))
        object.__setattr__(self, 'description', description)
        object.__setattr__(self, 'is_forecast', is_forecast)
        object.__setattr__(self, 'default_interval_minutes', default_interval_minutes)

    def get_variable(self, key: str) -> CollectionVariable:
        for v in self.variables:
            if v.key == key:
                return v
        raise KeyError(f"No variable '{key}' in CollectionDefinition '{self.key}'")


# =============================================================================
# Developer-friendly dict → CollectionDefinition parser
# =============================================================================

def parse_collection_defs(raw: dict) -> list['CollectionDefinition']:
    """
    Convert a plain-dict collection spec to a list of CollectionDefinition objects.

    This is a convenience layer for plugin developers who prefer writing plain
    Python dicts over importing and constructing the dataclasses directly.

    Dict format::

        COLLECTIONS = {
            "chirps-monthly": {                     # → CollectionDefinition.key
                "name": "CHIRPS Monthly",           # required
                "time_resolution": "monthly",       # required
                "description": "...",               # optional
                "is_forecast": False,               # optional, default False
                "default_interval_minutes": 43200,  # optional
                "groups": [...],                    # optional, see _parse_group
                "variables": [                      # required
                    {
                        "key": "precip",            # optional; slugified from name if absent
                        "name": "Precipitation",    # required
                        "source_units": "m",       # required (raw unit of source data)
                        "output_units": "mm",      # optional; exposed unit (defaults to source_units)
                        "source_variable": "band_1",  # str shorthand, OR dict with name/level
                        "value_range": (0.0, 2000.0),  # optional
                        "description": "",         # optional
                        "palette": None,           # optional
                        # For derived (vector) variables:
                        "transform": "vector_magnitude",
                        "components": {"u": "10u", "v": "10v"},
                    }
                ],
            },
        }

        return parse_collection_defs(COLLECTIONS)

    Source shorthand: ``"source_variable": "band_1"`` is equivalent to
    ``"source_variable": {"name": "band_1", "level": None}``.
    """
    return [_parse_collection(key, data) for key, data in raw.items()]


def _parse_collection(key: str, data: dict) -> CollectionDefinition:
    return CollectionDefinition(
        key=key,
        name=data['name'],
        time_resolution=data['time_resolution'],
        variables=tuple(_parse_variable(v) for v in data.get('variables', [])),
        groups=tuple(_parse_group(g) for g in data.get('groups', [])),
        description=data.get('description', ''),
        is_forecast=data.get('is_forecast', False),
        default_interval_minutes=data.get('default_interval_minutes'),
    )


def _parse_variable(v: dict) -> CollectionVariable:
    from django.utils.text import slugify
    
    key = v.get('key') or slugify(v['name'])
    transform = v.get('transform', 'passthrough')
    
    if transform == 'passthrough':
        return CollectionVariable(
            key=key,
            name=v['name'],
            source_units=v['source_units'],
            output_units=v.get('output_units'),
            source_variable=_parse_source_key(v['source_variable']),
            description=v.get('description', ''),
            value_range=tuple(v['value_range']) if v.get('value_range') else None,
            palette=v.get('palette'),
        )

    # Vector-derived variable
    components = {k: _parse_source_key(s) for k, s in v['components'].items()}
    return CollectionVariable(
        key=key,
        name=v['name'],
        source_units=v['source_units'],
        output_units=v.get('output_units'),
        transform=transform,
        components=components,
        description=v.get('description', ''),
        value_range=tuple(v['value_range']) if v.get('value_range') else None,
        palette=v.get('palette'),
    )


def _parse_source_key(source) -> Optional[SourceKey]:
    """Accept a string shorthand or a dict with name/level."""
    from georiva.sources.parameters import Level
    
    if source is None:
        return None
    if isinstance(source, str):
        return SourceKey(name=source)
    level_data = source.get('level')
    level = None
    if level_data:
        level = Level(
            type=level_data['type'],
            value=level_data.get('value'),
            dimension=level_data.get('dimension'),
            unit=level_data.get('unit'),
        )
    return SourceKey(name=source['name'], level=level)


def _parse_group(g: dict) -> VariableGroup:
    return VariableGroup(
        key=g['key'],
        name=g['name'],
        variable_keys=g.get('variable_keys', []),
    )
