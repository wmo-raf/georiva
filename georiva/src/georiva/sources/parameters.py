"""
Parameter manifest contract for GeoRiva source plugins.

"""
from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class Level:
    """A point on a vertical dimension."""
    type: str  # 'surface' | 'pressure' | 'heightAboveGround'
    value: Optional[float] = None  # 850, 2, 10
    dimension: Optional[str] = None  # GRIB key: 'isobaricInhPa', 'heightAboveGround'
    unit: Optional[str] = None  # 'hPa', 'm'


@dataclass(frozen=True)
class SourceKey:
    """How to locate the raw band inside a source file."""
    name: str  # GRIB shortName / NetCDF var / 'band_1'
    level: Optional[Level] = None


@dataclass(frozen=True)
class Parameter:
    """An atomic, directly-readable quantity -> PASSTHROUGH Variable."""
    key: str  # '2t', 'u850'
    name: str  # '2m Temperature'
    units: str
    source: SourceKey
    description: str = ''
    standard_name: Optional[str] = None  # CF standard name
    value_range: Optional[tuple[float, float]] = None
    palette: Optional[str] = None  # palette slug


@dataclass(frozen=True)
class DerivedParameter:
    """A composite quantity -> VECTOR_MAGNITUDE or VECTOR_DIRECTION Variable."""
    key: str  # 'wind_speed_10m'
    name: str  # '10m Wind Speed'
    units: str
    transform: str  # must match Variable.TransformType values
    components: dict[str, SourceKey]  # {'u': SourceKey(...), 'v': SourceKey(...)}
    description: str = ''
    value_range: Optional[tuple[float, float]] = None
    palette: Optional[str] = None


@dataclass(frozen=True)
class ParameterGroup:
    """A recommended bundle the wizard can turn into one Collection."""
    key: str
    name: str  # '10m Wind'
    member_keys: tuple[str, ...]  # keys of Parameter / DerivedParameter
    
    def __init__(self, key: str, name: str, member_keys):
        # Allow list or tuple for member_keys; store as tuple for hashability
        object.__setattr__(self, 'key', key)
        object.__setattr__(self, 'name', name)
        object.__setattr__(self, 'member_keys', tuple(member_keys))


@dataclass(frozen=True)
class ParameterManifest:
    parameters: tuple[Parameter, ...] = field(default_factory=tuple)
    derived: tuple[DerivedParameter, ...] = field(default_factory=tuple)
    groups: tuple[ParameterGroup, ...] = field(default_factory=tuple)
    
    def __init__(self, parameters=(), derived=(), groups=()):
        object.__setattr__(self, 'parameters', tuple(parameters))
        object.__setattr__(self, 'derived', tuple(derived))
        object.__setattr__(self, 'groups', tuple(groups))
    
    def by_key(self, key: str) -> 'Parameter | DerivedParameter':
        """Resolve a key to a Parameter or DerivedParameter."""
        for p in (*self.parameters, *self.derived):
            if p.key == key:
                return p
        raise KeyError(key)
    
    def all_keys(self) -> list[str]:
        """All parameter keys in declaration order: scalars first, then derived."""
        return [p.key for p in (*self.parameters, *self.derived)]
    
    def ungrouped_keys(self) -> list[str]:
        """Keys that don't belong to any ParameterGroup."""
        grouped = {k for g in self.groups for k in g.member_keys}
        return [k for k in self.all_keys() if k not in grouped]


def expand_levels(
        base_key: str,
        base_name: str,
        units: str,
        source_name: str,
        levels: list[Level],
        **kwargs,
) -> list[Parameter]:
    """
    Produce one Parameter per level.

    Example: expand_levels('t', 'Temperature', 'K', 't', pl) ->
        [Parameter('t_1000', 'Temperature at 1000 hPa', ...), ...]
    """
    out = []
    for lv in levels:
        level_label = f"{lv.value:.0f}" if lv.value is not None else lv.type
        unit_label = f" {lv.unit}" if lv.unit else ""
        out.append(Parameter(
            key=f"{base_key}_{level_label}",
            name=f"{base_name} at {level_label}{unit_label}".strip(),
            units=units,
            source=SourceKey(source_name, lv),
            **kwargs,
        ))
    return out
