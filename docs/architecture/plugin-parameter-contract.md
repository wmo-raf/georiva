# Source Plugin Parameter Contract & Setup Wizard

**Geospatial Raster Ingestion, Visualization & Analysis**

|             |                              |
|-------------|------------------------------|
| **Status**  | Draft — Request for Comments |
| **Version** | 0.1                          |
| **Date**    | 2026-05-29                   |
| **Author**  | Erick                        |

---

## Table of Contents

- [1. Motivation](#1-motivation)
- [2. Goals & Non-Goals](#2-goals--non-goals)
- [3. What Already Exists](#3-what-already-exists)
- [4. The Contract](#4-the-contract)
    - [4.1 Data Structures](#41-data-structures)
    - [4.2 The `describe_parameters()` Method](#42-the-describe_parameters-method)
    - [4.3 Reference Declaration (ECMWF AIFS)](#43-reference-declaration-ecmwf-aifs)
- [5. Mapping the Manifest onto Core Models](#5-mapping-the-manifest-onto-core-models)
- [6. The Setup Wizard / Provisioning Service](#6-the-setup-wizard--provisioning-service)
- [7. Backwards Compatibility & Migration](#7-backwards-compatibility--migration)
- [8. Open Questions](#8-open-questions)

---

## 1. Motivation

Today, wiring a new data source into GeoRiva is a manual, multi-step task. An operator must
hand-author a `Catalog`, one or more `Collection`s, and every `Variable` inside them through the
Wagtail admin — typing parameter keys, full names, units, vertical levels, value ranges, and
palettes by hand, and re-deriving combined products (such as wind speed/direction from U and V
components) from memory.

The data source plugin already *knows* all of this. An ECMWF AIFS source knows it provides `2t`,
`10u`, `10v`, `msl`, temperature on thirteen pressure levels, and that `10u`/`10v` combine into a
10 m wind vector. There is no contract that lets a plugin *declare* this knowledge in a structured,
machine-readable way so the system can populate the catalog automatically.

This document proposes a **parameter manifest contract** that every source plugin can implement, and
a **setup wizard** that consumes the manifest to provision `Catalog → Collection → Variable` rows.

## 2. Goals & Non-Goals

**Goals**

- A plugin can enumerate every parameter it provides: canonical key, full human name, units, and the
  raw source binding (GRIB shortName / NetCDF variable / GeoTIFF band).
- A plugin can declare *combinations* — most importantly vector pairs (U/V) that become magnitude and
  direction — as first-class entries, not as something the operator must reconstruct.
- A plugin can declare vertical level dimensions (pressure, height) once and let the manifest expand
  them into concrete per-level parameters.
- The declaration drives automatic creation of `Catalog`, `Collection`, and `Variable` records.
- The contract is format-agnostic: it works for GRIB, NetCDF, and GeoTIFF sources alike.

**Non-Goals**

- This is not a replacement for manual authoring. Operators must still be able to fine-tune the
  generated records (palettes, value ranges, clipping) afterwards.
- It does not change the ingestion pipeline, the fetch strategies, or the storage layout.
- It does not attempt to auto-discover parameters by downloading and introspecting files (although a
  plugin's implementation of the contract *may* do this internally — e.g. by reading a GRIB `.index`
  file).

## 3. What Already Exists

The proposal builds on existing primitives rather than introducing parallel concepts.

`BaseDataSource.get_available_variables()` (`sources/source.py`) already returns a loosely-typed
`list[dict]` with `slug`, `name`, `units`, `level_type`, and `level`. This is the embryonic form of
the manifest — but it is untyped, has no notion of derived products, and forces each plugin to
hand-roll the level cartesian product (see the ECMWF plugin's `get_available_variables()`).

More importantly, the `Variable` model (`core/models/variable.py`) **already encodes combinations**:

- `VariableSourceStreamBlock` defines `primary`, `u_component`, and `v_component` source blocks, each
  carrying `source_name`, `vertical_dimension`, and `vertical_value`.
- `Variable.TransformType` defines `PASSTHROUGH`, `VECTOR_MAGNITUDE` (√(u²+v²)), and
  `VECTOR_DIRECTION` (atan2).

So "Wind U,V → speed/direction" is already a fully-supported concept at the data-model layer. It is
simply authored by hand today instead of being declared by the plugin. The contract closes that gap.

## 4. The Contract

### 4.1 Data Structures

A new module, `georiva/sources/parameters.py`, defines the typed manifest. All structures are frozen
dataclasses with no Django dependencies, so they can be unit-tested in isolation and imported from
plugins without circular-import concerns.

```python
# georiva/sources/parameters.py
from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class Level:
    """A point on a vertical dimension."""
    type: str                          # 'surface' | 'pressure' | 'heightAboveGround'
    value: Optional[float] = None      # 850, 2, 10
    dimension: Optional[str] = None    # GRIB key: 'isobaricInhPa', 'heightAboveGround'
    unit: Optional[str] = None         # 'hPa', 'm'


@dataclass(frozen=True)
class SourceKey:
    """How to locate the raw band inside a source file."""
    name: str                          # GRIB shortName / NetCDF var / 'band_1'
    level: Optional[Level] = None


@dataclass(frozen=True)
class Parameter:
    """An atomic, directly-readable quantity -> PASSTHROUGH Variable."""
    key: str                           # '2t', 'u'
    name: str                          # 'Temperature'
    units: str
    source: SourceKey
    description: str = ''
    standard_name: Optional[str] = None             # CF standard name
    value_range: Optional[tuple[float, float]] = None
    palette: Optional[str] = None                   # palette slug


@dataclass(frozen=True)
class DerivedParameter:
    """A composite -> VECTOR_MAGNITUDE / VECTOR_DIRECTION Variable."""
    key: str                           # 'wind_speed_10m'
    name: str                          # '10m Wind Speed'
    units: str
    transform: str                     # must match Variable.TransformType values
    components: dict[str, SourceKey]   # {'u': SourceKey(...), 'v': SourceKey(...)}
    description: str = ''
    value_range: Optional[tuple[float, float]] = None
    palette: Optional[str] = None


@dataclass(frozen=True)
class ParameterGroup:
    """A recommended bundle the wizard can turn into one Collection."""
    key: str
    name: str                          # '10m Wind'
    member_keys: list[str]             # keys of Parameter / DerivedParameter


@dataclass(frozen=True)
class ParameterManifest:
    parameters: list[Parameter] = field(default_factory=list)
    derived: list[DerivedParameter] = field(default_factory=list)
    groups: list[ParameterGroup] = field(default_factory=list)

    def by_key(self, key: str):
        """Resolve a key to a Parameter or DerivedParameter."""
        for p in (*self.parameters, *self.derived):
            if p.key == key:
                return p
        raise KeyError(key)
```

A small helper keeps the level cartesian product out of plugin code:

```python
def expand_levels(
    base_key: str,
    base_name: str,
    units: str,
    source_name: str,
    levels: list[Level],
    **kwargs,
) -> list[Parameter]:
    """Produce one Parameter per level, e.g. t_850, t_700, ..."""
    out = []
    for lv in levels:
        out.append(Parameter(
            key=f"{base_key}_{lv.value:.0f}",
            name=f"{base_name} at {lv.value:.0f} {lv.unit or ''}".strip(),
            units=units,
            source=SourceKey(source_name, lv),
            **kwargs,
        ))
    return out
```

### 4.2 The `describe_parameters()` Method

A single new method is added to the `DataSource` protocol and `BaseDataSource`:

```python
class BaseDataSource(ABC):
    def describe_parameters(self) -> ParameterManifest:
        """Declare every parameter (and combination) this source provides."""
        raise NotImplementedError
```

`get_available_variables()` is retained as a thin, derived adapter (see §7) so nothing downstream
breaks during the transition.

### 4.3 Reference Declaration (ECMWF AIFS)

```python
from georiva.sources.parameters import (
    ParameterManifest, Parameter, DerivedParameter, ParameterGroup,
    Level, SourceKey, expand_levels,
)

def describe_parameters(self) -> ParameterManifest:
    pl = [Level('pressure', lv, 'isobaricInhPa', 'hPa') for lv in self.PRESSURE_LEVELS]

    parameters = [
        Parameter('2t', '2m Temperature', 'K',
                  SourceKey('2t', Level('heightAboveGround', 2, 'heightAboveGround', 'm')),
                  value_range=(233, 323)),
        Parameter('msl', 'Mean Sea Level Pressure', 'Pa', SourceKey('msl')),
        Parameter('tp', 'Total Precipitation', 'm', SourceKey('tp')),
        *expand_levels('t', 'Temperature', 'K', 't', pl),
        *expand_levels('z', 'Geopotential', 'm2/s2', 'z', pl),
        *expand_levels('q', 'Specific Humidity', 'kg/kg', 'q', pl),
    ]

    derived = [
        DerivedParameter('wind_speed_10m', '10m Wind Speed', 'm/s',
                         transform='vector_magnitude',
                         components={'u': SourceKey('10u'), 'v': SourceKey('10v')}),
        DerivedParameter('wind_dir_10m', '10m Wind Direction', 'deg',
                         transform='vector_direction',
                         components={'u': SourceKey('10u'), 'v': SourceKey('10v')}),
    ]

    groups = [
        ParameterGroup('wind_10m', '10m Wind', ['wind_speed_10m', 'wind_dir_10m']),
        ParameterGroup('surface', 'Surface', ['2t', 'msl', 'tp']),
    ]

    return ParameterManifest(parameters, derived, groups)
```

## 5. Mapping the Manifest onto Core Models

The manifest maps cleanly onto the existing models with **no schema changes required**:

| Manifest entity                         | Core model result                                                      |
|-----------------------------------------|-----------------------------------------------------------------------|
| `Parameter`                             | `Variable(transform_type=PASSTHROUGH)` with one `primary` source block |
| `DerivedParameter` (`vector_magnitude`) | `Variable(transform_type=VECTOR_MAGNITUDE)` with `u_component` + `v_component` blocks |
| `DerivedParameter` (`vector_direction`) | `Variable(transform_type=VECTOR_DIRECTION)` with `u_component` + `v_component` blocks |
| `SourceKey`                             | A `SourceBlock` (`source_name`, `vertical_dimension`, `vertical_value`) |
| `ParameterGroup`                        | One `Collection` containing the member `Variable`s                     |
| (manifest as a whole)                   | One `Catalog` for the source                                           |

The `SourceKey → SourceBlock` translation is direct: `SourceKey.name → source_name`,
`Level.dimension → vertical_dimension`, `Level.value → vertical_value`.

## 6. The Setup Wizard / Provisioning Service

A stateless service turns a manifest (plus operator selections) into persisted records:

```python
class SourceSetupService:
    def provision(
        self,
        manifest: ParameterManifest,
        *,
        catalog: Catalog,
        selected_keys: list[str],
        group_into_collections: bool = True,
    ) -> list[Collection]:
        """
        Materialize Collections + Variables for the selected parameter keys.
        - vector DerivedParameters become VECTOR_* Variables with u/v source blocks
        - groups become Collections; ungrouped selections fall into a default Collection
        Idempotent: re-running updates rather than duplicates (keyed by slug).
        """
        ...
```

The Wagtail-facing wizard is a multi-step view registered via `sources/wagtail_hooks.py`:

1. **Pick a source** — choose the plugin / `LoaderProfile` type.
2. **Review parameters** — render `describe_parameters()` as a checklist, grouped by
   `ParameterGroup`, with level multi-selects for expandable parameters.
3. **Confirm targets** — choose/confirm the `Catalog` and whether groups become separate
   `Collection`s.
4. **Provision** — call `SourceSetupService.provision()` and link the resulting `Collection`s to the
   `LoaderProfile`.

Because provisioning is idempotent (keyed on slug), re-running the wizard after a plugin adds new
parameters (e.g. the AIFS v2 wave fields) simply tops up the catalog.

## 7. Backwards Compatibility & Migration

- `get_available_variables()` becomes a thin adapter that flattens `describe_parameters()` into the
  legacy dict shape, so existing callers keep working unchanged.
- `describe_parameters()` raises `NotImplementedError` by default; plugins adopt it incrementally.
  The wizard only offers sources that implement it.
- The ECMWF AIFS plugin is migrated first as the reference implementation; CHIRPS follows.
- No migrations are needed for the core models — `Variable`, `Collection`, and `Catalog` are unchanged.

## 8. Open Questions

1. **Value ranges & palettes** — should sensible defaults live in the plugin manifest, in a shared
   parameter dictionary (keyed by CF standard name), or be left blank for the operator? A shared
   dictionary would let many sources reuse one good "2m temperature" palette.
2. **Idempotency key** — slug-based upsert is simple but brittle if names change. Should we store the
   originating manifest `key` on the `Variable` for stable round-tripping?
3. **Level selection granularity** — expose every declared level in the wizard, or let the plugin
   mark a recommended subset as default-on?
4. **Cross-source standard names** — adopting CF `standard_name` consistently would enable
   cross-source search and unit reconciliation; worth requiring in the contract?
5. **Relationship to the analysis layer** — derived analysis products (anomalies, indices) also
   produce Variables. Should the analysis plugin contract share this manifest vocabulary?
