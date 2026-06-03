# GeoRiva Data Model Guide

> How to organize your data into Catalogs, Collections, and Variables.
>
> Part of the [GeoRiva documentation](README.md). See also
> the [Architecture Design Document §4](architecture/README.md#4-data-model)
> for the data model's place in the system, the [Format Plugin System](format-plugins.md) for how raw files are read
> into Variables, and [Download Deduplication](architecture/download-dedup.md) for organizing plugin collections.

## The Hierarchy

GeoRiva organizes geospatial data in a three-level hierarchy:

```
Catalog
└── Collection
    └── Variable
        └── VariableSource(s)
```

**Catalog** represents a data provider or product family. It defines how data is ingested: the file format, the loader
plugin, the boundary for clipping, and whether to archive source files. Think of it as "where does this data come from?"

**Collection** represents a specific dataset within a catalog. It groups variables that share the same spatial grid,
temporal resolution, and source file. Think of it as "what file am I processing?"

**Variable** represents a single measurable quantity that users see and interact with. It defines visualization (color
palette, value range, scale type), units, and how to extract the data from the source file. Think of it as "what do I
want to show on the map?"

**Sources** link a Variable to its raw parameter(s) in the source file. A Variable's `sources` is a Wagtail
**StreamField** (not a separate model): each entry is a *Source* block of one of three kinds — `primary`,
`u_component`, or `v_component`. A passthrough variable has exactly one `primary` source; a derived vector variable has
one `u_component` and one `v_component`. Each Source block carries a `source_name` (the exact name in the file) plus
optional `vertical_dimension` / `vertical_value` for level selection.

---

> ## Implementation status — read this first
>
> The transforms available today are defined by `Variable.TransformType` in
> [`core/models/variable.py`](../georiva/src/georiva/core/models/variable.py):
>
> | `transform_type`   | Sources required             | Output                                  |
> |--------------------|------------------------------|-----------------------------------------|
> | `passthrough`      | one `primary`                | the source band, read directly          |
> | `vector_magnitude` | `u_component` + `v_component`| wind speed √(u² + v²)                    |
> | `vector_direction` | `u_component` + `v_component`| meteorological wind direction (atan2)   |
>
> **Arbitrary band math is not implemented yet.** There is no `band_math` transform and no
> `transform_expression` field, so the NDVI/EVI and other band-arithmetic examples below are included to
> illustrate *how you would structure* such data — they describe a planned capability, not current behavior.
> Likewise, unit handling is done with two `Unit` foreign keys, **`source_unit`** (units in the file) and
> **`unit`** (output units); there is no `unit_conversion` string field. Wherever an example shows
> `transform: band_math` or `unit_conversion: …`, read it as illustrative.

---

## The Key Principle

**Collections should align with source files, not with individual parameters.**

A GRIB file from ECMWF AIFS contains temperature, precipitation, wind components, humidity, and pressure — all in one
file, on the same grid, at the same timestamps. That file should map to one Collection with many Variables, not many
Collections with one Variable each.

This matters because of how the ingestion pipeline works. When a file lands in storage, the pipeline downloads it once,
extracts timestamps once, and then loops through all Variables in the Collection. If you split each parameter into its
own Collection, the pipeline processes the same file multiple times — downloading, parsing, and iterating redundantly.

```
One file → one Collection → many Variables → one processing pass
```

---

## Deciding How to Structure Your Data

### When to use one Collection (most cases)

Use a single Collection when all variables come from the same source file, share the same spatial grid (resolution,
extent, CRS), share the same temporal resolution, and are always produced together.

**Example: ECMWF AIFS**

ECMWF AIFS produces a single GRIB file per model run containing all surface parameters on the same 0.25° global grid at
the same forecast steps.

```
Catalog: ECMWF AIFS
└── Collection: ecmwf-aifs
    ├── Variable: temperature
    │   └── Source: TMP_2maboveground (passthrough, K→°C)
    ├── Variable: precipitation
    │   └── Source: APCP_surface (passthrough, kg/m²/s→mm)
    ├── Variable: relative_humidity
    │   └── Source: RH_2maboveground (passthrough)
    ├── Variable: wind_speed
    │   ├── Source: UGRD_10maboveground (role: u_component)
    │   └── Source: VGRD_10maboveground (role: v_component)
    │   (transform: vector_magnitude → √(u² + v²))
    └── Variable: wind_direction
        ├── Source: UGRD_10maboveground (role: u_component)
        └── Source: VGRD_10maboveground (role: v_component)
        (transform: vector_direction → atan2)
```

Wind speed and wind direction share the same two sources but use different transforms. This is exactly what the
Variable/VariableSource model is designed for.

**Example: CHIRPS Rainfall**

CHIRPS is a single-parameter product. One file, one variable.

```
Catalog: CHIRPS
└── Collection: chirps-daily
    └── Variable: precipitation
        └── Source: precip (passthrough)
```

Simple and clean. No need for multiple collections.

**Example: MSG/SEVIRI**

Meteosat produces multiple channels, all on the same grid and timestamp.

```
Catalog: MSG SEVIRI
└── Collection: msg-seviri
    ├── Variable: visible_06
    │   └── Source: VIS006
    ├── Variable: infrared_108
    │   └── Source: IR_108
    ├── Variable: water_vapor
    │   └── Source: WV_062
    └── Variable: cloud_top_temperature
        └── Source: IR_108 (passthrough, with different value range/palette)
```

### When to use multiple Collections

Split into multiple Collections when data comes in **separate files** with different characteristics. The deciding
factors are: different spatial grids, different temporal resolutions, different file formats, or data that is produced
and delivered independently.

**Example: GFS (different forecast horizons)**

GFS produces hourly forecasts for the first 120 hours and 3-hourly forecasts from 120 to 384 hours. These come as
separate files with different temporal spacing.

```
Catalog: GFS
├── Collection: gfs-hourly
│   │   (0–120h, hourly, 0.25° grid)
│   ├── Variable: temperature
│   ├── Variable: precipitation
│   └── Variable: wind_speed
└── Collection: gfs-3hourly
        (120–384h, 3-hourly, 0.25° grid)
    ├── Variable: temperature
    ├── Variable: precipitation
    └── Variable: wind_speed
```

The same variables appear in both collections, but the temporal resolution differs. A plugin fetching GFS data would
save hourly files to `gfs/gfs-hourly/` and 3-hourly files to `gfs/gfs-3hourly/`.

**Example: ERA5 (different grids)**

ERA5 has pressure-level data (3D, on pressure surfaces) and single-level data (2D, surface only). These are different
files with different dimensionality.

```
Catalog: ERA5
├── Collection: era5-surface
│   │   (single level, 0.25° grid)
│   ├── Variable: temperature_2m
│   ├── Variable: total_precipitation
│   └── Variable: mean_sea_level_pressure
└── Collection: era5-pressure
        (pressure levels, 0.25° grid, with vertical dimension)
    ├── Variable: temperature_500hpa
    │   └── Source: T (vertical_dimension: isobaricInhPa, vertical_value: 500)
    ├── Variable: temperature_850hpa
    │   └── Source: T (vertical_dimension: isobaricInhPa, vertical_value: 850)
    └── Variable: geopotential_500hpa
        └── Source: Z (vertical_dimension: isobaricInhPa, vertical_value: 500)
```

**Example: Sentinel-2 (different processing levels)**

Sentinel-2 imagery comes in processing levels. Level-1C is top-of-atmosphere reflectance; Level-2A is surface
reflectance with atmospheric correction. Different products, different files.

```
Catalog: Sentinel-2
├── Collection: sentinel2-l1c
│   │   (top of atmosphere)
│   ├── Variable: B04_red
│   └── Variable: B08_nir
└── Collection: sentinel2-l2a
        (surface reflectance, atmospherically corrected)
    ├── Variable: ndvi
    │   ├── Source: B04 (role: red)
    │   └── Source: B08 (role: nir)
    │   (transform: band_math → (nir - red) / (nir + red))
    └── Variable: evi
        ├── Source: B02 (role: blue)
        ├── Source: B04 (role: red)
        └── Source: B08 (role: nir)
        (transform: band_math → 2.5 * (nir - red) / (nir + 6*red - 7.5*blue + 1))
```

---

## Decision Flowchart

When setting up a new data source, work through these questions:

**1. Does all the data come from one file (or one file per timestep)?**

If yes → one Collection. If no → consider multiple Collections.

**2. Do all parameters share the same spatial grid?**

Same resolution, same extent, same CRS → same Collection. Different grids → separate Collections.

**3. Do all parameters share the same temporal resolution?**

Hourly and daily from the same source → separate Collections. All hourly → same Collection.

**4. Are the files always produced together?**

If parameter A always arrives with parameter B in the same file → same Collection. If they arrive independently →
separate Collections.

**5. Would a plugin save them to the same path?**

If a plugin downloads one file and saves it once → one Collection. If it downloads separate files and saves them to
different paths → separate Collections.

```
Does all data come from one file?
    ├── Yes → Same spatial grid?
    │           ├── Yes → Same temporal resolution?
    │           │           ├── Yes → ONE COLLECTION ✓
    │           │           └── No  → MULTIPLE COLLECTIONS
    │           └── No  → MULTIPLE COLLECTIONS
    └── No  → Are they related enough to share a Catalog?
                ├── Yes → MULTIPLE COLLECTIONS under one Catalog
                └── No  → SEPARATE CATALOGS entirely
```

---

## Common Patterns

### Single-parameter product

One file, one variable. The simplest case.

```
Catalog: CHIRPS
└── Collection: chirps-daily
    └── Variable: precipitation
```

Other examples: TAMSAT rainfall, CPC temperature, GPM IMERG.

### Multi-parameter product (single file)

One file containing many parameters on the same grid. The most common pattern for NWP data.

```
Catalog: ICON-EU
└── Collection: icon-eu
    ├── Variable: temperature
    ├── Variable: precipitation
    ├── Variable: cloud_cover
    ├── Variable: wind_speed
    └── Variable: wind_direction
```

Other examples: ECMWF IFS, ECMWF AIFS, GFS (within a single resolution tier), COSMO, HARMONIE.

### Multi-resolution product

Same parameters available at different resolutions or forecast ranges, delivered as separate files.

```
Catalog: ECMWF IFS
├── Collection: ifs-hres (0.1° grid, deterministic)
│   ├── Variable: temperature
│   └── Variable: precipitation
└── Collection: ifs-ens (0.2° grid, ensemble mean)
    ├── Variable: temperature
    └── Variable: precipitation
```

### Derived product with levels

Parameters extracted at specific vertical levels from a 3D dataset.

```
Catalog: ERA5
└── Collection: era5-pressure
    ├── Variable: temperature_500hpa
    │   └── Source: T (vertical_value: 500)
    ├── Variable: temperature_850hpa
    │   └── Source: T (vertical_value: 850)
    ├── Variable: wind_speed_250hpa
    │   ├── Source: U (vertical_value: 250, role: u_component)
    │   └── Source: V (vertical_value: 250, role: v_component)
    │   (transform: vector_magnitude)
    └── Variable: geopotential_500hpa
        └── Source: Z (vertical_value: 500)
```

Each Variable specifies which pressure level to extract via `VariableSource.vertical_value`. The pipeline reads the 3D
file once and extracts the correct slice for each Variable.

### Satellite imagery with indices

Raw bands and computed indices from the same satellite product.

```
Catalog: MODIS
└── Collection: modis-terra
    ├── Variable: ndvi
    │   ├── Source: sur_refl_b01 (role: red)
    │   └── Source: sur_refl_b02 (role: nir)
    │   (transform: band_math → (nir - red) / (nir + red))
    ├── Variable: lst_day
    │   └── Source: LST_Day_1km (passthrough, scale + offset)
    └── Variable: lst_night
        └── Source: LST_Night_1km (passthrough, scale + offset)
```

---

## How Variables Work

### Simple variable (passthrough)

One source parameter, read directly. Optionally with unit conversion.

```python
Variable:
slug: temperature
transform_type: passthrough
source_unit: K  # units in the source file (Unit FK)
unit: °C  # output units (Unit FK)
value_min: -40
value_max: 50

sources:  # StreamField
- primary:
source_name: TMP_2maboveground
```

The pipeline reads `TMP_2maboveground` from the file, converts Kelvin to Celsius (derived from `source_unit` → `unit`),
and encodes it.

### Derived variable (vector magnitude)

Two source parameters combined with a mathematical operation.

```python
Variable:
slug: wind_speed
transform_type: vector_magnitude
source_unit: m / s
unit: km / h
value_min: 0
value_max: 150

sources:  # StreamField
- u_component:
source_name: UGRD_10maboveground
- v_component:
source_name: VGRD_10maboveground
```

The pipeline reads both U and V components, computes √(u² + v²), converts m/s to km/h, and encodes.

### Derived variable (band math) — *planned, not yet implemented*

> **Not available today.** There is no `band_math` transform or `transform_expression` field in the current model
> (`transform_type` is limited to `passthrough`, `vector_magnitude`, `vector_direction`). The block below shows the
> *intended* shape of arbitrary band arithmetic for planning purposes only.

Multiple source parameters combined with a custom expression.

```python
# PLANNED — does not work yet
Variable:
slug: ndvi
transform_type: band_math
transform_expression: (nir - red) / (nir + red)
value_min: -1.0
value_max: 1.0

sources:
- red: {source_name: B04}
- nir: {source_name: B08}
```

In the planned design the pipeline would read both bands and evaluate the expression using the role names as variable
identifiers. (Note that today only the `primary`/`u_component`/`v_component` source kinds exist, so generic role names
like `red`/`nir` are part of the proposed extension, not the current StreamField.)

### Pressure level extraction

Same source parameter extracted at a specific vertical level.

```python
Variable:
slug: temperature_850hpa
transform_type: passthrough
source_unit: K
unit: °C
value_min: -60
value_max: 40

sources:
- primary:
source_name: T
vertical_dimension: isobaricInhPa
vertical_value: 850
```

The pipeline reads parameter `T` at the 850 hPa level. The `vertical_dimension` and `vertical_value` fields on the
Source block tell the format plugin exactly which slice to extract.

---

## Relationship to Ingestion

How you structure your data model directly affects how files are processed:

**Plugin saves a file:**

```
georiva-sources/ecmwf-aifs/ecmwf-aifs/GR--20250115T0600--aifs.grib2
                ↑ catalog   ↑ collection
```

Or without a collection in the path (the pipeline processes all collections under the catalog):

```
georiva-sources/ecmwf-aifs/GR--20250115T0600--aifs.grib2
                ↑ catalog
```

**Pipeline processes the file:**

```
1. Resolve catalog: ecmwf-aifs
2. Resolve collection(s):
   - Path has collection → [ecmwf-aifs]
   - Path has no collection → all active collections under ecmwf-aifs
3. For each collection:
   4. Load active variables: temperature, precipitation, wind_speed, wind_direction
   5. Extract timestamps from file
   6. For each timestamp:
      7. For each variable:
         8. Read source(s) from file using VariableSource config
         9. Apply transform (passthrough, vector_magnitude, or vector_direction)
         10. Apply unit conversion
         11. Clip to boundary
         12. Encode PNG + COG + JSON
         13. Save to georiva-assets
```

With one Collection containing all variables, steps 3–13 happen in a single pass over the file. With multiple
Collections for the same file, the file gets re-read for each Collection.

---

## Summary

The guiding principle is to let the source data structure drive your model organization. If the data arrives as one
file, it should map to one Collection. Variables are the unit of extraction and visualization — they define what to pull
from the file and how to display it. Collections group variables that travel together. Catalogs group collections that
come from the same provider or product.

When in doubt, start with one Collection per Catalog and add more only when the data genuinely arrives in separate files
with different grids or temporal resolutions.
