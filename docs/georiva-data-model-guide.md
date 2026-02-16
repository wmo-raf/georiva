# GeoRiva Data Model Guide

> How to organize your data into Catalogs, Collections, and Variables.

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

**VariableSource** links a Variable to its raw parameter in the source file. Simple variables have one source; derived
variables (like wind speed from U and V components) have multiple sources combined with an expression.

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
unit_conversion: K_to_C
value_min: -40
value_max: 50
units: °C

VariableSource:
source_name: TMP_2maboveground
role: primary
```

The pipeline reads `TMP_2maboveground` from the file, converts Kelvin to Celsius, and encodes it.

### Derived variable (vector magnitude)

Two source parameters combined with a mathematical operation.

```python
Variable:
slug: wind_speed
transform_type: vector_magnitude
unit_conversion: ms_to_kmh
value_min: 0
value_max: 150
units: km / h

VariableSource:
source_name: UGRD_10maboveground
role: u_component

VariableSource:
source_name: VGRD_10maboveground
role: v_component
```

The pipeline reads both U and V components, computes √(u² + v²), converts m/s to km/h, and encodes.

### Derived variable (band math)

Multiple source parameters combined with a custom expression.

```python
Variable:
slug: ndvi
transform_type: band_math
transform_expression: (nir - red) / (nir + red)
value_min: -1.0
value_max: 1.0
units: ""

VariableSource:
source_name: B04
role: red

VariableSource:
source_name: B08
role: nir
```

The pipeline reads both bands, evaluates the expression using the role names as variable identifiers, and encodes the
result.

### Pressure level extraction

Same source parameter extracted at a specific vertical level.

```python
Variable:
slug: temperature_850hpa
transform_type: passthrough
unit_conversion: K_to_C
value_min: -60
value_max: 40
units: °C

VariableSource:
source_name: T
role: primary
vertical_dimension: isobaricInhPa
vertical_value: 850
```

The pipeline reads parameter `T` at the 850 hPa level. The `vertical_dimension` and `vertical_value` fields on
VariableSource tell the format plugin exactly which slice to extract.

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
         9. Apply transform (passthrough, vector_magnitude, band_math, etc.)
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
