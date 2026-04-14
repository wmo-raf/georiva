from django.db import migrations

PREDEFINED_UNITS = [
    # Temperature
    {
        "name": "Kelvin",
        "symbol": "K",
        "description": "SI unit of thermodynamic temperature. Native unit in NWP model output (GFS, ECMWF).",
    },
    {
        "name": "Degree Celsius",
        "symbol": "°C",
        "description": "Celsius temperature scale. Common display unit for surface temperature.",
    },
    
    # Pressure
    {
        "name": "Pascal",
        "symbol": "Pa",
        "description": "SI unit of pressure. Native unit in NWP model output.",
    },
    {
        "name": "Hectopascal",
        "symbol": "hPa",
        "description": "Standard meteorological pressure unit. 1 hPa = 1 mbar.",
    },
    
    # Length / Height
    {
        "name": "Metre",
        "symbol": "m",
        "description": "SI unit of length. Used for geopotential height, wave height, visibility.",
    },
    {
        "name": "Kilometre",
        "symbol": "km",
        "description": "Used for ceiling height and visibility in aviation.",
    },
    {
        "name": "Millimetre",
        "symbol": "mm",
        "description": "Standard unit for precipitation accumulation.",
    },
    
    # Wind / Speed
    {
        "name": "Metre per second",
        "symbol": "m/s",
        "description": "SI unit of speed. Native unit for wind components in NWP output.",
    },
    {
        "name": "Kilometre per hour",
        "symbol": "km/h",
        "description": "Common display unit for wind speed in public forecasts.",
    },
    
    # Precipitation flux
    {
        "name": "Kilogram per square metre per second",
        "symbol": "kg/m²/s",
        "description": "Native precipitation rate unit in NWP models. Convert to mm/h by multiplying by 3600.",
    },
    
    # Radiation / Energy
    {
        "name": "Watt per square metre",
        "symbol": "W/m²",
        "description": "Irradiance unit. Used for solar radiation, longwave radiation, net radiation.",
    },
    {
        "name": "Joule per kilogram",
        "symbol": "J/kg",
        "description": "Specific energy unit. Used for CAPE, CIN, geopotential.",
    },
    
    # Kinematic
    {
        "name": "Square metre per square second",
        "symbol": "m²/s²",
        "description": "Used for geopotential (divide by g=9.80665 to get metres).",
    },
    
    # Angle
    {
        "name": "Degree",
        "symbol": "°",
        "description": "Angular unit. Used for wind direction (0–360° meteorological convention).",
    },
    
    # Fraction / Ratio
    {
        "name": "Percent",
        "symbol": "%",
        "description": "Used for relative humidity, cloud cover, soil moisture fraction.",
    },
    {
        "name": "Dimensionless",
        "symbol": "dimensionless",
        "description": "Unitless quantity. Used for indices (NDVI, SPI, anomaly scores etc).",
    },
]


def create_units(apps, schema_editor):
    Unit = apps.get_model("georivacore", "Unit")
    for unit_data in PREDEFINED_UNITS:
        Unit.objects.get_or_create(
            symbol=unit_data["symbol"],
            defaults={
                "name": unit_data["name"],
                "description": unit_data["description"],
            },
        )


def delete_units(apps, schema_editor):
    Unit = apps.get_model("georivacore", "Unit")
    symbols = [u["symbol"] for u in PREDEFINED_UNITS]
    Unit.objects.filter(symbol__in=symbols).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("georivacore", "0003_add_default_topics"),
    ]
    
    operations = [
        migrations.RunPython(create_units, delete_units),
    ]
