from django.db import migrations

DEFAULT_TOPICS = [
    # Atmospheric
    {"name": "Temperature", "icon": "bi-thermometer-half", "sort_order": 10},
    {"name": "Precipitation", "icon": "bi-cloud-rain", "sort_order": 20},
    {"name": "Wind", "icon": "bi-wind", "sort_order": 30},
    {"name": "Humidity & Moisture", "icon": "bi-droplet-half", "sort_order": 40},
    {"name": "Atmospheric Pressure", "icon": "bi-speedometer", "sort_order": 50},
    {"name": "Radiation & Solar", "icon": "bi-sun", "sort_order": 60},
    {"name": "Cloud & Convection", "icon": "bi-cloud", "sort_order": 70},
    # Land Surface
    {"name": "Vegetation", "icon": "bi-tree", "sort_order": 80},
    {"name": "Soil Moisture", "icon": "bi-layers", "sort_order": 90},
    {"name": "Land Cover", "icon": "bi-map", "sort_order": 100},
    {"name": "Elevation & Terrain", "icon": "bi-triangle", "sort_order": 110},
    {"name": "Fire Weather", "icon": "bi-fire", "sort_order": 120},
    # Water
    {"name": "Hydrology & Streamflow", "icon": "bi-water", "sort_order": 130},
    {"name": "Sea Surface Temperature", "icon": "bi-tsunami", "sort_order": 140},
    {"name": "Ocean & Currents", "icon": "bi-globe-americas", "sort_order": 150},
    # Derived & Indices
    {"name": "Drought Indices", "icon": "bi-exclamation-triangle", "sort_order": 160},
    {"name": "Air Quality", "icon": "bi-lungs", "sort_order": 170},
    {"name": "Extreme Weather", "icon": "bi-lightning-charge", "sort_order": 180},
    {"name": "Climate & Reanalysis", "icon": "bi-clock-history", "sort_order": 190},
]


def add_default_topics(apps, schema_editor):
    Topic = apps.get_model('georivacore', 'Topic')
    for topic in DEFAULT_TOPICS:
        Topic.objects.get_or_create(
            name=topic['name'],
            defaults={
                'icon': topic['icon'],
                'sort_order': topic['sort_order'],
            }
        )


def remove_default_topics(apps, schema_editor):
    Topic = apps.get_model('georivacore', 'Topic')
    names = [t['name'] for t in DEFAULT_TOPICS]
    Topic.objects.filter(name__in=names).delete()


class Migration(migrations.Migration):
    dependencies = [
        ('georivacore', '0002_initial'),
    ]
    
    operations = [
        migrations.RunPython(
            add_default_topics,
            reverse_code=remove_default_topics,
        ),
    ]
