"""
Request and response serializers for the timeseries API.
"""

from __future__ import annotations

from rest_framework import serializers


# ---------------------------------------------------------------------------
# Shared fields
# ---------------------------------------------------------------------------

class VariablePathField(serializers.CharField):
    """
    Parses and validates ``catalog_slug/collection_slug/variable_slug``.

    Returns a resolved ``Variable`` instance via ``to_internal_value``.
    """
    
    def to_internal_value(self, data: str):
        value = super().to_internal_value(data)
        parts = value.strip("/").split("/")
        if len(parts) != 3:
            raise serializers.ValidationError(
                "Must be catalog_slug/collection_slug/variable_slug, "
                f"e.g. chirps/chirps-monthly/precipitation. Got: {value!r}"
            )
        catalog_slug, collection_slug, variable_slug = parts
        
        from georiva.core.models import Variable
        
        try:
            return Variable.objects.select_related(
                "collection",
                "collection__catalog",
                "unit",
            ).get(
                slug=variable_slug,
                collection__slug=collection_slug,
                collection__catalog__slug=catalog_slug,
                is_active=True,
            )
        except Variable.DoesNotExist:
            raise serializers.ValidationError(
                f"Variable not found or inactive: {value!r}"
            )


# ---------------------------------------------------------------------------
# Request serializers
# ---------------------------------------------------------------------------

class PointRequestSerializer(serializers.Serializer):
    variable = VariablePathField(
        help_text="catalog_slug/collection_slug/variable_slug"
    )
    lat = serializers.FloatField(min_value=-90.0, max_value=90.0)
    lon = serializers.FloatField(min_value=-180.0, max_value=180.0)
    time_start = serializers.DateTimeField(required=False, default=None)
    time_end = serializers.DateTimeField(required=False, default=None)
    
    def validate(self, attrs):
        start = attrs.get("time_start")
        end = attrs.get("time_end")
        if start and end and start >= end:
            raise serializers.ValidationError(
                "time_start must be before time_end."
            )
        return attrs


class AreaRequestSerializer(serializers.Serializer):
    AGGREGATIONS = ["mean", "sum", "min", "max", "std"]
    
    variable = VariablePathField(
        help_text="catalog_slug/collection_slug/variable_slug"
    )
    geometry = serializers.JSONField(
        help_text="GeoJSON geometry (Polygon or MultiPolygon)"
    )
    aggregation = serializers.ChoiceField(
        choices=AGGREGATIONS,
        default="mean",
    )
    time_start = serializers.DateTimeField(required=False, default=None)
    time_end = serializers.DateTimeField(required=False, default=None)
    
    def validate_geometry(self, value):
        """Check geometry is a valid GeoJSON Polygon or MultiPolygon."""
        if not isinstance(value, dict):
            raise serializers.ValidationError("Must be a GeoJSON geometry object.")
        
        geom_type = value.get("type")
        if geom_type not in ("Polygon", "MultiPolygon"):
            raise serializers.ValidationError(
                f"geometry.type must be Polygon or MultiPolygon, got {geom_type!r}."
            )
        if "coordinates" not in value:
            raise serializers.ValidationError(
                "geometry must have a 'coordinates' key."
            )
        
        # Validate with shapely so we catch self-intersections etc.
        try:
            from shapely.geometry import shape
            from shapely.validation import explain_validity
            
            geom = shape(value)
            if not geom.is_valid:
                raise serializers.ValidationError(
                    f"Invalid geometry: {explain_validity(geom)}"
                )
        except ImportError:
            pass  # shapely not installed — skip deep validation
        
        return value
    
    def validate(self, attrs):
        start = attrs.get("time_start")
        end = attrs.get("time_end")
        if start and end and start >= end:
            raise serializers.ValidationError(
                "time_start must be before time_end."
            )
        return attrs


# ---------------------------------------------------------------------------
# Response serializers
# ---------------------------------------------------------------------------

class TimeseriesValueSerializer(serializers.Serializer):
    time = serializers.DateTimeField()
    value = serializers.FloatField(allow_null=True)


class PointResponseSerializer(serializers.Serializer):
    variable = serializers.CharField()
    units = serializers.CharField()
    lat = serializers.FloatField()
    lon = serializers.FloatField()
    time_start = serializers.DateTimeField(allow_null=True)
    time_end = serializers.DateTimeField(allow_null=True)
    count = serializers.IntegerField()
    data = TimeseriesValueSerializer(many=True)


class AreaResponseSerializer(serializers.Serializer):
    variable = serializers.CharField()
    units = serializers.CharField()
    aggregation = serializers.CharField()
    time_start = serializers.DateTimeField(allow_null=True)
    time_end = serializers.DateTimeField(allow_null=True)
    count = serializers.IntegerField()
    data = TimeseriesValueSerializer(many=True)
