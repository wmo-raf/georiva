from rest_framework.renderers import JSONRenderer


class GeoJSONRenderer(JSONRenderer):
    """Renderer for application/geo+json content type."""
    media_type = 'application/geo+json'
    format = 'geojson'


class STACJSONRenderer(JSONRenderer):
    """Renderer for application/json with STAC compatibility."""
    media_type = 'application/json'
    format = 'json'
