from rest_framework.renderers import JSONRenderer


class GeoJSONRenderer(JSONRenderer):
    media_type = "application/geo+json"
    format = "geojson"


class STACJSONRenderer(JSONRenderer):
    media_type = "application/json"
    format = "json"
