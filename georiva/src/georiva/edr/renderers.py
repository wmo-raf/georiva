from rest_framework.renderers import JSONRenderer


class EDRJSONRenderer(JSONRenderer):
    media_type = "application/json"
    format = "json"


class CoverageJSONRenderer(JSONRenderer):
    """
    Renderer for EDR data query responses (position, area, etc.)
    application/prs.coverage+json

    """

    media_type = "application/prs.coverage+json"
    format = "coveragejson"
