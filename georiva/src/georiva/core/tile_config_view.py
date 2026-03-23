"""
Internal tile-config endpoint consumed by Titiler on Redis cache miss.

GET /api/tile-config/{catalog_slug}/{collection_slug}/{variable_slug}/

Returns the same payload structure as the Redis palette cache:
  With palette:    {"vmin", "vmax", "scale_type", "colormap": {0-255 entries}}
  Without palette: {"vmin", "vmax", "scale_type"}
"""

from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import status

from georiva.core.models import Variable
from georiva.core.palette_cache import build_variable_payload


class TileConfigView(APIView):
    """Return rendering config for a variable (internal use by Titiler)."""

    permission_classes = []
    authentication_classes = []

    def get(self, request, catalog_slug, collection_slug, variable_slug):
        try:
            variable = (
                Variable.objects
                .select_related('collection__catalog', 'palette')
                .prefetch_related('palette__stops')
                .get(
                    collection__catalog__slug=catalog_slug,
                    collection__slug=collection_slug,
                    slug=variable_slug,
                    is_active=True,
                )
            )
        except Variable.DoesNotExist:
            return Response(status=status.HTTP_404_NOT_FOUND)

        return Response(build_variable_payload(variable))
