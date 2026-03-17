from django.conf import settings


def georiva_settings(request):
    endpoint = settings.MINIO_PUBLIC_ENDPOINT
    protocol = "https" if settings.MINIO_PUBLIC_ENDPOINT_USE_SSL else "http"
    return {
        "MINIO_ASSETS_BASE_URL": f"{protocol}://{endpoint}/georiva-assets",
    }
