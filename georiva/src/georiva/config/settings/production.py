from .base import *

DEBUG = False

# ManifestStaticFilesStorage is recommended in production, to prevent
# outdated JavaScript / CSS assets being served from cache
# (e.g. after a Wagtail upgrade).
# See https://docs.djangoproject.com/en/5.2/ref/contrib/staticfiles/#manifeststaticfilesstorage
STORAGES["staticfiles"]["BACKEND"] = "georiva.config.storage.ManifestStaticFilesStorageNotStrict"

try:
    from .local import *
except ImportError:
    pass

SECRET_KEY = env.str("SECRET_KEY")

MANIFEST_LOADER = {
    "cache": True,
    # recommended True for production, requires a server restart to pick up new values from the manifest.
}

WAGTAIL_ENABLE_UPDATE_CHECK = False

# Every organisation lives on a subdomain of the base domain, so the whole
# subtree is allowed here; the middleware, not ALLOWED_HOSTS, decides which
# hostnames actually resolve to an organisation.
ALLOWED_HOSTS = [
    "georiva",
    GEORIVA_BASE_DOMAIN,
    f".{GEORIVA_BASE_DOMAIN}",
] + env.list("ALLOWED_HOSTS", default=[])

CSRF_TRUSTED_ORIGINS = [
    f"https://{GEORIVA_BASE_DOMAIN}",
    f"https://*.{GEORIVA_BASE_DOMAIN}",
] + env.list("CSRF_TRUSTED_ORIGINS", cast=None, default=[])

CORS_ALLOW_ALL_ORIGINS = False
CORS_ALLOWED_ORIGINS = env.list("CORS_ALLOWED_ORIGINS", cast=None, default=[])
