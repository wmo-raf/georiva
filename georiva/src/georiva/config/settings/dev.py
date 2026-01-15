from .base import *

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = "django-insecure-(8@3v=7vqhetw$!4ml24=n-=od$2zyd8=+u&ln&=4n6@^j7eu0"

# SECURITY WARNING: define the correct hosts in production!
ALLOWED_HOSTS = ["*"]

EMAIL_BACKEND = "django.core.mail.backends.console.EmailBackend"

GDAL_LIBRARY_PATH = env.str('GDAL_LIBRARY_PATH', None)
GEOS_LIBRARY_PATH = env.str('GEOS_LIBRARY_PATH', None)

CORS_ALLOW_ALL_ORIGINS = True

INSTALLED_APPS = INSTALLED_APPS + [
    "wagtail.contrib.styleguide",
]

try:
    from .local import *
except ImportError:
    pass
