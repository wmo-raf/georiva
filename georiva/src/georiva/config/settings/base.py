import os

import dj_database_url
import environ

from georiva.version import VERSION

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_DIR = os.path.dirname(PROJECT_DIR)

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/5.0/howto/deployment/checklist/

env = environ.Env(
    # set casting, default value
    DEBUG=(bool, False),
)

dev_env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(BASE_DIR))), ".env")

if os.path.isfile(dev_env_path):
    # reading .env file
    environ.Env.read_env(dev_env_path)

DEBUG = env('DEBUG', False)

INSTALLED_APPS = [
    "wagtail.contrib.forms",
    "wagtail.contrib.redirects",
    "wagtail.contrib.settings",
    "wagtail.embeds",
    "wagtail.sites",
    "wagtail.users",
    "wagtail.snippets",
    "wagtail.documents",
    "wagtail.images",
    "wagtail.search",
    "wagtail.admin",
    "wagtail",
    "modelcluster",
    "taggit",
    "django_filters",
    'django_extensions',
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.gis",
    
    "georiva.home",
    "georiva.core",
    "georiva.formats",
    "georiva.loaders",
    "georiva.ingestion",
    "georiva.analysis",
    "georiva.api",
    "georiva.stac",
    
    'django_cleanup.apps.CleanupConfig',
    'rest_framework',
    "corsheaders",
    "adminboundarymanager",
    "django_countries",
]

MIDDLEWARE = [
    "corsheaders.middleware.CorsMiddleware",
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "wagtail.contrib.redirects.middleware.RedirectMiddleware",
]

ROOT_URLCONF = "georiva.config.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [
            os.path.join(PROJECT_DIR, "templates"),
        ],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "georiva.config.wsgi.application"
ASGI_APPLICATION = "georiva.config.asgi.application"

# Database
# https://docs.djangoproject.com/en/5.2/ref/settings/#databases

DB_CONNECTION_MAX_AGE = env.int("DB_CONNECTION_MAX_AGE", default=0)
DB_CONN_HEALTH_CHECKS = env.bool("DB_CONN_HEALTH_CHECKS", default=False)
DB_DISABLE_SERVER_SIDE_CURSORS = env.bool("DB_DISABLE_SERVER_SIDE_CURSORS", default=False)
DB_SSL_REQUIRE = env.bool("DB_SSL_REQUIRE", default=False)

DATABASES = {
    "default": dj_database_url.config(
        conn_max_age=DB_CONNECTION_MAX_AGE,
        conn_health_checks=DB_CONN_HEALTH_CHECKS,
        disable_server_side_cursors=DB_DISABLE_SERVER_SIDE_CURSORS,
        ssl_require=DB_SSL_REQUIRE,
    )
}

# Password validation
# https://docs.djangoproject.com/en/5.2/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

# Internationalization
# https://docs.djangoproject.com/en/5.2/topics/i18n/

LANGUAGE_CODE = "en-us"

TIME_ZONE = "UTC"

USE_I18N = True

USE_TZ = True

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/5.2/howto/static-files/

STATICFILES_FINDERS = [
    "django.contrib.staticfiles.finders.FileSystemFinder",
    "django.contrib.staticfiles.finders.AppDirectoriesFinder",
]

STATICFILES_DIRS = [
    os.path.join(PROJECT_DIR, "static"),
]

STATIC_ROOT = os.path.join(BASE_DIR, "static")
STATIC_URL = "/static/"

MEDIA_ROOT = os.path.join(BASE_DIR, "media")
MEDIA_URL = "/media/"

# Storage backend selection
GEORIVA_STORAGE_BACKEND = env('GEORIVA_STORAGE_BACKEND', default='local')

# Local storage settings
GEORIVA_STORAGE_ROOT = env('GEORIVA_STORAGE_ROOT', default=os.path.join(BASE_DIR, "georiva_data"))

# S3/MinIO settings
AWS_ACCESS_KEY_ID = env('AWS_ACCESS_KEY_ID', default=None)
AWS_SECRET_ACCESS_KEY = env('AWS_SECRET_ACCESS_KEY', default=None)
AWS_STORAGE_BUCKET_NAME = env('AWS_STORAGE_BUCKET_NAME', default='georiva')
AWS_S3_REGION_NAME = env('AWS_S3_REGION_NAME', default='us-east-1')
AWS_S3_ENDPOINT_URL = env('AWS_S3_ENDPOINT_URL', default=None)  # For MinIO
AWS_S3_CUSTOM_DOMAIN = env('AWS_S3_CUSTOM_DOMAIN', default=None)
AWS_DEFAULT_ACL = env('AWS_DEFAULT_ACL', default=None)
AWS_QUERYSTRING_AUTH = env.bool('AWS_QUERYSTRING_AUTH', default=True)
AWS_S3_FILE_OVERWRITE = env.bool('AWS_S3_FILE_OVERWRITE', default=True)
AWS_S3_SIGNATURE_VERSION = env('AWS_S3_SIGNATURE_VERSION', default='s3v4')
AWS_S3_ADDRESSING_STYLE = env('AWS_S3_ADDRESSING_STYLE', default='path')

MINIO_WEBHOOK_ARN = env('MINIO_WEBHOOK_ARN', default='arn:minio:sqs::primary:webhook')
MINIO_WEBHOOK_BEARER_TOKEN = env('MINIO_WEBHOOK_BEARER_TOKEN', default=None)

# Configure storage backends
if GEORIVA_STORAGE_BACKEND == 's3':
    STORAGES = {
        "default": {
            "BACKEND": "django.core.files.storage.FileSystemStorage",
        },
        "staticfiles": {
            "BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage",
        },
        "georiva": {
            "BACKEND": "storages.backends.s3boto3.S3Boto3Storage",
            "OPTIONS": {
                "bucket_name": AWS_STORAGE_BUCKET_NAME,
                "region_name": AWS_S3_REGION_NAME,
                "endpoint_url": AWS_S3_ENDPOINT_URL,
                "custom_domain": AWS_S3_CUSTOM_DOMAIN,
                "default_acl": AWS_DEFAULT_ACL,
                "querystring_auth": AWS_QUERYSTRING_AUTH,
                "file_overwrite": AWS_S3_FILE_OVERWRITE,
                "signature_version": AWS_S3_SIGNATURE_VERSION,
                "addressing_style": AWS_S3_ADDRESSING_STYLE,
            },
        },
    }
elif GEORIVA_STORAGE_BACKEND == 'gcs':
    GS_BUCKET_NAME = env('GS_BUCKET_NAME', default='georiva')
    GS_PROJECT_ID = env('GS_PROJECT_ID', default=None)
    GS_CREDENTIALS_FILE = env('GS_CREDENTIALS_FILE', default=None)
    
    STORAGES = {
        "default": {
            "BACKEND": "django.core.files.storage.FileSystemStorage",
        },
        "staticfiles": {
            "BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage",
        },
        "georiva": {
            "BACKEND": "storages.backends.gcloud.GoogleCloudStorage",
            "OPTIONS": {
                "bucket_name": GS_BUCKET_NAME,
                "project_id": GS_PROJECT_ID,
            },
        },
    }
else:
    # Local filesystem storage
    STORAGES = {
        "default": {
            "BACKEND": "django.core.files.storage.FileSystemStorage",
        },
        "staticfiles": {
            "BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage",
        },
        "georiva": {
            "BACKEND": "django.core.files.storage.FileSystemStorage",
            "OPTIONS": {
                "location": GEORIVA_STORAGE_ROOT,
                "base_url": "/tiles/",
            },
        },
    }

# Django sets a maximum of 1000 fields per form by default, but particularly complex page models
# can exceed this limit within Wagtail's page editor.
DATA_UPLOAD_MAX_NUMBER_FIELDS = 10_000

# Wagtail settings

WAGTAIL_SITE_NAME = "GeoRiva"

# Search
# https://docs.wagtail.org/en/stable/topics/search/backends.html
WAGTAILSEARCH_BACKENDS = {
    "default": {
        "BACKEND": "wagtail.search.backends.database",
    }
}

# Base URL to use when referring to full URLs within the Wagtail admin backend -
# e.g. in notification emails. Don't include '/admin' or a trailing slash
WAGTAILADMIN_BASE_URL = "http://example.com"

# Allowed file extensions for documents in the document library.
# This can be omitted to allow all files, but note that this may present a security risk
# if untrusted users are allowed to upload files -
# see https://docs.wagtail.org/en/stable/advanced_topics/deploying.html#user-uploaded-files
WAGTAILDOCS_EXTENSIONS = ['csv', 'docx', 'key', 'odt', 'pdf', 'pptx', 'rtf', 'txt', 'xlsx', 'zip']

REDIS_HOST = env.str("REDIS_HOST", "adl_redis")
REDIS_PORT = env.str("REDIS_PORT", "6379")
REDIS_USERNAME = env.str("REDIS_USER", "")
REDIS_PASSWORD = env.str("REDIS_PASSWORD", "")
REDIS_PROTOCOL = env.str("REDIS_PROTOCOL", "redis")
REDIS_URL = env.str(
    "REDIS_URL",
    f"{REDIS_PROTOCOL}://{REDIS_USERNAME}:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/0",
)

CELERY_BROKER_URL = REDIS_URL
CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True
CELERY_BEAT_SCHEDULER = "django_celery_beat.schedulers:DatabaseScheduler"

CELERY_SINGLETON_BACKEND_CLASS = (
    "adl.celery_singleton_backend.RedisBackendForSingleton"
)

CELERY_RESULT_BACKEND = 'django-db'
CELERY_RESULT_EXTENDED = True

CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": REDIS_URL,
        "OPTIONS": {"CLIENT_CLASS": "django_redis.client.DefaultClient"},
        "KEY_PREFIX": "georiva-default-cache",
        "VERSION": VERSION,
    },
}

CELERY_CACHE_BACKEND = "default"

GEORIVA_LOG_LEVEL = env.str("GEORIVA_LOG_LEVEL", "INFO")
GEORIVA_DATABASE_LOG_LEVEL = env.str("GEORIVA_DATABASE_LOG_LEVEL", "ERROR")

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "console": {
            "format": "%(levelname)s %(asctime)s %(name)s.%(funcName)s:%(lineno)s- %("
                      "message)s "
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "console",
        },
    },
    "loggers": {
        "django.db.backends": {
            "handlers": ["console"],
            "level": GEORIVA_DATABASE_LOG_LEVEL,
            "propagate": True,
        },
    },
    "root": {
        "handlers": ["console"],
        "level": GEORIVA_LOG_LEVEL,
    },
}
