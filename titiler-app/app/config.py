import os
import re

MINIO_HOST = os.getenv("MINIO_HOST", "http://georiva-minio:9000")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "georiva-assets")
REDIS_URL = os.getenv("REDIS_URL", "redis://georiva-redis:6379/0")
DJANGO_BASE_URL = os.getenv("DJANGO_BASE_URL", "http://georiva:8000")
TTL_ROOT_PATH = os.getenv("TTL_ROOT_PATH", "/titiler")

PALETTE_KEY_PREFIX = "georiva:palette"
PATH_RE = re.compile(r"^[\w/.-]+\.tif$")
