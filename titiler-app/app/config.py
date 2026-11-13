import os
import re

#: Opening one COG otherwise makes GDAL list the whole directory it sits in,
#: hunting for sidecars ours never have (.ovr, .msk, .aux.xml — a COG carries
#: its overviews inside itself). That listing costs a request per open and,
#: worse, is cached: a sibling written afterwards is absent from it and so
#: unreadable until the process restarts, which is half of #400. Pinned rather
#: than merely defaulted in compose because the reader's correctness depends on
#: it; ``app.reader`` imports this name to say so, and an operator can still
#: override it from the environment.
GDAL_DISABLE_READDIR_ON_OPEN = os.environ.setdefault("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")

MINIO_HOST = os.getenv("MINIO_HOST", "http://georiva-minio:9000")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "georiva-assets")
REDIS_URL = os.getenv("REDIS_URL", "redis://georiva-redis:6379/0")
DJANGO_BASE_URL = os.getenv("DJANGO_BASE_URL", "http://georiva:8000")
TTL_ROOT_PATH = os.getenv("TTL_ROOT_PATH", "/titiler")

PALETTE_KEY_PREFIX = "georiva:palette"

#: Where Django publishes each Variable's Remote assets: one hash per variable,
#: timestep → upstream href, plus a marker field saying the hash is its whole
#: word. Spelled to match ``georiva/core/machine_plane/remote_assets.py``.
REMOTE_ASSETS_KEY_PREFIX = "georiva:remote-assets"
REMOTE_ASSETS_PUBLISHED_FIELD = "published"
PATH_RE = re.compile(r"^[\w/.-]+\.tif$")

#: Seconds a read from an Upstream store may take in all — opening it, or
#: reading one field — and each request it makes on the way (ADR 0044 §9). A
#: store that does not answer in time is an error for that tile and nothing else.
STORE_READ_TIMEOUT = float(os.getenv("GEORIVA_STORE_READ_TIMEOUT", "30"))
#: Seconds a store's host, or the place its data lives, may take to accept a connection.
STORE_CONNECT_TIMEOUT = float(os.getenv("GEORIVA_STORE_CONNECT_TIMEOUT", "5"))
