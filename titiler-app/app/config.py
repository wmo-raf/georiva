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
#: Where Django says the stores it publishes have changed (#706): each worker
#: listens, and reads the hashes again. Spelled to match ``STORES_CHANNEL`` in
#: ``georiva/core/machine_plane/remote_assets.py``.
STORES_CHANNEL = "georiva:stores-published"
PATH_RE = re.compile(r"^[\w/.-]+\.tif$")

#: Seconds a read from an Upstream store may take in all — opening it, or
#: reading one field — and each request it makes on the way (ADR 0044 §9). A
#: store that does not answer in time is an error for that tile and nothing else.
STORE_READ_TIMEOUT = float(os.getenv("GEORIVA_STORE_READ_TIMEOUT", "30"))
#: Seconds a store's host, or the place its data lives, may take to accept a connection.
STORE_CONNECT_TIMEOUT = float(os.getenv("GEORIVA_STORE_CONNECT_TIMEOUT", "5"))
#: Seconds a step a reopened store still does not hold is remembered, so that
#: requests for it fail without reopening the store again (#677). A step that
#: arrives meanwhile is drawn at the latest once this has passed.
STORE_MISS_TTL = float(os.getenv("GEORIVA_STORE_MISS_TTL", "60"))
#: Bytes of decoded fields each worker keeps from Upstream stores (ADR 0044 §13;
#: #663). Every tile of a store's map needs the same whole field — about 8 MB for
#: a global 0.25° grid — so a map view reads it upstream once and draws the rest
#: from here. Bounded in bytes, not fields, because a field's size is its grid's:
#: a count would bound memory only for the grid it was chosen for. 0 keeps none.
#: Set in megabytes (``GEORIVA_STORE_FIELD_CACHE_MB``), held here in bytes.
STORE_FIELD_CACHE_BYTES = int(float(os.getenv("GEORIVA_STORE_FIELD_CACHE_MB", "128")) * 2**20)
#: Whether each worker opens the stores of published Store assets as it starts,
#: and each one published while it runs, in the background, so a user's first
#: map of one does not wait 27 s for it (#684, #706). Costs each worker an open
#: store's memory whether or not it is asked for one. ``0`` leaves every store to
#: be opened by its first tile, and kept until the worker restarts.
STORE_WARM = os.getenv("GEORIVA_STORE_WARM", "1").strip().lower() not in ("0", "false", "no", "off", "")
