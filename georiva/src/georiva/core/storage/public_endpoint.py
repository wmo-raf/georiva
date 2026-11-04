"""Where a browser's upload goes, and whether it can get there.

A person's upload is a browser ``PUT`` to a presigned URL on
``MINIO_PUBLIC_ENDPOINT`` (``Bucket.get_upload_url``). If that name is wrong the
bytes are sent anyway, to whatever answers it, and the progress bar still reaches
100%: the failure shows up later, as a missing object. These functions say what
can be known about the setting without making a request.

Two things can be known for certain, and one only by convention:

- **Same host as the site** — the endpoint *is* the admin site. It has no route for
  a bucket path, so a ``PUT`` there is answered by the app, never by MinIO.
- **HTTPS page, HTTP endpoint** — a browser refuses a mixed-content request
  outright. The file never leaves it.
- **No ``s3.`` first label** — advisory only. The bundled nginx recognises the
  sender endpoint by ``server_name s3.*``, but an operator may pin another name,
  so this one is worth a warning and never a refusal.

Pure: no Django request, no settings, no network, so the page and the system check
share one rule and tests need nothing running.
"""

from dataclasses import dataclass

#: Why an upload to this endpoint can never work from this page.
SAME_HOST = "same_host"
MIXED_CONTENT = "mixed_content"

#: The endpoint is not named the way the bundled proxy routes the S3 API.
NO_S3_PREFIX = "no_s3_prefix"

_DEFAULT_PORTS = {"http": "80", "https": "443"}

#: Development hosts. MinIO is reached on its own port there, with no ``s3.`` name
#: and no proxy in front, so the naming convention does not apply.
_LOCAL_HOSTS = ("localhost", "127.0.0.1", "[::1]", "::1", "0.0.0.0")


@dataclass(frozen=True)
class UploadEndpoint:
    """What the settings say about the upload endpoint, from one point of view."""

    scheme: str
    host: str
    #: Set when an upload from this page can never work. One of the constants above.
    blocked: str | None = None
    #: Set when the name does not look like the S3 endpoint. Advisory.
    advisory: str | None = None

    @property
    def url(self) -> str:
        return f"{self.scheme}://{self.host}"


def _split(netloc: str) -> tuple[str, str]:
    """``(hostname, port)`` of ``host[:port]``, lower-cased; port ``""`` when absent."""
    netloc = netloc.strip().lower()
    if netloc.startswith("["):  # [::1]:9000
        host, _, rest = netloc.partition("]")
        return host + "]", rest.lstrip(":")
    host, _, port = netloc.partition(":")
    return host, port


def _is_local(hostname: str) -> bool:
    return hostname in _LOCAL_HOSTS or hostname.endswith(".localhost")


def _same_place(endpoint: str, endpoint_scheme: str, site_host: str, site_scheme: str) -> bool:
    """Whether the two name one host and port.

    A port left out means the scheme's default, except when *neither* names one:
    ``kmsa.example.org`` as the endpoint of a site at ``https://kmsa.example.org``
    is the same host whichever scheme the endpoint was set up with, and the scheme
    is a separate fault.
    """
    ep_host, ep_port = _split(endpoint)
    site_name, site_port = _split(site_host)
    if ep_host != site_name:
        return False
    if not ep_port and not site_port:
        return True
    return (ep_port or _DEFAULT_PORTS[endpoint_scheme]) == (site_port or _DEFAULT_PORTS[site_scheme])


def has_s3_shape(endpoint: str) -> bool:
    """Whether the endpoint is a development host or begins with an ``s3.`` label."""
    hostname, _ = _split(endpoint)
    return _is_local(hostname) or hostname.startswith("s3.")


def describe(
    endpoint: str,
    use_ssl: bool,
    site_host: str | None = None,
    site_is_secure: bool | None = None,
) -> UploadEndpoint:
    """Describe ``endpoint``, and judge it against the page it is used from.

    ``site_host`` and ``site_is_secure`` are the host (with port) and scheme of the
    page the browser is on. Leave them out, as the startup check does, and only the
    naming advisory can apply.
    """
    scheme = "https" if use_ssl else "http"
    blocked = None

    if site_host is not None and _same_place(endpoint, scheme, site_host, "https" if site_is_secure else "http"):
        blocked = SAME_HOST
    elif site_is_secure and not use_ssl:
        blocked = MIXED_CONTENT

    advisory = None if has_s3_shape(endpoint) else NO_S3_PREFIX
    return UploadEndpoint(scheme=scheme, host=endpoint, blocked=blocked, advisory=advisory)
