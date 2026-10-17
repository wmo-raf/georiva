from urllib.parse import urlsplit

from django.utils.encoding import force_str
from wagtail.models import Site


def get_full_url_by_request(request, path):
    # Already an absolute URL — return as-is (mirrors build_absolute_uri,
    # which is idempotent for absolute inputs). Prevents double-prefixing
    # things like S3/MinIO asset URLs that are already fully qualified.
    if path and urlsplit(force_str(path)).scheme:
        return path

    site = Site.find_for_request(request)
    if site is None:
        # No Wagtail Site matches the request host; fall back to the request.
        return request.build_absolute_uri(path)

    base_url = site.root_url

    # We only want the scheme and netloc
    base_url_parsed = urlsplit(force_str(base_url))

    base_url = base_url_parsed.scheme + "://" + base_url_parsed.netloc

    return base_url + path


def iso_utc_z(dt) -> str:
    """ISO 8601 UTC with a ``Z`` suffix — the one spelling of a timestamp in
    API URLs, storage-facing addresses and WMTS dimension values. ``Item``'s
    ``time_iso`` properties and the WMTS capabilities document both write
    through this, so the spelling cannot drift between them."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def get_base_stac_api_url(request=None):
    path = "/api/stac/"

    if request:
        return get_full_url_by_request(request, path)

    return path
