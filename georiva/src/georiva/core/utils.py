from urllib.parse import urlsplit

from django.utils.encoding import force_str
from wagtail.models import Site


def get_full_url_by_request(request, path):
    site = Site.find_for_request(request)
    base_url = site.root_url
    
    # We only want the scheme and netloc
    base_url_parsed = urlsplit(force_str(base_url))
    
    base_url = base_url_parsed.scheme + "://" + base_url_parsed.netloc
    
    return base_url + path


def get_base_stac_api_url(request=None):
    path = "/api/stac/"
    
    if request:
        return get_full_url_by_request(request, path)
    
    return path
