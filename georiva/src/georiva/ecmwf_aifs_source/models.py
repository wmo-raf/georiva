from django_extensions.db.models import TimeStampedModel
from wagtail.snippets.models import register_snippet

from georiva.sources.models import LoaderProfile


@register_snippet
class ECMWFAIFSLoaderProfile(LoaderProfile, TimeStampedModel):
    """
    Loader profile for ECMWF AIFS data source.
    """
    
    panels = [
        *LoaderProfile.base_panels,
    ]
    
    class Meta:
        verbose_name = "ECMWF AIFS Loader Profile"
        verbose_name_plural = "ECMWF AIFS Loader Profiles"
