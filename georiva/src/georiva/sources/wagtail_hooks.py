from django.urls import path, reverse
from django.utils.translation import gettext_lazy as _
from wagtail import hooks
from wagtail.admin.menu import MenuItem
from wagtail.admin.viewsets.model import ModelViewSet

from .models import LoaderProfile
from .registry import loader_profile_viewset_registry
from .utils import get_all_child_models
from .views import (
    loader_profile_list,
    loader_profile_add_select
)
from .viewsets import (
    admin_viewsets,
    LoaderProfileCreateView,
    LoaderProfileEditView,
    LoaderProfileDeleteView
)


@hooks.register('register_admin_urls')
def urlconf_georivasources():
    return [
        path('loader-profiles/', loader_profile_list, name="loader_profile_list"),
        path('loader-profiles/select/', loader_profile_add_select, name="loader_profile_add_select"),
    ]


@hooks.register('register_admin_menu_item')
def register_catalogs_menu():
    list_url = reverse('loader_profile_list')
    label = _("Loader Profiles")
    return MenuItem(label, list_url, icon_name='file-import', order=800)


def get_loader_profile_viewsets():
    loader_profile_model_cls = get_all_child_models(LoaderProfile)
    
    loader_profile_viewsets = []
    
    for model_cls in loader_profile_model_cls:
        model_name = model_cls._meta.model_name
        
        attrs = {
            "model": model_cls,
            "type": model_name,
            "add_view_class": LoaderProfileCreateView,
            "edit_view_class": LoaderProfileEditView,
            "delete_view_class": LoaderProfileDeleteView,
        }
        
        viewset = type(
            f"{model_cls.__name__}ViewSet",
            (ModelViewSet,),
            attrs
        )
        
        viewset_cls = viewset()
        
        loader_profile_viewsets.append(viewset_cls)
        loader_profile_viewset_registry.register(viewset_cls)
    
    return loader_profile_viewsets


@hooks.register("register_admin_viewset")
def register_viewset():
    loader_profile_viewsets = get_loader_profile_viewsets()
    return admin_viewsets + loader_profile_viewsets
