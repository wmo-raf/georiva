from django.urls import reverse
from wagtail.admin.views import generic
from wagtail.admin.viewsets.chooser import ChooserViewSet

from .models import LoaderProfile


class LoaderProfileSuccesUrlMixin:
    def get_success_url(self):
        return reverse("loader_profile_list")


class LoaderProfileCreateView(LoaderProfileSuccesUrlMixin, generic.CreateView):
    pass


class LoaderProfileEditView(LoaderProfileSuccesUrlMixin, generic.EditView, ):
    pass


class LoaderProfileDeleteView(LoaderProfileSuccesUrlMixin, generic.DeleteView):
    pass


class LoaderProfileChooserViewSet(ChooserViewSet):
    model = LoaderProfile
    
    icon = "file-import"
    choose_one_text = "Choose a Loader Profile"
    choose_another_text = "Choose another Loader Profile"
    edit_item_text = "Edit this Loader Profile"


admin_viewsets = [
    LoaderProfileChooserViewSet("loader_profile_chooser"),
]
