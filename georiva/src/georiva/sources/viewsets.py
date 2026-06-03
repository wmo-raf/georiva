from django.urls import reverse
from wagtail.admin.views import generic
from wagtail.admin.viewsets.chooser import ChooserViewSet

from .models import DataFeed


class DataFeedSuccessUrlMixin:
    def get_success_url(self):
        return reverse("data_feed_list")


class DataFeedCreateView(DataFeedSuccessUrlMixin, generic.CreateView):
    pass


class DataFeedEditView(DataFeedSuccessUrlMixin, generic.EditView):
    pass


class DataFeedDeleteView(DataFeedSuccessUrlMixin, generic.DeleteView):
    pass


class DataFeedChooserViewSet(ChooserViewSet):
    model = DataFeed

    icon = "file-import"
    choose_one_text = "Choose a Data Feed"
    choose_another_text = "Choose another Data Feed"
    edit_item_text = "Edit this Data Feed"


admin_viewsets = [
    DataFeedChooserViewSet("data_feed_chooser"),
]
