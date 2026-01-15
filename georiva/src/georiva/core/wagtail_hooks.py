from adminboundarymanager.wagtail_hooks import AdminBoundaryViewSetGroup
from wagtail import hooks
from wagtail.admin.viewsets.chooser import ChooserViewSet


class BoundaryChooserViewSet(ChooserViewSet):
    model = "adminboundarymanager.AdminBoundary"
    
    icon = "map"
    choose_one_text = "Choose a boundary"
    choose_another_text = "Choose another boundary"
    edit_item_text = "Edit this boundary"


@hooks.register("register_admin_viewset")
def register_viewset():
    return [
        AdminBoundaryViewSetGroup(),
        BoundaryChooserViewSet("boundary_chooser")
    ]
