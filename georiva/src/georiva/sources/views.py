from django.core.paginator import Paginator, InvalidPage
from django.shortcuts import render
from django.urls import reverse_lazy, reverse
from django.utils.translation import gettext as _
from wagtail.admin.ui.tables import TitleColumn, Table, ButtonsColumnMixin, BooleanColumn
from wagtail.admin.widgets import HeaderButton, ListingButton, ButtonWithDropdown

from georiva.sources.models import LoaderProfile
from georiva.sources.registry import loader_profile_viewset_registry
from georiva.sources.utils import get_all_child_models, get_child_model_by_name


def loader_profile_list(request):
    loader_profiles = LoaderProfile.objects.all()
    
    class LoaderProfileButtonsColumn(ButtonsColumnMixin, TitleColumn):
        def get_buttons(self, instance, parent_context):
            more_buttons = []
            buttons = []
            
            if edit_url := instance.edit_url:
                more_buttons.append(
                    ListingButton(
                        _("Edit"),
                        url=edit_url,
                        icon_name="edit",
                        attrs={
                            "aria-label": _("Edit '%(title)s'") % {"title": str(instance)}
                        },
                        priority=10,
                    )
                )
            
            if delete_url := instance.delete_url:
                more_buttons.append(
                    ListingButton(
                        _("Delete"),
                        url=delete_url,
                        icon_name="bin",
                        attrs={
                            "aria-label": _("Delete '%(title)s'") % {"title": str(instance)}
                        },
                        priority=30,
                    )
                )
            
            if more_buttons:
                buttons.append(
                    ButtonWithDropdown(
                        buttons=more_buttons,
                        icon_name="dots-horizontal",
                        attrs={
                            "aria-label": _("More options for '%(title)s'")
                                          % {"title": str(instance)},
                        },
                    )
                )
            
            return buttons
    
    def get_url(instance):
        return instance.edit_url
    
    columns = [
        LoaderProfileButtonsColumn("name", label=_("Loader Profile"), get_url=get_url),
        BooleanColumn("is_active", label=_("Active")),
    ]
    
    table = Table(columns, loader_profiles)
    
    context = {
        "breadcrumbs_items": [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": "", "label": _("Loader Profiles"), },
        ],
        "header_buttons": [
            HeaderButton(
                label=_('Add Loader Profile'),
                url=reverse("loader_profile_add_select"),
                icon_name="plus",
            ),
        ],
        "object_list": loader_profiles,
        "table": table,
    }
    
    return render(request, 'georivasources/loader_profile_list.html', context)


def loader_profile_add_select(request):
    breadcrumbs_items = [
        {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
        {"url": reverse_lazy("loader_profile_list"), "label": _("Loader Profiles")},
        {"url": "", "label": _("Select Loader Profile Type")},
    ]
    
    loader_profile_types = get_all_child_models(LoaderProfile)
    items = [{"name": cls.__name__, "verbose_name": cls._meta.verbose_name} for cls in loader_profile_types]
    count = len(items)
    
    # Get search parameters from the query string.
    try:
        page_num = int(request.GET.get("p", 0))
    except ValueError:
        page_num = 0
    
    user = request.user
    paginator = Paginator(items, 20)
    
    try:
        page_obj = paginator.page(page_num + 1)
    except InvalidPage:
        page_obj = paginator.page(1)
    
    def get_url(instance):
        model_cls = get_child_model_by_name(LoaderProfile, instance["name"])
        model_name = model_cls._meta.model_name
        
        viewset = loader_profile_viewset_registry.get(model_name)
        create_url = reverse(viewset.get_url_name("add"))
        return create_url
    
    columns = [
        TitleColumn("verbose_name", label=_("Name"), get_url=get_url),
    ]
    
    context = {
        "breadcrumbs_items": breadcrumbs_items,
        "all_count": count,
        "result_count": count,
        "paginator": paginator,
        "page_obj": page_obj,
        "object_list": page_obj.object_list,
        "table": Table(columns, page_obj.object_list),
    }
    
    return render(request, "georivasources/loader_profile_add_select.html", context)
