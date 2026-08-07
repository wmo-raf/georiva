from django.urls import reverse
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _
from wagtail.admin.filters import WagtailFilterSet
from wagtail.admin.panels import FieldPanel, ObjectList
from wagtail.admin.views import generic
from wagtail.admin.viewsets.chooser import ChooserViewSet
from wagtail.admin.viewsets.model import ModelViewSet
from wagtail.admin.widgets import ListingButton
from wagtail.snippets.views.snippets import SnippetViewSet, IndexView

from georiva.core.models import Item, Catalog, Collection, ColorRamp, Asset
from georiva.core.models.catalog import Topic
from .admin import CatalogIndexView
from georiva.organisations.access import may_change_org_object, require_active_org
from georiva.organisations.permissions import SuperuserOnlyPermissionPolicy
from georiva.organisations.scoping import OrgScopedChooserViewSetMixin, OrgScopedViewSetMixin


class BoundaryChooserViewSet(ChooserViewSet):
    """Administrative boundaries, offered to every organisation unscoped.

    The deliberate scoping exemption (decision #269), and the reason it is one:
    boundaries are shared reference data, not tenant data. A regional centre
    clips against several countries, a national service against its own and its
    neighbours', and the model is a third party's — putting an organisation FK on
    it would fork ``adminboundarymanager`` to express something no institution
    owns. Nothing under a boundary is anybody's data either: the zonal statistics
    computed over one belong to the variable they came from, and *those* are
    org-scoped through the catalog chain.

    So this chooser is not missing a guard. If per-org curation ever matters, the
    escape hatch is a mapping table between Organisation and boundary, not an FK
    on the third-party model.
    """

    model = "adminboundarymanager.AdminBoundary"

    icon = "map"
    choose_one_text = "Choose a boundary"
    choose_another_text = "Choose another boundary"
    edit_item_text = "Edit this boundary"


class TopicViewSet(ModelViewSet):
    """The instance's thematic taxonomy, curated by the instance admin.

    Topics are shared reference data (decision #259): one vocabulary across the
    instance, so that "Rainfall" means the same thing on every organisation's
    portal. Editing that vocabulary is therefore an instance-admin act, and org
    members meet topics only as options on the catalog form.
    """

    model = Topic
    # Under Settings, not the main menu: curating the instance-wide vocabulary
    # is an instance-admin act, and the entry is superuser-only anyway.
    add_to_settings_menu = True
    menu_order = 300
    menu_icon = "hashtag"
    exclude_form_fields = ["created_at", "updated_at"]

    @cached_property
    def permission_policy(self):
        return SuperuserOnlyPermissionPolicy(self.model)


class CatalogCreateView(generic.CreateView):
    def save_instance(self):
        # The owning organisation is the host's, not a form field: letting an
        # operator pick one would be a way to write into another institution's
        # storage prefix. Set here rather than on the panel set so there is no
        # editable widget to post around.
        self.form.instance.organisation = require_active_org(self.request)
        return super().save_instance()

    def get_success_url(self):
        url = reverse("catalog:index")
        return self._set_locale_query_param(url)


class CatalogEditView(generic.EditView):
    def get_success_url(self):
        return reverse("catalog:index")


class CatalogDeleteView(generic.DeleteView):
    def get_success_url(self):
        return reverse("catalog:index")


class CatalogViewSet(OrgScopedViewSetMixin, ModelViewSet):
    model = Catalog
    icon = "globe"
    menu_label = _("Catalogs")
    menu_icon = "globe"
    # Top-level, just above the "Data" menu group (order 400). Visibility is
    # permission-gated by the viewset policy: groups need a Catalog model
    # permission (view is enough) to see this entry.
    menu_order = 390
    add_to_admin_menu = True
    exclude_form_fields = ["created_at", "updated_at"]
    index_view_class = CatalogIndexView
    index_template_name = "core/catalog_index.html"
    index_results_template_name = "core/catalog_index_results.html"
    add_view_class = CatalogCreateView
    edit_view_class = CatalogEditView
    delete_view_class = CatalogDeleteView


class CatalogChooserViewSet(OrgScopedChooserViewSetMixin, ChooserViewSet):
    model = Catalog
    icon = "globe"
    choose_one_text = "Choose a catalog"
    choose_another_text = "Choose another catalog"
    edit_item_text = "Edit this catalog"


class CollectionCreateView(generic.CreateView):
    def get_success_url(self):
        return reverse("catalog:index")


class CollectionEditView(generic.EditView):
    def get_success_url(self):
        return reverse("catalog:index")


class CollectionDeleteView(generic.DeleteView):
    def get_success_url(self):
        return reverse("catalog:index")


class CollectionIndexView(generic.IndexView):
    def get_list_more_buttons(self, instance):
        buttons = super().get_list_more_buttons(instance)
        # buttons.append(
        #     ListingButton(
        #         _("Zarr Store"),
        #         url=reverse("zarr_collection_detail", args=[instance.pk]),
        #         icon_name="resubmit",
        #         attrs={"title": _("View Zarr Store Details")},
        #     )
        # )
        return buttons


class CollectionViewSet(OrgScopedViewSetMixin, ModelViewSet):
    model = Collection
    icon = "folder-open-inverse"
    add_to_admin_menu = False
    exclude_form_fields = ["created_at", "updated_at"]
    add_view_class = CollectionCreateView
    edit_view_class = CollectionEditView
    delete_view_class = CollectionDeleteView
    index_view_class = CollectionIndexView


class ItemIndexView(IndexView):
    def get_list_more_buttons(self, instance):
        buttons = super().get_list_more_buttons(instance)
        
        label = _("View")
        url = reverse("item_preview", args=[instance.id])
        icon_name = "view"
        attrs = {}
        if label and url:
            buttons.append(
                ListingButton(
                    label,
                    url=url,
                    icon_name=icon_name,
                    attrs=attrs,
                )
            )
        
        return buttons


class ItemFilterSet(WagtailFilterSet):
    class Meta:
        model = Item
        fields = ["collection"]


class ItemViewSet(OrgScopedViewSetMixin, SnippetViewSet):
    model = Item
    icon = "snippet"
    exclude_form_fields = ["created_at", "updated_at"]
    index_view_class = ItemIndexView
    list_filter = ["collection"]


class AssetViewSet(OrgScopedViewSetMixin, SnippetViewSet):
    model = Asset
    exclude_form_fields = ["created_at", "updated_at"]
    list_filter = ["format", "variable"]


class GlobalTierCreateView(generic.CreateView):
    """Creating a tiered row, in whichever tier the creator may write to.

    For an operator there is no choice to make and none is offered: a row
    they create belongs to the organisation whose admin they are in. An editable
    organisation would be a way to file one under another institution, or under
    none — the instance-wide tier, which is not theirs to add to.

    The instance admin is the one who curates that tier, so they get the field:
    blank makes an instance-wide row, and their own organisation is still
    the host's, so routine work on a tenant's host lands where it should.
    """

    def _may_choose_tier(self):
        user = self.request.user
        return user.is_authenticated and user.is_superuser

    def get_panel(self):
        panel = super().get_panel()
        if not self._may_choose_tier() or panel is None:
            return panel
        return ObjectList(
            list(panel.children) + [FieldPanel("organisation")]
        ).bind_to_model(self.model)

    def save_instance(self):
        if not self._may_choose_tier():
            self.form.instance.organisation = require_active_org(self.request)
        return super().save_instance()


class GlobalTierIndexView(generic.IndexView):
    """The listing, showing both tiers and offering edits on only one.

    The instance-wide rows are listed because an operator needs to see what
    they already have before making another; their edit and delete links are
    withheld because ``require_writable_org_object`` would refuse them. The
    listing asks that helper rather than re-deriving the rule.
    """

    def get_edit_url(self, instance):
        return super().get_edit_url(instance) if self._writable(instance) else None

    def get_delete_url(self, instance):
        return super().get_delete_url(instance) if self._writable(instance) else None

    def _writable(self, instance):
        return may_change_org_object(self.request, instance)


class ColorRampModelViewSet(OrgScopedViewSetMixin, ModelViewSet):
    """The Color ramp catalog (ADR 0022): both tiers listed, one writable.

    The instance-wide catalog is readable by every organisation and curated by
    the instance admin; an organisation creates and edits only its own ramps.
    """

    model = ColorRamp
    icon = "palette"
    add_to_admin_menu = True
    menu_order = 600
    exclude_form_fields = ["created_at", "updated_at"]
    add_view_class = GlobalTierCreateView
    index_view_class = GlobalTierIndexView
    # Wagtail's generic copy view binds its form to the *source* row, so saving
    # renames that row instead of duplicating it (verified against Wagtail 7.4.2).
    # Harmless-looking on a single-tenant instance; here it would be a member
    # rewriting the instance-wide ramp every organisation renders with. Until
    # that is fixed upstream or worked around deliberately, ramps are copied by
    # creating one.
    copy_view_enabled = False
    list_display = ["name", "preview", "ramp_type", "owner_label"]


CatalogChooserViewSetObject = CatalogChooserViewSet("catalog_chooser")

admin_viewsets = [
    TopicViewSet(),
    CatalogViewSet(),
    CatalogChooserViewSetObject,
    CollectionViewSet(),
    ColorRampModelViewSet(),
]
