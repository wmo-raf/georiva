import json

from django.contrib import messages
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse_lazy, reverse
from django.utils.translation import gettext as _
from wagtail.admin.forms import WagtailAdminModelForm
from wagtail.admin.ui.tables import TitleColumn, Table, ButtonsColumnMixin, BooleanColumn
from wagtail.admin.widgets import HeaderButton, ButtonWithDropdown, Button

from georiva.sources.models import DataFeed
from georiva.sources.registry import data_feed_viewset_registry
from georiva.sources.utils import get_all_child_models, get_child_model_by_name


def _to_json_safe(value):
    """Coerce a form cleaned_data value to a JSON-serializable type for session storage."""
    if hasattr(value, 'isoformat'):
        return value.isoformat()
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return str(value)


def data_feed_list(request):
    data_feeds = DataFeed.objects.all()
    
    class DataFeedButtonsColumn(ButtonsColumnMixin, TitleColumn):
        def get_buttons(self, instance, parent_context):
            buttons = []
            more_buttons = []
            
            if edit_url := instance.edit_url:
                more_buttons.append(
                    Button(
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
                    Button(
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
        return reverse("data_feed_detail", kwargs={"pk": instance.pk})
    
    columns = [
        DataFeedButtonsColumn("name", label=_("Data Feed"), get_url=get_url),
        BooleanColumn("is_active", label=_("Active")),
    ]
    
    table = Table(columns, data_feeds)
    
    context = {
        "breadcrumbs_items": [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": "", "label": _("Data Feeds")},
        ],
        "header_buttons": [
            HeaderButton(
                label=_("Add Data Feed"),
                url=reverse("data_feed_add_select"),
                icon_name="plus",
            ),
        ],
        "object_list": data_feeds,
        "table": table,
    }
    
    return render(request, 'georivasources/data_feed_list.html', context)


def data_feed_detail(request, pk):
    """Dashboard view for a single DataFeed."""
    feed = get_object_or_404(DataFeed, pk=pk)

    if request.method == "POST" and request.POST.get("action") == "run_now":
        feed.run_now(user=request.user)
        messages.success(request, _("Run started for '%s'.") % feed.name)
        return redirect("data_feed_detail", pk=pk)

    from georiva.sources.models import FetchRun  # noqa: keep import next to use
    recent_runs = (
        FetchRun.objects
        .filter(data_feed=feed)
        .order_by("-started_at")[:20]
    )
    
    raw_links = feed.collection_links.select_related("collection__catalog").all()
    collection_links = [link.get_real_instance() for link in raw_links]
    
    real_feed = feed.get_real_instance()
    all_definitions = type(real_feed).get_collection_definitions()
    enabled_keys = {link.definition_key for link in collection_links if link.definition_key}
    
    # Pair each definition with its link (or None if not enabled)
    # For enabled collections, count how many of the definition's variables are present
    from django.utils.text import slugify as _slugify
    definition_link_pairs = []
    link_by_key = {link.definition_key: link for link in collection_links if link.definition_key}
    for defn in all_definitions:
        link = link_by_key.get(defn.key)
        var_count = 0
        if link and defn.variables:
            def_slugs = {_slugify(v.key) for v in defn.variables}
            existing_slugs = set(link.collection.variables.values_list('slug', flat=True))
            var_count = len(def_slugs & existing_slugs)
        definition_link_pairs.append({
            "definition": defn,
            "link": link,
            "enabled": defn.key in enabled_keys,
            "var_count": var_count,
            "var_total": len(defn.variables),
            "multi_variable": len(defn.variables) > 1,
        })
    
    # Derived-products chain panel — the primary management surface (ADR-0008).
    # A malformed plugin declaration can't be laid out into stages; degrade to an
    # empty panel rather than breaking the whole feed page.
    from georiva.core.product_chain import ChainError
    from georiva.sources.product_service import build_chain
    try:
        product_stage_lanes = build_chain(feed)["stages"]
    except ChainError as exc:
        product_stage_lanes = []
        messages.error(request, _("Derived-product chain is invalid: %s") % exc)

    context = {
        "breadcrumbs_items": [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse_lazy("data_feed_list"), "label": _("Data Feeds")},
            {"url": "", "label": feed.name},
        ],
        "feed": feed,
        "real_feed": real_feed,
        "collection_links": collection_links,
        "definition_link_pairs": definition_link_pairs,
        "product_stage_lanes": product_stage_lanes,
        "recent_runs": recent_runs,
        "feed_type_name": type(real_feed)._meta.verbose_name,
    }

    return render(request, "georivasources/data_feed_detail.html", context)


def _apply_product_toggle(request, product, *, confirm_ctx, redirect_url):
    """Shared enable/disable handling for the tracking dashboard and the feed
    panel: enable through the structural gate, disable with the cascade
    confirmation. ``confirm_ctx`` carries the surface-specific confirm form
    (breadcrumbs, form_action, cancel_url, hidden_fields). Returns the confirm
    render or a redirect to ``redirect_url``."""
    from georiva.sources.product_service import (
        ProductActionError,
        disable_product,
        enable_product,
        enabled_dependents,
        product_label,
    )

    if product.is_enabled:
        dependents = enabled_dependents(product)
        if dependents and request.POST.get("confirmed") != "1":
            return render(request, "georivasources/product_disable_confirm.html", {
                "product": product,
                "product_label": product_label(product),
                "dependent_labels": [product_label(d) for d in dependents],
                **confirm_ctx,
            })
        disabled = disable_product(product)
        messages.success(
            request, _("Disabled: %s.") % ", ".join(product_label(d) for d in disabled)
        )
    else:
        try:
            enable_product(product)
            messages.success(request, _("'%s' enabled.") % product_label(product))
        except ProductActionError as exc:
            messages.error(request, str(exc))
    return redirect(redirect_url)


def feed_product_toggle(request, feed_pk, product_pk):
    """Enable/disable one derived product from the feed-detail panel, through the
    same gate/cascade service as the tracking dashboard."""
    from georiva.sources.models import DerivedProduct

    product = get_object_or_404(DerivedProduct, pk=product_pk, data_feed_id=feed_pk)
    detail_url = reverse("data_feed_detail", kwargs={"pk": feed_pk})
    return _apply_product_toggle(
        request, product,
        confirm_ctx={
            "breadcrumbs_items": [
                {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
                {"url": reverse_lazy("data_feed_list"), "label": _("Data Feeds")},
                {"url": detail_url, "label": product.data_feed.name},
                {"url": "", "label": _("Disable")},
            ],
            "form_action": reverse(
                "feed_product_toggle",
                kwargs={"feed_pk": feed_pk, "product_pk": product_pk},
            ),
            "cancel_url": detail_url,
            "hidden_fields": {},
        },
        redirect_url=detail_url,
    )


def feed_product_run(request, feed_pk, product_pk):
    """Manually dispatch one derived product from the feed panel, gated on data
    readiness (mirrors the tracking dashboard's Run now)."""
    from georiva.sources.derivation_invocation import run_product_now
    from georiva.sources.derivation_tracking import product_readiness
    from georiva.sources.models import DerivedProduct
    from georiva.sources.product_service import product_label

    product = get_object_or_404(DerivedProduct, pk=product_pk, data_feed_id=feed_pk)
    readiness = product_readiness(product)
    if readiness.ready:
        run_product_now(product)
        messages.success(request, _("Run started for '%s'.") % product_label(product))
    else:
        messages.error(
            request,
            _("'%(product)s' blocked: %(reason)s.") % {
                "product": product_label(product), "reason": readiness.reason,
            },
        )
    return redirect("data_feed_detail", pk=feed_pk)


def data_feed_edit(request, pk):
    """Edit feed name, interval, and global config fields inline."""
    feed = get_object_or_404(DataFeed, pk=pk)
    real_feed = feed.get_real_instance()
    real_cls = type(real_feed)
    
    form_cls = _global_config_form_class(real_cls, include_base=True)
    
    if request.method == "POST":
        form = form_cls(request.POST, instance=real_feed)
        if form.is_valid():
            form.save()
            messages.success(request, _("Feed details updated."))
            return redirect("data_feed_detail", pk=pk)
    else:
        form = form_cls(instance=real_feed)
    
    return render(request, "georivasources/data_feed_edit.html", {
        "breadcrumbs_items": [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse_lazy("data_feed_list"), "label": _("Data Feeds")},
            {"url": reverse("data_feed_detail", kwargs={"pk": pk}), "label": feed.name},
            {"url": "", "label": _("Edit")},
        ],
        "feed": feed,
        "form": form,
    })


def definition_collection_add(request, feed_pk, definition_key):
    """
    Enable a CollectionDefinition for an existing DataFeed.
    Creates the Collection, Variables, and DataFeedCollectionLink.
    """
    from georiva.sources.setup_service import SourceSetupService
    
    feed = get_object_or_404(DataFeed, pk=feed_pk)
    
    real_feed = feed.get_real_instance()
    real_cls = type(real_feed)
    
    definitions = {d.key: d for d in real_cls.get_collection_definitions()}
    definition = definitions.get(definition_key)
    if not definition:
        messages.error(request, _("Unknown collection definition: %s") % definition_key)
        return redirect("data_feed_detail", pk=feed_pk)
    
    if not feed.catalog:
        messages.error(request, _("This DataFeed has no linked Catalog. Cannot add collections."))
        return redirect("data_feed_detail", pk=feed_pk)
    
    config_form_cls = real_cls.get_collection_link_model().get_form_class()
    
    if request.method == "POST":
        config_form = config_form_cls(request.POST) if config_form_cls else None
        config_values = {}
        errors = []
        
        if config_form is not None:
            if config_form.is_valid():
                config_values.update(config_form.cleaned_data)
            else:
                errors.extend(
                    f"{config_form.fields[f].label}: {', '.join(errs)}"
                    for f, errs in config_form.errors.items()
                )
        
        if len(definition.variables) > 1:
            sel_vars = request.POST.getlist("variables")
            if not sel_vars:
                errors.append(_("Select at least one variable."))
            else:
                config_values["selected_variable_keys"] = sel_vars
        
        if not errors:
            service = SourceSetupService()
            service.provision_collection(
                catalog=feed.catalog,
                definition=definition,
                data_feed=real_feed,
                config_values=config_values,
            )
            messages.success(request, _("Collection '%s' added.") % definition.name)
            return redirect("data_feed_detail", pk=feed_pk)
        
        for e in errors:
            messages.error(request, e)
    else:
        config_form = config_form_cls() if config_form_cls else None
    
    return render(request, "georivasources/definition_collection_form.html", {
        "breadcrumbs_items": [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse_lazy("data_feed_list"), "label": _("Data Feeds")},
            {"url": reverse("data_feed_detail", kwargs={"pk": feed_pk}), "label": feed.name},
            {"url": "", "label": _("Add Collection")},
        ],
        "feed": feed,
        "definition": definition,
        "config_form": config_form,
        "multi_variable": len(definition.variables) > 1,
        "variable_groups": _build_variable_groups(definition),
    })


def definition_collection_edit(request, feed_pk, link_pk):
    """Edit per-collection config for an existing link."""
    from georiva.sources.models import DataFeedCollectionLink
    
    feed = get_object_or_404(DataFeed, pk=feed_pk)
    
    base_link = get_object_or_404(DataFeedCollectionLink, pk=link_pk, data_feed=feed)
    link = base_link.get_real_instance()
    
    real_cls = type(feed.get_real_instance())
    definitions = {d.key: d for d in real_cls.get_collection_definitions()}
    definition = definitions.get(link.definition_key)
    
    form_cls = type(link).get_form_class()
    
    if not form_cls:
        messages.info(request, _("This collection link has no configurable fields."))
        return redirect("data_feed_detail", pk=feed_pk)
    
    if request.method == "POST":
        form = form_cls(request.POST, instance=link)
        if form.is_valid():
            form.save()
            messages.success(request, _("Collection config updated."))
            return redirect("data_feed_detail", pk=feed_pk)
    else:
        form = form_cls(instance=link)
    
    return render(request, "georivasources/definition_collection_form.html", {
        "breadcrumbs_items": [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse_lazy("data_feed_list"), "label": _("Data Feeds")},
            {"url": reverse("data_feed_detail", kwargs={"pk": feed_pk}), "label": feed.name},
            {"url": "", "label": _("Edit Collection Config")},
        ],
        "feed": feed,
        "link": link,
        "definition": definition,
        "config_form": form,
        "is_edit": True,
        "multi_variable": False,  # variable selection not available on edit
    })


def definition_collection_remove_confirm(request, feed_pk, link_pk):
    """Confirmation page before removing a collection link and deleting the collection."""
    from georiva.sources.models import DataFeedCollectionLink
    
    feed = get_object_or_404(DataFeed, pk=feed_pk)
    base_link = get_object_or_404(DataFeedCollectionLink, pk=link_pk, data_feed=feed)
    link = base_link.get_real_instance()
    collection = link.collection
    
    if request.method == "POST":
        collection_name = collection.name
        collection.delete()  # cascades: link → Variables → Items → Assets
        messages.success(request, _("'%s' and all its data have been deleted.") % collection_name)
        return redirect("data_feed_detail", pk=feed_pk)
    
    return render(request, "georivasources/definition_collection_remove_confirm.html", {
        "breadcrumbs_items": [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse_lazy("data_feed_list"), "label": _("Data Feeds")},
            {"url": reverse("data_feed_detail", kwargs={"pk": feed_pk}), "label": feed.name},
            {"url": "", "label": _("Remove Collection")},
        ],
        "feed": feed,
        "link": link,
        "collection": collection,
    })


def definition_collection_vars_edit(request, feed_pk, link_pk):
    """Manage which variables from the definition are active in the collection."""
    from django.utils.text import slugify
    from georiva.sources.models import DataFeedCollectionLink
    from georiva.sources.setup_service import SourceSetupService
    
    feed = get_object_or_404(DataFeed, pk=feed_pk)
    base_link = get_object_or_404(DataFeedCollectionLink, pk=link_pk, data_feed=feed)
    link = base_link.get_real_instance()
    collection = link.collection
    
    real_cls = type(feed.get_real_instance())
    definitions = {d.key: d for d in real_cls.get_collection_definitions()}
    definition = definitions.get(link.definition_key)
    
    if not definition or len(definition.variables) <= 1:
        return redirect("data_feed_detail", pk=feed_pk)
    
    existing_slugs = set(collection.variables.values_list("slug", flat=True))
    
    if request.method == "POST":
        selected_keys = set(request.POST.getlist("variables"))
        service = SourceSetupService()
        
        for var_def in definition.variables:
            var_slug = slugify(var_def.key)
            in_collection = var_slug in existing_slugs
            selected = var_def.key in selected_keys
            
            if selected and not in_collection:
                service._upsert_variable(collection, var_def)
            elif not selected and in_collection:
                collection.variables.filter(slug=var_slug).delete()
        
        messages.success(request, _("Variables updated for '%s'.") % collection.name)
        return redirect("data_feed_detail", pk=feed_pk)
    
    # Pre-annotate each group's variables with active state so the template
    # doesn't need nested lookups.
    raw_groups = _build_variable_groups(definition)
    variable_groups = [
        {
            **grp,
            "variables": [
                {"var_def": v, "active": slugify(v.key) in existing_slugs}
                for v in grp["variables"]
            ],
        }
        for grp in raw_groups
    ]
    
    return render(request, "georivasources/definition_collection_vars.html", {
        "breadcrumbs_items": [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse_lazy("data_feed_list"), "label": _("Data Feeds")},
            {"url": reverse("data_feed_detail", kwargs={"pk": feed_pk}), "label": feed.name},
            {"url": "", "label": _("Manage Variables")},
        ],
        "feed": feed,
        "link": link,
        "definition": definition,
        "collection": collection,
        "variable_groups": variable_groups,
        "var_total": len(definition.variables),
    })


def data_feed_add_select(request):
    items = []
    for cls in get_all_child_models(DataFeed):
        model_name = cls._meta.model_name
        has_wizard = bool(cls.get_collection_definitions())
        
        if has_wizard:
            url = reverse("wizard_step1_catalog", kwargs={"model_name": model_name})
        else:
            viewset = data_feed_viewset_registry.get(model_name)
            url = reverse(viewset.get_url_name("add")) if viewset else "#"
        
        items.append({
            "verbose_name": cls._meta.verbose_name,
            "has_wizard": has_wizard,
            "url": url,
        })
    
    context = {
        "breadcrumbs_items": [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse_lazy("data_feed_list"), "label": _("Data Feeds")},
            {"url": "", "label": _("Add Data Feed")},
        ],
        "items": items,
    }
    
    return render(request, "georivasources/data_feed_add_select.html", context)


# =============================================================================
# Setup Wizard (3 steps)
# =============================================================================

_WIZARD_SESSION_KEY = "georiva_setup_wizard_{model_name}"


def _wizard_session_key(model_name):
    return _WIZARD_SESSION_KEY.format(model_name=model_name)


def _get_model_or_redirect(request, model_name):
    model_cls = get_child_model_by_name(DataFeed, model_name)
    if not model_cls:
        messages.error(request, _("Unknown source type: %s") % model_name)
        return None, redirect("setup_wizard_select")
    return model_cls, None


def _wizard_breadcrumbs(model_name, verbose_name, current_label):
    return [
        {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
        {"url": reverse_lazy("data_feed_list"), "label": _("Data Feeds")},
        {"url": reverse("setup_wizard_select"), "label": _("Setup Wizard")},
        {"url": reverse("wizard_step1_catalog", kwargs={"model_name": model_name}), "label": verbose_name},
        {"url": "", "label": current_label},
    ]


def _global_config_form_class(model_cls, include_base=False):
    """
    Build a ModelForm for the DataFeed subclass covering:
      - base fields (name, is_active, interval_minutes) if include_base=True
      - any extra fields declared in model_cls.panels beyond base_panels
    """
    from wagtail.admin.panels import FieldPanel, MultiFieldPanel
    from django.forms import modelform_factory
    
    base_field_names = {p.field_name for p in DataFeed.base_panels if isinstance(p, FieldPanel)}
    
    def _extract_fields(panels):
        names = []
        for panel in panels:
            if isinstance(panel, FieldPanel):
                names.append(panel.field_name)
            elif isinstance(panel, MultiFieldPanel):
                names.extend(_extract_fields(panel.children))
        return names
    
    all_fields = _extract_fields(model_cls.panels)
    
    if include_base:
        fields = all_fields
    else:
        fields = [f for f in all_fields if f not in base_field_names]
    
    if not fields:
        return None
    
    base_form_class = getattr(model_cls, "base_form_class", WagtailAdminModelForm)

    return modelform_factory(model_cls, form=base_form_class, fields=fields)


def build_product_config_form(definition):
    """
    Build a Django Form class for one DerivedProductDefinition's options, driven
    by its config_schema (ADR-0008). One field per ConfigField (a ChoiceField
    for ``choice``, typed fields otherwise), pre-filled from each field's
    default. The form's ``clean`` runs the values back through
    ``definition.validate_config`` — the single source of truth — and exposes
    the validated dict as ``form.cleaned_config``. Returns None when the product
    has no options (the wizard still shows its label/description, just no form).
    """
    from django import forms

    schema = definition.config_schema
    if not schema:
        return None

    field_makers = {
        "int": forms.IntegerField,
        "float": forms.FloatField,
        "bool": forms.BooleanField,
        "str": forms.CharField,
    }

    attrs = {}
    for field in schema:
        label = field.key.replace("_", " ").title()
        if field.type == "choice":
            attrs[field.key] = forms.ChoiceField(
                label=label, required=False, initial=field.default,
                choices=[(c, c) for c in field.choices],
            )
        else:
            attrs[field.key] = field_makers[field.type](
                label=label, required=False, initial=field.default,
            )

    keys = [f.key for f in schema]

    def clean(self):
        cleaned = forms.Form.clean(self)
        raw = {k: cleaned[k] for k in keys if cleaned.get(k) not in (None, "")}
        try:
            self.cleaned_config = definition.validate_config(raw)
        except ValueError as exc:
            raise forms.ValidationError(str(exc))
        return cleaned

    attrs["clean"] = clean
    return type("ProductConfigForm", (forms.Form,), attrs)


def selected_products_from_session(data_feed, session_data) -> list:
    """
    Map the wizard session onto the feed's declared DerivedProductDefinitions as
    ``(definition, config, enabled)`` triples ready for
    SourceSetupService.provision_derived_products.

    A triple is produced for *every* declared definition — provisioning always
    writes a full row set, with an operator's opt-out carried as ``enabled``
    rather than a missing row. ``enabled`` comes from the step-4 tick selection
    (``selected_product_keys``); if no selection was stored (a short-circuit or
    stale path) each product falls back to its declared ``default_enabled``. An
    empty selection list is honoured as "all unticked", not "no selection".
    Config stored for a product the feed no longer declares is ignored.
    """
    products_config = session_data.get("derived_products_config", {})
    selected_keys = session_data.get("selected_product_keys")
    triples = []
    for defn in data_feed.get_derived_products():
        if selected_keys is None:
            enabled = defn.default_enabled
        else:
            enabled = defn.key in selected_keys
        triples.append((defn, products_config.get(defn.key, {}), enabled))
    return triples


def _transient_feed_for_products(model_cls, session_data):
    """An unsaved feed instance (carrying the wizard's chosen catalog) used only
    to ask the plugin which derived products it declares, before the feed is
    provisioned.

    The feed has no collection_links yet, so the resolutions chosen in step 3 are
    stashed on the instance as ``_wizard_selected_keys``. An instance
    get_derived_products() reads links when the feed is saved, else this stash —
    so the declared product set is identical at step 4 and at provisioning."""
    from georiva.core.models import Catalog

    catalog = None
    if session_data.get("catalog_mode") == "create":
        catalog = Catalog(
            slug=session_data.get("new_catalog_slug", ""),
            name=session_data.get("new_catalog_name", ""),
        )
    elif session_data.get("catalog_id"):
        catalog = Catalog.objects.filter(pk=session_data["catalog_id"]).first()
    feed = model_cls(catalog=catalog) if catalog else model_cls()
    feed._wizard_selected_keys = session_data.get("selected_collection_keys", [])
    return feed


def setup_wizard_select(request):
    """Step 0: pick source type."""
    capable = [cls for cls in get_all_child_models(DataFeed) if cls.get_collection_definitions()]
    source_types = [
        {"model_name": cls.__name__, "verbose_name": cls._meta.verbose_name}
        for cls in capable
    ]
    breadcrumbs = [
        {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
        {"url": reverse_lazy("data_feed_list"), "label": _("Data Feeds")},
        {"url": "", "label": _("Setup Wizard")},
    ]
    return render(request, "georivasources/wizard_select.html", {
        "breadcrumbs_items": breadcrumbs,
        "source_types": source_types,
    })


def wizard_step1_catalog(request, model_name):
    """Step 1: create a new Catalog or select an existing unclaimed one."""
    from django.utils.text import slugify
    from georiva.core.models import Catalog
    
    model_cls, err = _get_model_or_redirect(request, model_name)
    if err:
        return err
    
    verbose_name = model_cls._meta.verbose_name
    # Only show catalogs that don't already have a DataFeed linked
    unclaimed_catalogs = Catalog.objects.filter(data_feed__isnull=True).order_by("name")
    catalog_defaults = model_cls.get_catalog_defaults()
    file_format_choices = Catalog.FileFormat.choices
    
    if request.method == "POST":
        catalog_mode = request.POST.get("catalog_mode", "create")
        catalog_id = request.POST.get("catalog_id") or None
        new_catalog_name = request.POST.get("new_catalog_name", "").strip()
        new_catalog_slug = request.POST.get("new_catalog_slug", "").strip() or slugify(new_catalog_name)
        new_catalog_format = request.POST.get("new_catalog_format", "")
        new_catalog_description = request.POST.get("new_catalog_description", "").strip()
        
        errors = []
        if catalog_mode == "select" and not catalog_id:
            errors.append(_("Please choose a catalog."))
        if catalog_mode == "create":
            if not new_catalog_name:
                errors.append(_("Please enter a name for the new Catalog."))
            if not new_catalog_format:
                errors.append(_("Please choose a file format."))
            if new_catalog_slug and Catalog.objects.filter(slug=new_catalog_slug).exists():
                errors.append(_("A Catalog with slug '%s' already exists.") % new_catalog_slug)
        
        if errors:
            for e in errors:
                messages.error(request, e)
        else:
            session = request.session.get(_wizard_session_key(model_name), {})
            session.update({
                "catalog_mode": catalog_mode,
                "catalog_id": int(catalog_id) if catalog_mode == "select" and catalog_id else None,
                "new_catalog_name": new_catalog_name if catalog_mode == "create" else None,
                "new_catalog_slug": new_catalog_slug if catalog_mode == "create" else None,
                "new_catalog_format": new_catalog_format if catalog_mode == "create" else None,
                "new_catalog_description": new_catalog_description if catalog_mode == "create" else None,
            })
            request.session[_wizard_session_key(model_name)] = session
            return redirect("wizard_step2_feed", model_name=model_name)
    
    session_data = request.session.get(_wizard_session_key(model_name), {})
    
    breadcrumbs = [
        {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
        {"url": reverse_lazy("data_feed_list"), "label": _("Data Feeds")},
        {"url": reverse("setup_wizard_select"), "label": _("Setup Wizard")},
        {"url": "", "label": verbose_name},
    ]
    return render(request, "georivasources/wizard_step1_catalog.html", {
        "breadcrumbs_items": breadcrumbs,
        "model_name": model_name,
        "source_verbose_name": verbose_name,
        "unclaimed_catalogs": unclaimed_catalogs,
        "catalog_defaults": catalog_defaults,
        "file_format_choices": file_format_choices,
        "session": session_data,
        "step": 1,
    })


def wizard_step2_feed(request, model_name):
    """Step 2: feed name, run interval, and plugin-specific global config fields."""
    model_cls, err = _get_model_or_redirect(request, model_name)
    if err:
        return err
    
    session_data = request.session.get(_wizard_session_key(model_name), {})
    if not session_data.get("catalog_mode"):
        return redirect("wizard_step1_catalog", model_name=model_name)
    
    verbose_name = model_cls._meta.verbose_name
    extra_form_cls = _global_config_form_class(model_cls, include_base=False)
    
    if request.method == "POST":
        new_feed_name = request.POST.get("new_feed_name", "").strip()
        new_feed_interval = int(request.POST.get("new_feed_interval") or 360)
        extra_form = extra_form_cls(request.POST) if extra_form_cls else None
        
        errors = []
        if not new_feed_name:
            errors.append(_("Please enter a name for the Data Feed."))
        
        global_config = {}
        if extra_form is not None:
            if not extra_form.is_valid():
                errors.extend(
                    f"{extra_form.fields[f].label}: {', '.join(errs)}"
                    for f, errs in extra_form.errors.items()
                )
            else:
                global_config = {k: _to_json_safe(v) for k, v in extra_form.cleaned_data.items()}
        
        if not errors:
            session_data.update({
                "new_feed_name": new_feed_name,
                "new_feed_interval": new_feed_interval,
                "global_config": global_config,
            })
            request.session[_wizard_session_key(model_name)] = session_data
            return redirect("wizard_step3_collections", model_name=model_name)
        
        for e in errors:
            messages.error(request, e)
    else:
        extra_form = extra_form_cls(initial=session_data.get("global_config", {})) if extra_form_cls else None
    
    return render(request, "georivasources/wizard_step2_feed.html", {
        "breadcrumbs_items": _wizard_breadcrumbs(model_name, verbose_name, _("Feed Details")),
        "model_name": model_name,
        "source_verbose_name": verbose_name,
        "default_feed_name": verbose_name,
        "prefill_feed_name": session_data.get("new_feed_name", ""),
        "prefill_interval": session_data.get("new_feed_interval", 360),
        "extra_form": extra_form,
        "step": 2,
    })


def _build_variable_groups(definition):
    """
    Return a list of group dicts for template rendering.

    Each dict has: type ('group' | 'ungrouped'), name, key, variables.
    Variables not covered by any group are collected into an 'Other' section.
    When there are no declared groups the single 'ungrouped' entry covers all variables.
    """
    if not definition.groups:
        return [{'type': 'ungrouped', 'key': 'all', 'name': '', 'variables': list(definition.variables)}]
    
    var_by_key = {v.key: v for v in definition.variables}
    covered = set()
    entries = []
    for grp in definition.groups:
        grp_vars = [var_by_key[k] for k in grp.variable_keys if k in var_by_key]
        if grp_vars:
            entries.append({'type': 'group', 'key': grp.key, 'name': grp.name, 'variables': grp_vars})
            covered.update(grp.variable_keys)
    
    remaining = [v for v in definition.variables if v.key not in covered]
    if remaining:
        entries.append({'type': 'ungrouped', 'key': 'other', 'name': 'Other', 'variables': remaining})
    return entries


def wizard_step3_collections(request, model_name):
    """Step 3: choose which CollectionDefinitions to enable and fill per-collection config."""
    model_cls, err = _get_model_or_redirect(request, model_name)
    if err:
        return err
    
    session_data = request.session.get(_wizard_session_key(model_name), {})
    if not session_data.get("new_feed_name") and not session_data.get("new_feed_interval"):
        return redirect("wizard_step2_feed", model_name=model_name)
    
    verbose_name = model_cls._meta.verbose_name
    definitions = model_cls.get_collection_definitions()
    
    # All definitions share the same link model — build the form class once.
    link_form_cls = model_cls.get_collection_link_model().get_form_class()
    
    def_entries = []
    for defn in definitions:
        saved_config = (session_data.get("collections_config") or {}).get(defn.key, {})
        if request.method == "POST":
            form = link_form_cls(request.POST, prefix=defn.key) if link_form_cls else None
        else:
            form = link_form_cls(initial=saved_config, prefix=defn.key) if link_form_cls else None
        def_entries.append({
            "definition": defn,
            "config_form": form,
            "variable_groups": _build_variable_groups(defn),
            "multi_variable": len(defn.variables) > 1,
        })
    
    if request.method == "POST":
        selected_keys = request.POST.getlist("collections")
        
        errors = []
        if not selected_keys:
            errors.append(_("Please select at least one collection."))
        
        # Validate config forms and collect variable selections
        collections_config = {}
        for entry in def_entries:
            defn = entry["definition"]
            if defn.key not in selected_keys:
                continue
            cfg = {}
            
            form = entry["config_form"]
            if form is not None:
                if form.is_valid():
                    cfg.update(form.cleaned_data)
                else:
                    errors.extend(
                        f"{defn.name} — {form.fields[f].label}: {', '.join(errs)}"
                        for f, errs in form.errors.items()
                    )
            
            # Collect variable selection (only when multiple variables)
            if len(defn.variables) > 1:
                sel_vars = request.POST.getlist(f"vars_{defn.key}")
                if not sel_vars:
                    errors.append(_("%(name)s: select at least one variable.") % {"name": defn.name})
                else:
                    cfg["selected_variable_keys"] = sel_vars
            
            collections_config[defn.key] = cfg
        
        if not errors:
            session_data.update({
                "selected_collection_keys": selected_keys,
                "collections_config": {
                    k: {
                           field: _to_json_safe(v)
                           for field, v in cfg.items()
                           if field != "selected_variable_keys"
                       } | (
                           {"selected_variable_keys": cfg["selected_variable_keys"]}
                           if "selected_variable_keys" in cfg else {}
                       )
                    for k, cfg in collections_config.items()
                },
            })
            request.session[_wizard_session_key(model_name)] = session_data
            return redirect("wizard_step4_products", model_name=model_name)

        for e in errors:
            messages.error(request, e)

    prefill_keys = set(session_data.get("selected_collection_keys", [k["definition"].key for k in def_entries]))
    
    return render(request, "georivasources/wizard_step3_collections.html", {
        "breadcrumbs_items": _wizard_breadcrumbs(model_name, verbose_name, _("Collections")),
        "model_name": model_name,
        "source_verbose_name": verbose_name,
        "def_entries": def_entries,
        "prefill_keys": prefill_keys,
        "step": 3,
        "link_form_cls": link_form_cls,
    })


def _product_chain_context(definitions, label_by_key):
    """The product-chain read-model the wizard step renders from: topological
    stage lanes, each product's direct dependencies (for "needs" chips), and the
    transitive closures both directions (the server-side gate and the client-side
    cascade adjacency).

    A malformed declaration (duplicate keys, unknown ``depends_on``, a cycle)
    can't be laid out into stages; rather than 500, fall back to a single lane
    and return the error string so the step still renders and names the problem.
    """
    from georiva.core.product_chain import (
        ChainError,
        dependencies_closure,
        dependents_closure,
        product_dependencies,
        topological_stages,
    )

    keys = list(label_by_key)
    try:
        stages = topological_stages(definitions)
        direct = {k: sorted(v) for k, v in product_dependencies(definitions).items()}
        dep_closures = {k: dependencies_closure(definitions, k) for k in keys}
        dependent_closures = {k: dependents_closure(definitions, k) for k in keys}
        error = None
    except ChainError as exc:
        stages = [list(definitions)]
        direct = {k: [] for k in keys}
        dep_closures = {k: set() for k in keys}
        dependent_closures = {k: set() for k in keys}
        error = str(exc)

    return {
        "stages": stages,
        "direct_deps": direct,
        "dep_closures": dep_closures,
        "dep_closures_json": {k: sorted(v) for k, v in dep_closures.items()},
        "dependent_closures_json": {k: sorted(v) for k, v in dependent_closures.items()},
        "error": error,
    }


def wizard_step4_products(request, model_name):
    """Step 4: configure the derived products the feed declares (ADR-0008).

    A feed that declares no products skips this step straight to provisioning.
    """
    model_cls, err = _get_model_or_redirect(request, model_name)
    if err:
        return err

    session_data = request.session.get(_wizard_session_key(model_name), {})
    if not session_data.get("selected_collection_keys"):
        return redirect("wizard_step3_collections", model_name=model_name)

    feed = _transient_feed_for_products(model_cls, session_data)
    definitions = feed.get_derived_products()
    if not definitions:
        # Nothing to configure — don't show an empty step.
        return redirect("wizard_provision", model_name=model_name)

    verbose_name = model_cls._meta.verbose_name
    saved = session_data.get("derived_products_config", {})
    # Prior tick selection, if the operator has visited this step before (a
    # back-navigation); None means "not chosen yet" -> fall back to default_enabled.
    session_selected = session_data.get("selected_product_keys")
    posted_selected = set(request.POST.getlist("products")) if request.method == "POST" else None

    label_by_key = {d.key: d.label for d in definitions}
    chain = _product_chain_context(definitions, label_by_key)
    if chain["error"]:
        messages.error(request, chain["error"])

    entries = {}
    for defn in definitions:
        form_cls = build_product_config_form(defn)
        if form_cls is None:
            form = None
        elif request.method == "POST":
            form = form_cls(request.POST, prefix=defn.key)
        else:
            form = form_cls(initial=saved.get(defn.key, {}), prefix=defn.key)

        if posted_selected is not None:
            checked = defn.key in posted_selected
        elif session_selected is not None:
            checked = defn.key in session_selected
        else:
            checked = defn.default_enabled

        entries[defn.key] = {
            "definition": defn,
            "config_form": form,
            "checked": checked,
            # Direct-dependency labels, for the card's "needs: X" chips.
            "needs": [label_by_key[dep] for dep in chain["direct_deps"].get(defn.key, [])],
        }

    # Entries grouped into topological stage lanes (single lane on a broken chain).
    stage_lanes = [[entries[d.key] for d in stage] for stage in chain["stages"]]

    if request.method == "POST":
        errors = []
        products_config = {}
        for defn in definitions:
            entry = entries[defn.key]
            form = entry["config_form"]
            # Config is validated only for ticked products; an unticked product
            # (or a ticked one with no options) provisions with schema defaults
            # (empty config), so a bad value on a product the operator opted out
            # of never blocks the wizard.
            if not entry["checked"] or form is None:
                products_config[defn.key] = {}
            elif form.is_valid():
                products_config[defn.key] = {
                    k: _to_json_safe(v) for k, v in form.cleaned_config.items()
                }
            else:
                errors.append(f"{defn.label}: {'; '.join(form.errors.get('__all__', []))}".strip(": "))

        # Enforce the chain server-side: every ticked product must have its whole
        # dependency closure ticked too. JS cascade is advisory — this is the
        # authoritative gate, so a hand-crafted POST can't smuggle in an orphan.
        selected_keys = {k for k, e in entries.items() if e["checked"]}
        for key in selected_keys:
            missing = chain["dep_closures"].get(key, set()) - selected_keys
            if missing:
                errors.append(
                    _("%(product)s needs %(deps)s to be enabled too.") % {
                        "product": label_by_key[key],
                        "deps": ", ".join(sorted(label_by_key.get(m, m) for m in missing)),
                    }
                )

        if not errors:
            session_data["derived_products_config"] = products_config
            session_data["selected_product_keys"] = [
                k for k in label_by_key if entries[k]["checked"]
            ]
            request.session[_wizard_session_key(model_name)] = session_data
            return redirect("wizard_provision", model_name=model_name)

        for e in errors:
            messages.error(request, e)

    return render(request, "georivasources/wizard_step4_products.html", {
        "breadcrumbs_items": _wizard_breadcrumbs(model_name, verbose_name, _("Derived Products")),
        "model_name": model_name,
        "source_verbose_name": verbose_name,
        "stage_lanes": stage_lanes,
        "dependencies_closure_json": chain["dep_closures_json"],
        "dependents_closure_json": chain["dependent_closures_json"],
        "step": 4,
    })


def wizard_provision(request, model_name):
    """Execute provisioning and redirect to the new DataFeed detail page."""
    from georiva.core.models import Catalog
    from georiva.sources.setup_service import SourceSetupService
    
    model_cls, err = _get_model_or_redirect(request, model_name)
    if err:
        return err
    
    session_data = request.session.get(_wizard_session_key(model_name))
    if not session_data or not session_data.get("selected_collection_keys"):
        messages.warning(request, _("Please complete all steps first."))
        return redirect("setup_wizard_select")
    
    # Resolve or create Catalog
    catalog_mode = session_data.get("catalog_mode", "select")
    if catalog_mode == "create":
        catalog, _created = Catalog.objects.get_or_create(
            slug=session_data["new_catalog_slug"],
            defaults={
                "name": session_data["new_catalog_name"],
                "file_format": session_data["new_catalog_format"],
                "description": session_data.get("new_catalog_description", ""),
            },
        )
    else:
        catalog = get_object_or_404(Catalog, pk=session_data["catalog_id"])
    
    definitions_map = {d.key: d for d in model_cls.get_collection_definitions()}
    selected_keys = session_data["selected_collection_keys"]
    raw_configs = session_data.get("collections_config", {})
    
    # Deserialise session config through the link model's form (handles type coercion).
    link_form_cls = model_cls.get_collection_link_model().get_form_class()
    selected_definitions = []
    for key in selected_keys:
        defn = definitions_map.get(key)
        if not defn:
            continue
        raw_cfg = raw_configs.get(key, {})
        cfg = {}
        if link_form_cls:
            form_data = {k: v for k, v in raw_cfg.items() if k != 'selected_variable_keys'}
            form = link_form_cls(form_data)
            if form.is_valid():
                # interval_minutes=None means "use definition/feed default" — exclude it
                cfg.update({
                    k: v for k, v in form.cleaned_data.items()
                    if not (k == 'interval_minutes' and v is None)
                })
        if 'selected_variable_keys' in raw_cfg:
            cfg['selected_variable_keys'] = raw_cfg['selected_variable_keys']
        selected_definitions.append((defn, cfg))
    
    global_config = session_data.get("global_config", {})
    
    service = SourceSetupService()
    try:
        data_feed, collections = service.provision(
            model_cls,
            catalog=catalog,
            feed_name=session_data["new_feed_name"],
            feed_interval=session_data.get("new_feed_interval", 360),
            global_config=global_config,
            selected_definitions=selected_definitions,
        )
        # Derived products are configured against the now-saved feed's declared
        # definitions (ADR-0008); a feed declaring none provisions nothing here.
        service.provision_derived_products(
            data_feed, selected_products_from_session(data_feed, session_data)
        )
        request.session.pop(_wizard_session_key(model_name), None)
        messages.success(
            request,
            _("%(name)s set up with %(n)d collection(s).") % {
                "name": data_feed.name,
                "n": len(collections),
            }
        )
        return redirect("data_feed_detail", pk=data_feed.pk)
    
    except Exception as exc:
        messages.error(request, _("Provisioning failed: %s") % exc)
        return redirect("wizard_step3_collections", model_name=model_name)


def derived_product_tracking(request):
    """
    Ingestion-style tracking dashboard for derived products (ADR-0008).

    Lists every DerivedProduct with an aggregate run status (idle / running /
    failed / completed) computed by joining DerivationRuns on the product's
    origin key, plus an enable/disable toggle that pauses a product without
    deleting its configuration.
    """
    from georiva.sources.derivation_invocation import run_product_now
    from georiva.sources.derivation_tracking import product_readiness, product_status
    from georiva.sources.models import DerivedProduct
    from georiva.sources.product_service import product_label

    if request.method == "POST" and request.POST.get("action") == "toggle":
        product = get_object_or_404(DerivedProduct, pk=request.POST.get("product_pk"))
        # Route through the shared gate/cascade helper — the same write-path the
        # feed panel uses — so the dependency invariant holds from every surface.
        tracking_url = reverse("derived_product_tracking")
        return _apply_product_toggle(
            request, product,
            confirm_ctx={
                "breadcrumbs_items": [
                    {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
                    {"url": reverse_lazy("data_feed_list"), "label": _("Data Feeds")},
                    {"url": tracking_url, "label": _("Derived Products")},
                    {"url": "", "label": _("Disable")},
                ],
                "form_action": tracking_url,
                "cancel_url": tracking_url,
                "hidden_fields": {"action": "toggle", "product_pk": product.pk},
            },
            redirect_url=tracking_url,
        )

    if request.method == "POST" and request.POST.get("action") == "run_now":
        product = get_object_or_404(DerivedProduct, pk=request.POST.get("product_pk"))
        readiness = product_readiness(product)
        if readiness.ready:
            run_product_now(product)
            messages.success(request, _("Run started for '%s'.") % product.definition_key)
        else:
            messages.error(
                request,
                _("'%(key)s' blocked: %(reason)s.") % {
                    "key": product.definition_key, "reason": readiness.reason,
                },
            )
        return redirect("derived_product_tracking")

    products = (
        DerivedProduct.objects
        .select_related("data_feed", "data_feed__catalog")
        .order_by("data_feed_id", "id")
    )

    rows = []
    for product in products:
        # Best-effort human label from the feed's declaration (falls back to key).
        label = product.definition_key
        for defn in product.data_feed.get_derived_products():
            if defn.key == product.definition_key:
                label = defn.label
                break
        rows.append({
            "product": product,
            "label": label,
            "status": product_status(product),
            "readiness": product_readiness(product),
        })

    context = {
        "breadcrumbs_items": [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse_lazy("data_feed_list"), "label": _("Data Feeds")},
            {"url": "", "label": _("Derived Products")},
        ],
        "rows": rows,
    }
    return render(request, "georivasources/derived_product_tracking.html", context)


def derived_product_chain(request, feed_pk):
    """
    Server-rendered chain diagram (ADR-0008): the planned DAG of a feed's
    pipeline — collections are nodes, products are edges labeled with
    recipe / status / readiness / trigger, including configured-but-unrun
    (blocked) edges with their reason. No interactive graph library.
    """
    from georiva.sources.derivation_chain import build_chain_graph

    feed = get_object_or_404(DataFeed, pk=feed_pk)
    graph = build_chain_graph(feed)

    context = {
        "breadcrumbs_items": [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse_lazy("data_feed_list"), "label": _("Data Feeds")},
            {"url": reverse("data_feed_detail", kwargs={"pk": feed.pk}), "label": feed.name},
            {"url": "", "label": _("Chain")},
        ],
        "feed": feed,
        "graph": graph,
    }
    return render(request, "georivasources/derived_product_chain.html", context)


def item_lineage(request, item_pk):
    """
    Item-level provenance drill-down (ADR-0008): the input items a produced
    item was derived from, read from DerivationLink. The read-side lineage view
    for one produced Item.
    """
    from georiva.core.models import Item
    from georiva.sources.derivation_chain import item_lineage as lineage_sources

    item = get_object_or_404(Item, pk=item_pk)
    sources = lineage_sources(item)

    context = {
        "breadcrumbs_items": [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse_lazy("data_feed_list"), "label": _("Data Feeds")},
            {"url": "", "label": _("Lineage")},
        ],
        "item": item,
        "sources": sources,
    }
    return render(request, "georivasources/item_lineage.html", context)
