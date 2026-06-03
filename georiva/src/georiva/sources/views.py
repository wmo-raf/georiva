from django.contrib import messages
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse_lazy, reverse
from django.utils.translation import gettext as _
from wagtail.admin.ui.tables import TitleColumn, Table, ButtonsColumnMixin, BooleanColumn
from wagtail.admin.widgets import HeaderButton, ButtonWithDropdown, Button

from georiva.sources.models import DataFeed
from georiva.sources.registry import data_feed_viewset_registry
from georiva.sources.source import BaseDataSource
from georiva.sources.utils import get_all_child_models, get_child_model_by_name


def data_feed_list(request):
    data_feeds = DataFeed.objects.prefetch_related("collections__catalog").all()
    
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
    from georiva.sources.models import DataFeedRun
    
    feed = get_object_or_404(
        DataFeed.objects.prefetch_related("collections__catalog"),
        pk=pk,
    )
    
    if request.method == "POST" and request.POST.get("action") == "run_now":
        feed.run_now(user=request.user)  # no collection → all collections
        messages.success(request, _("Run started for '%s'.") % feed.name)
        return redirect("data_feed_detail", pk=pk)
    
    recent_runs = (
        DataFeedRun.objects
        .filter(data_feed=feed)
        .select_related("collection")
        .order_by("-started_at")[:20]
    )
    
    context = {
        "breadcrumbs_items": [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse_lazy("data_feed_list"), "label": _("Data Feeds")},
            {"url": "", "label": feed.name},
        ],
        "feed": feed,
        "linked_collections": feed.collections.select_related("catalog").all(),
        "recent_runs": recent_runs,
        "feed_type_name": feed.get_real_instance_class()._meta.verbose_name,
    }
    
    return render(request, "georivasources/data_feed_detail.html", context)


def data_feed_add_select(request):
    wizard_capable_names = {cls.__name__ for cls in _capable_source_types()}
    
    items = []
    for cls in get_all_child_models(DataFeed):
        model_name = cls._meta.model_name
        has_wizard = cls.__name__ in wizard_capable_names
        
        if has_wizard:
            url = reverse("wizard_step1_catalog", kwargs={"model_name": model_name})
        else:
            viewset = data_feed_viewset_registry.get(model_name)
            url = reverse(viewset.get_url_name("add"))
        
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
# Setup Wizard
# =============================================================================

_WIZARD_SESSION_KEY = "georiva_setup_wizard_{model_name}"


def _wizard_session_key(model_name):
    return _WIZARD_SESSION_KEY.format(model_name=model_name)


def _get_data_source_cls(model_cls):
    """
    Return the DataSource class for a DataFeed subclass without a DB instance.

    data_source_cls is a property whose getter only does an import + return, so
    calling it on an uninitialized instance is safe.
    """
    prop = model_cls.__dict__.get('data_source_cls')
    if not isinstance(prop, property):
        return None
    try:
        dummy = object.__new__(model_cls)
        return prop.fget(dummy)
    except Exception:
        return None


def _capable_source_types():
    """Return DataFeed subclasses whose data source implements describe_parameters()."""
    results = []
    for model_cls in get_all_child_models(DataFeed):
        ds_cls = _get_data_source_cls(model_cls)
        if ds_cls is None:
            continue
        if not (isinstance(ds_cls, type) and issubclass(ds_cls, BaseDataSource)):
            continue
        if ds_cls.describe_parameters is BaseDataSource.describe_parameters:
            continue
        results.append(model_cls)
    return results


def _manifest_for_model(model_cls):
    """Instantiate a temporary data source with empty config and return its manifest."""
    ds_cls = _get_data_source_cls(model_cls)
    return ds_cls({}).describe_parameters()


def _wizard_breadcrumbs(model_name, verbose_name, current_label):
    return [
        {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
        {"url": reverse_lazy("data_feed_list"), "label": _("Data Feeds")},
        {"url": reverse("setup_wizard_select"), "label": _("Setup Wizard")},
        {"url": reverse("wizard_step1_catalog", kwargs={"model_name": model_name}), "label": verbose_name},
        {"url": "", "label": current_label},
    ]


def _get_model_or_redirect(request, model_name):
    """Return (model_cls, None) or (None, redirect_response)."""
    model_cls = get_child_model_by_name(DataFeed, model_name)
    if not model_cls:
        messages.error(request, _("Unknown source type: %s") % model_name)
        return None, redirect("setup_wizard_select")
    return model_cls, None


def _collection_preview(manifest, selected_keys, group_into_collections):
    """Return preview list of collections that will be created."""
    from django.utils.text import slugify
    selected_set = set(selected_keys)
    preview = []
    if group_into_collections:
        for group in manifest.groups:
            keys = [k for k in group.member_keys if k in selected_set]
            if keys:
                preview.append({
                    "name": group.name,
                    "slug": slugify(group.key),
                    "variables": [manifest.by_key(k).name for k in keys],
                })
    ungrouped = [k for k in manifest.ungrouped_keys() if k in selected_set]
    if ungrouped:
        preview.append({
            "name": _("Default Collection"),
            "slug": _("(from catalog slug)"),
            "variables": [manifest.by_key(k).name for k in ungrouped],
        })
    return preview


def setup_wizard_select(request):
    """Step 0: pick source type."""
    capable = _capable_source_types()
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
    """Step 1: select an existing Catalog or create a new one."""
    from django.utils.text import slugify
    from georiva.core.models import Catalog
    
    model_cls, err = _get_model_or_redirect(request, model_name)
    if err:
        return err
    
    verbose_name = model_cls._meta.verbose_name
    catalogs = Catalog.objects.order_by("name")
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
    is_edit = session_data.get("is_edit", False)
    edit_catalog = None
    if is_edit and session_data.get("catalog_id"):
        from georiva.core.models import Catalog
        edit_catalog = Catalog.objects.filter(pk=session_data["catalog_id"]).first()

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
        "catalogs": catalogs,
        "catalog_defaults": catalog_defaults,
        "file_format_choices": file_format_choices,
        "is_edit": is_edit,
        "edit_catalog": edit_catalog,
        "step": 1,
    })


def wizard_step2_feed(request, model_name):
    """Step 2: create a new DataFeed or skip."""
    model_cls, err = _get_model_or_redirect(request, model_name)
    if err:
        return err
    
    session_data = request.session.get(_wizard_session_key(model_name), {})
    if not session_data.get("catalog_mode"):
        return redirect("wizard_step1_catalog", model_name=model_name)
    
    verbose_name = model_cls._meta.verbose_name
    existing_feeds = model_cls.objects.order_by("name")
    
    if request.method == "POST":
        feed_mode = request.POST.get("feed_mode", "create")
        new_feed_name = request.POST.get("new_feed_name", "").strip()
        new_feed_interval = int(request.POST.get("new_feed_interval") or 360)
        data_feed_id = request.POST.get("data_feed_id") or None
        
        errors = []
        if feed_mode == "create" and not new_feed_name:
            errors.append(_("Please enter a name for the new Data Feed."))
        
        if errors:
            for e in errors:
                messages.error(request, e)
        else:
            session_data.update({
                "feed_mode": feed_mode,
                "new_feed_name": new_feed_name if feed_mode == "create" else None,
                "new_feed_interval": new_feed_interval,
                "data_feed_id": int(data_feed_id) if feed_mode == "link" and data_feed_id else None,
            })
            request.session[_wizard_session_key(model_name)] = session_data
            return redirect("wizard_step3_parameters", model_name=model_name)
    
    is_edit = session_data.get("is_edit", False)
    edit_feed = None
    if is_edit and session_data.get("data_feed_id"):
        edit_feed = DataFeed.objects.filter(pk=session_data["data_feed_id"]).first()

    return render(request, "georivasources/wizard_step2_feed.html", {
        "breadcrumbs_items": _wizard_breadcrumbs(model_name, verbose_name, _("Data Feed")),
        "model_name": model_name,
        "source_verbose_name": verbose_name,
        "existing_feeds": existing_feeds,
        "default_feed_name": verbose_name,
        "prefill_feed_mode": session_data.get("feed_mode", "create"),
        "prefill_feed_id": session_data.get("data_feed_id"),
        "is_edit": is_edit,
        "edit_feed": edit_feed,
        "step": 2,
    })


def wizard_step3_parameters(request, model_name):
    """Step 3: select parameters and collection grouping."""
    model_cls, err = _get_model_or_redirect(request, model_name)
    if err:
        return err
    
    session_data = request.session.get(_wizard_session_key(model_name), {})
    if not session_data.get("feed_mode") and not session_data.get("catalog_mode"):
        return redirect("wizard_step1_catalog", model_name=model_name)
    
    verbose_name = model_cls._meta.verbose_name
    manifest = _manifest_for_model(model_cls)
    
    if request.method == "POST":
        selected_keys = request.POST.getlist("parameters")
        group_into_collections = request.POST.get("group_into_collections") == "on"
        if not selected_keys:
            messages.error(request, _("Please select at least one parameter."))
        else:
            session_data.update({
                "selected_keys": selected_keys,
                "group_into_collections": group_into_collections,
            })
            request.session[_wizard_session_key(model_name)] = session_data
            return redirect("wizard_step4_review", model_name=model_name)
    
    all_keys = set(manifest.all_keys())
    grouped = []
    covered = set()
    for group in manifest.groups:
        members = []
        for key in group.member_keys:
            if key in all_keys:
                members.append(manifest.by_key(key))
                covered.add(key)
        if members:
            grouped.append({"group": group, "members": members})
    ungrouped = [manifest.by_key(k) for k in manifest.all_keys() if k not in covered]
    
    prefill_keys = set(session_data.get("selected_keys", []))
    prefill_group = session_data.get("group_into_collections", True)

    return render(request, "georivasources/wizard_step3_parameters.html", {
        "breadcrumbs_items": _wizard_breadcrumbs(model_name, verbose_name, _("Parameters")),
        "model_name": model_name,
        "source_verbose_name": verbose_name,
        "grouped": grouped,
        "ungrouped": ungrouped,
        "prefill_keys": prefill_keys,
        "prefill_group": prefill_group,
        "is_edit": session_data.get("is_edit", False),
        "step": 3,
    })


def wizard_step4_review(request, model_name):
    """Step 4: review summary and confirm before provisioning."""
    from georiva.core.models import Catalog
    
    model_cls, err = _get_model_or_redirect(request, model_name)
    if err:
        return err
    
    session_data = request.session.get(_wizard_session_key(model_name), {})
    if not session_data.get("selected_keys"):
        return redirect("wizard_step3_parameters", model_name=model_name)
    
    verbose_name = model_cls._meta.verbose_name
    manifest = _manifest_for_model(model_cls)
    selected_keys = session_data["selected_keys"]
    group_into_collections = session_data.get("group_into_collections", True)
    
    # Catalog summary
    catalog_mode = session_data.get("catalog_mode", "create")
    if catalog_mode == "select":
        catalog_summary = Catalog.objects.filter(pk=session_data["catalog_id"]).first()
        catalog_label = str(catalog_summary) if catalog_summary else "—"
    else:
        catalog_label = f"{session_data.get('new_catalog_name')} ({session_data.get('new_catalog_slug')})"
    
    # Feed summary
    feed_mode = session_data.get("feed_mode", "skip")
    if feed_mode == "create":
        feed_label = f"{session_data.get('new_feed_name')} — every {session_data.get('new_feed_interval', 360)} min"
    elif feed_mode == "link":
        feed_obj = DataFeed.objects.filter(pk=session_data.get("data_feed_id")).first()
        feed_label = str(feed_obj) if feed_obj else "—"
    else:
        feed_label = _("Skip — no Data Feed")
    
    collection_preview = _collection_preview(manifest, selected_keys, group_into_collections)
    
    if request.method == "POST":
        return redirect("wizard_provision", model_name=model_name)
    
    return render(request, "georivasources/wizard_step4_review.html", {
        "breadcrumbs_items": _wizard_breadcrumbs(model_name, verbose_name, _("Review")),
        "model_name": model_name,
        "source_verbose_name": verbose_name,
        "catalog_mode": catalog_mode,
        "catalog_label": catalog_label,
        "catalog_format": session_data.get("new_catalog_format", ""),
        "feed_mode": feed_mode,
        "feed_label": feed_label,
        "selected_count": len(selected_keys),
        "collection_preview": collection_preview,
        "is_edit": session_data.get("is_edit", False),
        "step": 4,
    })


def wizard_provision(request, model_name):
    """Execute provisioning and show the result."""
    from georiva.core.models import Catalog
    from georiva.sources.setup_service import SourceSetupService
    
    model_cls, err = _get_model_or_redirect(request, model_name)
    if err:
        return err
    
    session_data = request.session.get(_wizard_session_key(model_name))
    if not session_data or not session_data.get("selected_keys"):
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
    
    selected_keys = session_data["selected_keys"]
    group_into_collections = session_data.get("group_into_collections", True)
    feed_mode = session_data.get("feed_mode", "skip")
    new_feed_name = session_data.get("new_feed_name")
    new_feed_interval = session_data.get("new_feed_interval", 360)
    data_feed_id = session_data.get("data_feed_id")
    
    data_feed = None
    if feed_mode == "link" and data_feed_id:
        data_feed = get_object_or_404(DataFeed, pk=data_feed_id)
    
    manifest = _manifest_for_model(model_cls)
    service = SourceSetupService()
    try:
        collections, data_feed = service.provision(
            manifest,
            catalog=catalog,
            selected_keys=selected_keys,
            data_feed=data_feed,
            new_feed_name=new_feed_name if feed_mode == "create" else None,
            new_feed_interval=new_feed_interval,
            model_cls=model_cls if feed_mode == "create" else None,
            group_into_collections=group_into_collections,
        )
        request.session.pop(_wizard_session_key(model_name), None)
        
        breadcrumbs = [
            {"url": reverse_lazy("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse_lazy("data_feed_list"), "label": _("Data Feeds")},
            {"url": reverse("setup_wizard_select"), "label": _("Setup Wizard")},
            {"url": "", "label": _("Done")},
        ]
        return render(request, "georivasources/wizard_result.html", {
            "breadcrumbs_items": breadcrumbs,
            "source_verbose_name": model_cls._meta.verbose_name,
            "catalog": catalog,
            "collections": collections,
            "selected_count": len(selected_keys),
            "data_feed": data_feed,
        })
    
    except Exception as exc:
        messages.error(request, _("Provisioning failed: %s") % exc)
        return redirect("wizard_step4_review", model_name=model_name)


def wizard_resume(request, pk, step):
    """
    Pre-populate the wizard session from an existing DataFeed and redirect
    to the requested step.  This enables smart re-run: the user can jump
    directly to any step with all previous choices pre-filled.
    """
    from django.utils.text import slugify
    
    feed = get_object_or_404(
        DataFeed.objects.prefetch_related("collections__variables", "collections__catalog"),
        pk=pk,
    )
    
    real_cls = feed.get_real_instance_class()
    model_name = real_cls._meta.model_name
    
    first_col = feed.collections.select_related("catalog").first()
    catalog = first_col.catalog if first_col else None
    
    selected_keys = [
        var.slug
        for col in feed.collections.prefetch_related("variables").all()
        for var in col.variables.all()
    ]
    
    manifest = _manifest_for_model(real_cls)
    group_keys = {slugify(g.key) for g in manifest.groups} if manifest else set()
    collection_slugs = set(feed.collections.values_list("slug", flat=True))
    
    session = {
        "is_edit": True,
        "catalog_mode": "select" if catalog else "create",
        "catalog_id": catalog.pk if catalog else None,
        "feed_mode": "link",
        "data_feed_id": feed.pk,
        "new_feed_interval": feed.interval_minutes,
        "selected_keys": selected_keys,
        "group_into_collections": bool(collection_slugs & group_keys),
    }
    request.session[_wizard_session_key(model_name)] = session
    
    step_urls = {
        "catalog": reverse("wizard_step1_catalog", kwargs={"model_name": model_name}),
        "feed": reverse("wizard_step2_feed", kwargs={"model_name": model_name}),
        "parameters": reverse("wizard_step3_parameters", kwargs={"model_name": model_name}),
        "review": reverse("wizard_step4_review", kwargs={"model_name": model_name}),
    }
    return redirect(step_urls.get(step, step_urls["catalog"]))
