"""The Styling surface (ADR 0022): the one place where styling is tuned.

Two pages: a collection-level listing — every variable with its range and its
default style's swatch, grayscale defaults plainly visible — and the canonical
per-variable form, where ranges are edited, ramps applied, stops fine-tuned and
the style set managed. Every other admin surface only seeds or links here.

The per-variable form carries a live map preview of the latest item (#382), so
the question the operator actually has — does this style read correctly on real
data? — is answered on the page where it is asked rather than somewhere else
after a save. Two seams serve it: :func:`_preview_context`, which finds the item
and the texture to draw it from, and :func:`variable_style_stops`, which
generates a ramp's stops without saving them so the map can show a ramp before
the operator commits to it.
"""

from django import forms
from django.core.exceptions import ValidationError
from django.http import JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils.text import slugify
from django.utils.translation import gettext_lazy as _
from wagtail.admin import messages

from georiva.core.machine_plane import titiler_encoded_preview_url
from georiva.core.models import Collection, ColorRamp, Item, Variable, VariableStyle
from georiva.core.models.visualization import _swatch_html, generate_stops
from georiva.organisations.access import get_org_object_or_404, scoped_queryset

#: The stepped-mode class count preselected for a fresh style (ADR 0022).
DEFAULT_STEPPED_CLASSES = 7

#: What a style-less variable renders as, shown honestly in the listing.
GRAYSCALE_GRADIENT = "linear-gradient(to right, #000000 0%, #ffffff 100%)"

#: The ``?style=`` value naming the blank "new style" pane rather than a saved
#: style — impossible as a real slug, since slugify never yields underscores
#: around a word.
NEW_STYLE = "__new__"

#: Why the map preview has nothing to draw. A fresh feed that has ingested
#: nothing, a variable served as something other than a COG, and an item whose
#: ingest recorded no extent are three different problems with three different
#: fixes, and an operator can only tell which one they have if the panel says
#: which one it is. A single "preview unavailable" would read as a bug in the
#: styling page at exactly the moment it is not one.
PREVIEW_NO_DATA = "no-data"
PREVIEW_NO_BOUNDS = "no-bounds"


def styling_summary(variable):
    """The read-only summary a demoted surface shows (issue #323): the default
    style, its swatch, and the link here — the one place styling is tuned.

    Shared by the collection form's inline variables panel and the
    manual-upload variable forms, so every demoted surface shows the same
    thing the same way.
    """
    default = variable.default_style
    gradient = default.css_gradient() if default else GRAYSCALE_GRADIENT
    return {
        "default_style": default,
        "swatch": _swatch_html(gradient),
        "styling_url": reverse("variable_styling", args=[variable.collection_id, variable.pk]),
    }


class VariableRangeForm(forms.Form):
    """The variable's encoding contract — delegates range sanity to the model."""

    value_min = forms.FloatField()
    value_max = forms.FloatField()
    scale_type = forms.ChoiceField(choices=Variable.ScaleType.choices)

    def clean(self):
        cleaned = super().clean()
        Variable.validate_value_range(cleaned.get("value_min"), cleaned.get("value_max"))
        return cleaned


class VariableStyleForm(forms.Form):
    """The style's own fields; the stops travel beside it as row inputs."""

    name = forms.CharField(max_length=255)
    ramp = forms.ModelChoiceField(queryset=ColorRamp.objects.none(), required=False)
    mode = forms.ChoiceField(choices=VariableStyle.Mode.choices)
    steps = forms.IntegerField(required=False, min_value=2)

    def __init__(self, *args, ramp_queryset, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["ramp"].queryset = ramp_queryset

    def clean(self):
        cleaned = super().clean()
        if cleaned.get("mode") == VariableStyle.Mode.STEPPED and not cleaned.get("steps"):
            self.add_error("steps", _("Stepped mode needs a number of classes."))
        return cleaned


def _stop_rows(post):
    """The submitted stop rows verbatim, fully-empty rows skipped — the one
    reading of the row inputs, shared by parsing and failed-form re-renders."""
    return [
        {"value": value.strip(), "color": color.strip()}
        for value, color in zip(post.getlist("stop_value"), post.getlist("stop_color"), strict=True)
        if value.strip() or color.strip()
    ]


def _parse_stops(post):
    """The submitted stop rows as snapshot entries.

    Only numeric parsing happens here; color shape and value ordering are the
    model's rules and stay there (``VariableStyle.clean()``).
    """
    entries = []
    for row in _stop_rows(post):
        try:
            value = float(row["value"])
        except ValueError:
            raise ValidationError({"stops": _("Not a number: %(value)s") % {"value": row["value"]}}) from None
        entries.append({"value": value, "color": row["color"]})
    return entries


def _unique_style_slug(variable, name):
    """A slug for a new style, unique within the variable.

    The slug is the machine-plane selector (ADR 0023), so it is minted once
    from the name and never edited here.
    """
    base = slugify(name) or "style"
    existing = set(variable.styles.values_list("slug", flat=True))
    slug, n = base, 2
    while slug in existing:
        slug = f"{base}-{n}"
        n += 1
    return slug


def _add_model_errors(form, error):
    """Surface a model ValidationError on the plain form driving it."""
    for field, messages_ in error.message_dict.items():
        target = field if field in form.fields else None
        for message in messages_:
            form.add_error(target, message)


#: The order the ramp picker groups its catalog in (#383). Sequential first
#: because most fields are magnitudes; diverging next because the fields that
#: need it — anomalies, differences — are the ones where picking a sequential
#: ramp by accident actively misleads.
RAMP_TYPE_ORDER = (
    ColorRamp.RampType.SEQUENTIAL,
    ColorRamp.RampType.DIVERGING,
    ColorRamp.RampType.QUALITATIVE,
)


def _ramp_groups(ramp_queryset, selected_ramp_id):
    """The ramp catalog as headed groups for the picker (#383).

    Grouping is by ``ramp_type`` rather than left flat, because the distinction
    is load-bearing rather than decorative: an anomaly field wants a diverging
    ramp and a rainfall total wants a sequential one, and a picker that sorts
    them together leaves that entirely to the operator's memory.

    Each row carries its own gradient so the markup needs no lookup table
    beside it, and the browser can repaint the toggle from the row it was
    given.
    """
    labels = dict(ColorRamp.RampType.choices)
    by_type = {}
    for ramp in ramp_queryset:
        by_type.setdefault(ramp.ramp_type, []).append(
            {
                "pk": ramp.pk,
                "name": ramp.name,
                "gradient": ramp.css_gradient(),
                "owner_label": ramp.owner_label(),
                "selected": ramp.pk == selected_ramp_id,
            }
        )
    return [
        {"label": labels.get(ramp_type, ramp_type), "ramps": by_type[ramp_type]}
        for ramp_type in RAMP_TYPE_ORDER
        if ramp_type in by_type
    ]


def _preview_context(variable):
    """What the map preview draws, or why it draws nothing (#382).

    The texture is Titiler's on-demand ``encoded-preview.png`` — the item's whole
    extent with pixel = rescale(value, vmin→vmax, 0→255), which the browser
    unscales back to physical values and colors client-side. Nothing about the
    style enters this URL, which is the point: recoloring is a palette swap in
    the browser, so a stop edit repaints without a request and without a save.

    That also fixes what the preview can honestly show. The texture is encoded
    against the variable's *saved* range, so a stop beyond it renders as flat
    clipped color; the panel says so rather than letting the operator read the
    clipping as a broken ramp.
    """
    item = Item.objects.latest_for_variable(variable)
    if item is None:
        return {"unavailable": PREVIEW_NO_DATA, "config": None}
    if not item.bounds:
        return {"unavailable": PREVIEW_NO_BOUNDS, "config": None, "item": item}
    return {
        "unavailable": None,
        "item": item,
        "item_url": reverse("item_preview", args=[item.pk]),
        "config": {
            "textureUrl": titiler_encoded_preview_url(item, variable),
            "bounds": list(item.bounds),
            "imageUnscale": [variable.value_min, variable.value_max],
            "unit": variable.units or "",
        },
    }


def collection_styling(request, collection_pk):
    """The collection's Styling page: every variable, range and default swatch."""
    collection = get_org_object_or_404(
        request,
        Collection.objects.select_related("catalog"),
        pk=collection_pk,
    )
    variables = list(collection.variables.select_related("unit").prefetch_related("styles"))
    rows = []
    for variable in variables:
        default = variable.default_style
        rows.append(
            {
                "variable": variable,
                "default_style": default,
                "gradient": default.css_gradient() if default else GRAYSCALE_GRADIENT,
                "style_count": len(variable.styles.all()),
                "url": reverse("variable_styling", args=[collection.pk, variable.pk]),
            }
        )

    context = {
        "breadcrumbs_items": [
            {"url": reverse("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse("catalog:index"), "label": _("Catalogs")},
            {
                "url": reverse("collection_items_list", args=[collection.pk]),
                "label": collection.name,
            },
            {"url": None, "label": _("Styling")},
        ],
        "header_title": _("Styling — %(collection)s") % {"collection": collection.name},
        "header_icon": "palette",
        "collection": collection,
        "rows": rows,
    }
    return render(request, "core/collection_styling.html", context)


def _get_style_or_none(variable, slug):
    if not slug:
        return None
    return next((style for style in variable.styles.all() if style.slug == slug), None)


def variable_styling(request, collection_pk, variable_pk):
    """The canonical per-variable form (ADR 0022): range, ramp, stops, styles."""
    collection = get_org_object_or_404(
        request,
        Collection.objects.select_related("catalog"),
        pk=collection_pk,
    )
    variable = get_org_object_or_404(
        request,
        Variable.objects.select_related("unit").prefetch_related("styles__ramp"),
        pk=variable_pk,
        collection=collection,
    )
    styles = list(variable.styles.all())
    ramp_queryset = scoped_queryset(request, ColorRamp.objects.prefetch_related("stops")).order_by("name")

    form_url = reverse("variable_styling", args=[collection.pk, variable.pk])

    def style_url(style=None):
        if style is None:
            return f"{form_url}?style={NEW_STYLE}"
        return f"{form_url}?style={style.slug}"

    # Which style the editor pane shows: the one asked for, else the default,
    # else the first, else a blank "new style" pane.
    requested_slug = request.POST.get("style_slug", request.GET.get("style", ""))
    creating = requested_slug in ("", NEW_STYLE) if request.method == "POST" else requested_slug == NEW_STYLE
    selected = None if creating else _get_style_or_none(variable, requested_slug)
    if selected is None and not creating:
        if requested_slug:
            messages.error(request, _("No such style."))
            return redirect(form_url)
        selected = variable.default_style or (styles[0] if styles else None)
        creating = selected is None

    range_form = VariableRangeForm(
        initial={
            "value_min": variable.value_min,
            "value_max": variable.value_max,
            "scale_type": variable.scale_type,
        }
    )
    style_form = VariableStyleForm(
        ramp_queryset=ramp_queryset,
        initial={
            "name": selected.name if selected else "",
            "ramp": selected.ramp_id if selected else None,
            "mode": selected.mode if selected else VariableStyle.Mode.CONTINUOUS,
            "steps": (selected.steps if selected else None) or DEFAULT_STEPPED_CLASSES,
        },
    )
    stop_rows = list(selected.stops) if selected else []

    if request.method == "POST":
        action = request.POST.get("action")

        if action == "save-range":
            range_form = VariableRangeForm(request.POST)
            if range_form.is_valid():
                variable.value_min = range_form.cleaned_data["value_min"]
                variable.value_max = range_form.cleaned_data["value_max"]
                variable.scale_type = range_form.cleaned_data["scale_type"]
                # A range edit alone never rewrites any style's stops — the
                # snapshot is the operator's (ADR 0022); regeneration is only
                # ever the explicit re-apply gesture below.
                variable.save(update_fields=["value_min", "value_max", "scale_type", "modified"])
                messages.success(request, _("Range saved."))
                return redirect(style_url(selected))

        elif action == "save-style":
            style_form = VariableStyleForm(request.POST, ramp_queryset=ramp_queryset)
            if style_form.is_valid():
                data = style_form.cleaned_data
                if creating:
                    style = VariableStyle(
                        variable=variable,
                        slug=_unique_style_slug(variable, data["name"]),
                        # The first style is what serves, so it is the default;
                        # later ones stage until explicitly promoted.
                        is_default=not styles,
                    )
                else:
                    style = selected
                style.name = data["name"]
                style.ramp = data["ramp"]
                style.mode = data["mode"]
                style.steps = data["steps"]
                try:
                    stops = _parse_stops(request.POST)
                    if creating and not stops and style.ramp:
                        style.apply_ramp()
                    else:
                        style.stops = stops
                    style.full_clean()
                except ValidationError as error:
                    _add_model_errors(style_form, error)
                else:
                    style.save()
                    messages.success(request, _("Style saved."))
                    return redirect(style_url(style))
            stop_rows = _stop_rows(request.POST)

        elif action == "promote":
            if selected is None:
                messages.error(request, _("No such style."))
                return redirect(form_url)
            selected.promote_to_default()
            messages.success(
                request,
                _('"%(name)s" is now the default style.') % {"name": selected.name},
            )
            return redirect(style_url(selected))

        elif action == "delete-style":
            if selected is None:
                messages.error(request, _("No such style."))
                return redirect(form_url)
            if selected.is_default and len(styles) > 1:
                messages.error(
                    request,
                    _("This is the default style. Promote another style first."),
                )
                return redirect(style_url(selected))
            selected.delete()
            messages.success(request, _("Style deleted."))
            return redirect(form_url)

    # The field's current value, whichever way the form got here: a string on a
    # bound POST, the initial pk otherwise.
    try:
        selected_ramp_id = int(style_form["ramp"].value())
    except (TypeError, ValueError):
        selected_ramp_id = None
    ramp_groups = _ramp_groups(ramp_queryset, selected_ramp_id)
    selected_ramp = next(
        (ramp for group in ramp_groups for ramp in group["ramps"] if ramp["selected"]),
        None,
    )

    context = {
        "breadcrumbs_items": [
            {"url": reverse("wagtailadmin_home"), "label": _("Home")},
            {"url": reverse("catalog:index"), "label": _("Catalogs")},
            {
                "url": reverse("collection_items_list", args=[collection.pk]),
                "label": collection.name,
            },
            {
                "url": reverse("collection_styling", args=[collection.pk]),
                "label": _("Styling"),
            },
            {"url": None, "label": variable.name},
        ],
        "header_title": _("Styling — %(variable)s") % {"variable": variable.name},
        "header_icon": "palette",
        "collection": collection,
        "variable": variable,
        "styles": styles,
        "selected": selected,
        "creating": creating,
        "range_form": range_form,
        "style_form": style_form,
        "stop_rows": stop_rows,
        "ramp_groups": ramp_groups,
        "selected_ramp": selected_ramp,
        "ramp_catalog_url": reverse("colorramp:index"),
        "form_url": form_url,
        "stops_url": reverse("variable_style_stops", args=[collection.pk, variable.pk]),
        "new_style_url": style_url(),
        "grayscale_gradient": GRAYSCALE_GRADIENT,
        "default_stepped_classes": DEFAULT_STEPPED_CLASSES,
        "preview": _preview_context(variable),
    }
    return render(request, "core/variable_styling.html", context)


def variable_style_stops(request, collection_pk, variable_pk):
    """A ramp's stops over the variable's range, generated and *not* saved.

    What the styling form's "Apply ramp" reads. Applying used to be a POST that
    regenerated the snapshot in the database behind a confirm round-trip, so the
    only way to see a ramp on real data was to destroy the tuning it replaced.
    Here the stops come back as JSON, fill the form unsaved, and the map
    repaints — nothing is lost until Save, and a reload undoes it.

    It runs the same :func:`generate_stops` the model's ``apply_ramp()`` does
    over the same saved range, so what the form fills in is exactly what a saved
    re-apply would have produced. Read-only and side-effect-free, hence GET.
    """
    collection = get_org_object_or_404(request, Collection, pk=collection_pk)
    variable = get_org_object_or_404(request, Variable, pk=variable_pk, collection=collection)

    try:
        ramp_pk = int(request.GET.get("ramp", ""))
    except ValueError:
        ramp_pk = None
    ramp = (
        scoped_queryset(request, ColorRamp.objects.prefetch_related("stops")).filter(pk=ramp_pk).first()
        if ramp_pk
        else None
    )
    if ramp is None:
        return JsonResponse({"error": _("Pick a ramp to apply.")}, status=400)

    mode = request.GET.get("mode", VariableStyle.Mode.CONTINUOUS)
    if mode not in VariableStyle.Mode.values:
        return JsonResponse({"error": _("Unknown rendering mode.")}, status=400)

    steps = None
    if mode == VariableStyle.Mode.STEPPED:
        try:
            steps = int(request.GET.get("steps", ""))
        except ValueError:
            steps = 0
        if steps < 2:
            return JsonResponse({"error": _("Stepped mode needs at least two classes.")}, status=400)

    return JsonResponse(
        {
            "stops": generate_stops(ramp, variable.value_min, variable.value_max, mode=mode, steps=steps),
        }
    )
