# Template Conventions (Django/Wagtail HTML)

Conventions for all HTML templates in GeoRiva. Referenced from `CLAUDE.md`.

## General rules

- **No inline styles** — never use `style="..."` attributes. Extract all CSS into a
  `{% block extra_css %}<style>…</style>{% endblock %}` block and give elements semantic class names.
- **Modern JS** — use `const` and `let`; never `var`.
- **JS placement** — all JavaScript goes in `{% block extra_js %}…{% endblock %}` at the bottom of the template. Wrap
  code in `document.addEventListener('DOMContentLoaded', function () { … })` instead of IIFEs `(function(){ … }())`.

## Admin pages: extend `wagtailadmin/generic/base.html`

Every function-based-view admin page extends **`wagtailadmin/generic/base.html`** — never
`wagtailadmin/base.html` with a manual header include. The generic base renders Wagtail's slim header
(breadcrumbs + screen-reader-only `h1`) automatically when `breadcrumbs_items` is in the context.

Do **not** include `wagtailadmin/shared/header.html` yourself: that template silently ignores a
`breadcrumbs` variable (it has a fixed list of accepted variables), which is exactly the bug this
pattern replaced.

### Template contract

```django
{% extends "wagtailadmin/generic/base.html" %}
{% load i18n wagtailadmin_tags %}

{% block titletag %}…{% endblock %}

{% block extra_css %}
    {{ block.super }}
    <style>…</style>
{% endblock %}

{% block main_content %}
    …page body…
{% endblock %}
```

- Body goes in `{% block main_content %}` — the base already wraps it in `<div class="nice-padding">`,
  so never add your own `nice-padding` wrapper.
- Top breathing space is provided globally: an `insert_global_admin_css` hook in
  `core/wagtail_hooks.py` adds `margin-top: 2rem` to the slim-header + bare-`nice-padding` pairing
  (the `w-mt-8` value Wagtail's own pages use). Don't add per-page spacing hacks.

### View context contract

```python
context = {
    "breadcrumbs_items": [
        {"url": reverse("wagtailadmin_home"), "label": _("Home")},
        {"url": reverse("data_feed_list"), "label": _("Data Feeds")},
        {"url": None, "label": current_page_label},   # leaf: url=None, NOT ""
    ],
    "header_title": …,   # slim header's sr-only h1 + titletag fallback
    "header_icon": "cogs",  # icon shown beside the breadcrumbs
    ...
}
```

- **Leaf crumb uses `url: None`** — the breadcrumbs component checks `is not None`, so an empty
  string renders a useless self-link.
- `header_title` should carry what a big header would have shown: combine title and subtitle
  sensibly, e.g. `_("Runs — %s") % product.display_label`.
- Without `breadcrumbs_items` in context the base falls back to the old big header
  (`page_title`/`page_subtitle`) — always provide a trail instead.

### What the slim header cannot do

- **No visible page title** — the page identity is carried by the leaf breadcrumb. If the old
  header's subtitle carried real information, fold it into the leaf crumb label
  (e.g. `"Step 2 of 3 — Feed Details"`) or add a lead line at the top of `main_content`.
- **No action buttons** — `action_url`/`action_text` don't exist here. Put action bars
  (e.g. "New config", "Edit Details") at the top of `main_content` using the
  `button bicolor button--icon` idiom.

Reference examples: `sources/templates/georivasources/derived_product_runs.html` + its view
`sources/views.py:derived_product_runs`, and `visualization/templates/visualization/item_preview.html`.

## CSS variable namespaces

Two separate CSS contexts — use the right tokens for each:

| Context                                                                  | Variables     | Where defined                   |
|--------------------------------------------------------------------------|---------------|---------------------------------|
| **Wagtail admin** templates (`extends "wagtailadmin/generic/base.html"`) | `--w-color-*` | Wagtail 7 admin CSS             |
| **Public-facing** templates (`extends "georiva/base.html"`)              | `--gr-*`      | `config/static/css/georiva.css` |

**Never mix them.** Wagtail 7 removed the old unprefixed `--color-*` aliases entirely — use `--w-color-*` only.

Key `--w-color-*` tokens for admin templates:

- Borders: `--w-color-border-furniture`
- Muted text / secondary: `--w-color-grey-400`
- Subtle backgrounds / Panel headers bg: `--w-color-grey-50`, `--w-color-grey-100`
- Menus: `--w-color-surface-menus`, `--w-color-surface-field`, `--w-color-surface-page`
- Labels / primary text: `--w-color-text-label`
- White: `--w-color-white`
- Status colours: `--w-color-info-100`, `--w-color-positive-100`, `--w-color-warning-100`, `--w-color-critical-200`
- Brand/action colours: `--w-color-primary` (dark indigo — used for structural framing, e.g. the
  derived-products stage brackets), `--w-color-secondary` (teal — used for action links/chips)

Key `--gr-*` tokens for public templates (full list in `georiva.css`):

- Brand accent: `--gr-accent`, `--gr-accent-dark`, `--gr-accent-dim`
- Text on light: `--gr-text-1`, `--gr-text-2`, `--gr-text-3`
- Light surfaces: `--gr-light`, `--gr-light-1`, `--gr-light-2`

`visualization/templates/visualization/item_preview.html` defines its own `--geo-*` variables
locally on top of the generic base — leave them as-is.
