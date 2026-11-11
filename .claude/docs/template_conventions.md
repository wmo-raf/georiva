# Template Conventions (Django/Wagtail HTML)

Conventions for all HTML templates in GeoRiva. Referenced from `CLAUDE.md`.

## General rules

- **No inline styles** — never use `style="..."` attributes. Extract all CSS into a
  `{% block extra_css %}<style>…</style>{% endblock %}` block and give elements semantic class names.
- **Multi-line comments use `{% comment %}`** — the `{# … #}` syntax is for single-line comments only.
  When a comment spans multiple lines, always use a `{% comment %}…{% endcomment %}` block:

  ```django
  {% comment %}
      Always rendered, whatever the ladder concluded — nothing else
      reports which broker stack an installation runs.
  {% endcomment %}
  ```

  **This applies to the HTML only.** Inside executable `<script>` and `<style>` blocks, use the
  language's own comments — `/* … */` in CSS, `/** … **/` or `//` in JS. A `{% comment %}` there does
  work at render time, but editors and formatters read those blocks as JS/CSS, cannot parse the tag,
  and mangle the code that follows it.

  **Data blocks are the exception, and go back to `{% comment %}`.** A
  `<script type="application/json">` payload is JSON, which has no comment syntax at all — `//` or
  `/* … */` inside one is a parse error that takes the page's JS down with it. Django strips
  `{% comment %}` before the browser sees the block, so it is the only way to annotate one. See
  `pages/datasets/templates/datasets/item_detail.html`'s `grItemConfig`.
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
        {"url": None, "label": current_page_label},  # leaf: url=None, NOT ""
    ],
    "header_title": …,      # slim header's sr-only h1 + titletag fallback
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

## Use Wagtail's styleguide for UI markup

Before writing markup for a button, icon, status tag, listing, form field or other UI element, look at Wagtail's
styleguide and copy its markup and template tags. Don't hand-roll a component, or write custom CSS, for something it
already has.

- Live: `/admin/styleguide/` (the `wagtail.contrib.styleguide` app, enabled in `config/settings/dev.py` only).
- Source, for when there is no browser: `wagtail/contrib/styleguide/templates/wagtailstyleguide/base.html` in the
  installed Wagtail package (`just shell`, then `python -c "import wagtail.contrib.styleguide as m; print(m.__file__)"`).
- Buttons: `button` + `button-small` / `button-secondary`; with an icon, `button bicolor button--icon` and
  `{% icon name="plus" wrapped=1 %}` — the `wrapped=1` is what spaces the icon from its label. Never `<input type="button">`.
- Tags: `{% status label="…" classname="w-status--primary" %}`. `w-status-tag` has no styles in Wagtail 7.

## Panel vs. card

- **Panel** — a titled region of a page: a bordered, rounded block with a header strip (title, a muted note,
  right-aligned actions) and a body below it. It groups and labels related content. The reference is the
  dashboard's "Recent failures" (`gr-failures` in `events/static/georivaevents/history.css`); the item-preview page's
  "Item details" (`gr-item-panel`) copies its shape. Also `vs-panel`, `vz-section`.
- **Card** — a compact, self-contained summary of one thing, usually one of several in a grid or list (a catalog,
  a feed, an item). No header strip. E.g. `gr-catalog-card`, `gfeed-card`, `core/card_view.html`.

A header strip plus a body is a panel; a compact summary of one entity in a listing is a card. Name new classes
accordingly (`gr-<name>-panel`, `gr-<name>-card`), and copy the panel shape into the page's own `<style>` block
rather than reusing another app's stylesheet. This is the project's own vocabulary — it is **not** Wagtail's
`{% panel %}` / `w-panel` component, which is a collapsible block used on editor screens.

## CSS variable namespaces

Two separate CSS contexts — use the right tokens for each:

| Context                                                                  | Variables     | Where defined                   |
|--------------------------------------------------------------------------|---------------|---------------------------------|
| **Wagtail admin** templates (`extends "wagtailadmin/generic/base.html"`) | `--w-color-*` | Wagtail 7 admin CSS             |
| **Public-facing** templates (`extends "georiva/base.html"`)              | `--gr-*`      | `config/static/css/georiva.css` |

**Never mix them.** Wagtail 7 removed the old unprefixed `--color-*` aliases entirely — use `--w-color-*` only.

Key `--w-color-*` tokens for admin templates:

- Borders: `--w-color-border-furniture`
- Muted text / secondary: `--w-color-text-meta`
- Panel header background: `--w-color-surface-header` (not the grey scale — see *Theme-aware colours*)
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

## Theme-aware colours (light and dark)

Every admin page must work in both the light and dark theme. Colour comes from `--w-color-*` semantic tokens,
never from fixed values.

- **No fixed colours** for surfaces, text or borders in admin templates: no hex, `rgb()`, or named colours.
- **Never the `--w-color-grey-*` scale for backgrounds or text.** The scale is the same in both themes (`grey-50` stays
  near-white), so in dark mode a `grey-50` panel header is a light bar under light text. Use the tokens that flip with the theme:
  `--w-color-surface-header`, `--w-color-surface-page`, `--w-color-text-meta`, `--w-color-border-furniture`, …
- **Nor `--w-color-white` / `--w-color-black` for a surface.** They don't flip either: a white panel body is
  `--w-color-surface-page`. White *text* on a filled colour that doesn't flip (a status chip, an active step) is fine.
- **Neutral pills and badges** take the same shape as the coloured ones beside them:
  `color-mix(in srgb, var(--w-color-text-meta) 15%, transparent)`. Neutral marks (rails, stripes, dots) are
  `--w-color-icon-secondary`; a grey chip with white text is `--w-color-surface-button-inactive`.
- **Enforced** by `core/tests/test_admin_theme_colours.py`, which scans admin templates, app stylesheets and the
  dashboard's Vue sources. An intentional exception goes in its pinned `ALLOWED` count, with the comment beside it.
- **Page-local variables** (`--geo-*` and the like) are fine, but define them *from* `--w-color-*` tokens, not hex.
- **A tint or hover with no semantic token:** mix one in, e.g.
  `color-mix(in srgb, var(--w-color-text-label) 6%, var(--w-color-surface-header))`.
- **Allowed exceptions**, each with a one-line comment saying why: colours that are data (palettes, legends, chart
  series), overlays that sit on a map and are dark by design, and elements inside the always-dark sidebar.
- **The public site is out of scope.** It uses the separate `--gr-*` system and has no theme switch.
- **Done means checked in both themes.** In dev, swap the `<html>` class `w-theme-system` for `w-theme-dark` in the
  console, and include both in PR evidence for a UI change.
