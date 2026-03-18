# datetime-selector

Hierarchical datetime selector for time-series data. No framework required — vanilla JS.

Two build targets:
- **`dist/es/`** — ES module, dayjs external. For Vue/Vite/webpack projects.
- **`dist/umd/`** — UMD, dayjs bundled in. For Django templates and plain script tags.

---

## Usage

### Vue / Vite project

Install as a local package:

```bash
npm install ../path/to/datetime-selector
```

```js
import DateTimeSelector from 'datetime-selector';
import 'datetime-selector/style';
```

In a Vue component:

```vue
<template>
  <div id="my-selector"></div>
</template>

<script setup>
import { onMounted, onUnmounted } from 'vue';
import DateTimeSelector from 'datetime-selector';
import 'datetime-selector/style';

let selector;

onMounted(() => {
  selector = new DateTimeSelector('my-selector', dates, {
    onChange: (iso, index) => console.log(iso),
  });
});

onUnmounted(() => selector?.destroy());
</script>
```

---

### Django template

Copy `dist/umd/datetime-selector.js` and `dist/umd/datetime-selector.css`
into your app's `static/` directory, then:

```html
{% load static %}
<link rel="stylesheet" href="{% static 'datetime-selector.css' %}">
<script src="{% static 'datetime-selector.js' %}"></script>

<div id="my-selector"></div>

<script>
  const selector = new DateTimeSelector('my-selector', dates, {
    onChange: function(iso, index) { console.log(iso); },
  });
</script>
```

No CDN, no import maps, no build step needed in the template.

---

## API

### Constructor

```js
new DateTimeSelector(elementId, availableDates, options)
```

| Parameter | Type | Description |
|---|---|---|
| `elementId` | `string` | ID of the DOM element to mount into |
| `availableDates` | `string[]` | Array of ISO 8601 UTC strings |
| `options.selectedDate` | `string` | ISO string to select on init (default: latest) |
| `options.onChange` | `function` | Called with `(isoString, index)` on every change |

### Methods

| Method | Description |
|---|---|
| `setDates(dates, selectedDate?)` | Replace available dates without reinstantiating |
| `setDate(isoString)` | Jump to a specific date programmatically |
| `getDate()` | Returns the currently selected ISO string |
| `previous()` | Step to the previous available date |
| `next()` | Step to the next available date |
| `destroy()` | Tear down the component and remove all listeners |

---

## Picker behaviour

- **≤ 12 timestamps** → flat list
- **> 12 timestamps** → hierarchical drill-down: Year → Month strips → Calendar → Hour buckets → Time list
- Picker remembers last viewed position and restores it on reopen
- Dropdown width always matches the selector bar
- Full dark mode support via `prefers-color-scheme`

---

## Build

```bash
npm install
npm run build       # builds both targets
npm run build:es    # ES module only  → dist/es/
npm run build:umd   # UMD bundle only → dist/umd/
npm run dev         # ES watch mode
```

| File | Raw | Gzip |
|---|---|---|
| `dist/es/datetime-selector.js` | 13 kB | 3.7 kB |
| `dist/umd/datetime-selector.js` | 21 kB | 7.2 kB |
| `datetime-selector.css` | 5.7 kB | 1.4 kB |

The UMD bundle is larger because dayjs is included.
