# WMTS legacy-client validation runbook

**For:** [#364](https://github.com/wmo-raf/georiva/issues/364) — the human-in-the-loop
sign-off on the org-wide WMTS service ([#354](https://github.com/wmo-raf/georiva/issues/354)).

Slices #356–#363 are built and covered by automated tests. What no test on this instance
can tell us is whether the software a National Meteorological Service actually runs will
*read* it. That is what this session is for: two real clients, one paste-able URL each, and
an honest record of what they do.

**Time:** about 90 minutes, plus install time if QGIS or the legacy client is not already
on the machine.

**The deliverable is the findings, not a pass.** A client that cannot step time or cannot
identify is a successful session — it is exactly what #364 exists to discover. File it and
move on. Do not fix anything during the session; note it and keep testing, or you will lose
the thread.

---

## 0. Before you start

You need an organisation on the instance with **real data**, not an empty shell. Specifically:

| Need | Why | How to check |
|---|---|---|
| A **forecast** collection (items carry a `reference_time`) | It is the only thing that exercises the two-dimension `Time` + `Reftime` layer shape | Its layers show both dimensions in the capabilities document |
| An **observation** collection (items have no `reference_time`) | Proves a dimension-ignorant client is not asked for a run that does not exist | Its layers show `Time` only |
| A **public** collection | The anonymous path | Visible without a key |
| A **private** collection | The API-key path — criterion 3 cannot be tested without one | Invisible without a key, visible with one |
| A variable with **two or more named styles** | Style switching | The layer lists more than one `<Style>` |

If any row is missing, ingest or configure it first. A session run against one public
collection proves almost nothing.

Fill these in and use them throughout:

```
HOST=<the organisation's own hostname, e.g. kenya.georiva.example>
ORG=<the organisation slug, e.g. kenya>
KEY=<a personal API key, see below>
```

> **The host is the authority.** The organisation appears twice — in the hostname and in the
> URL path — and they must agree. Dialling `HOST` for one organisation with another's slug
> in the path is refused (ADR 0013/0015), and §5 tests that on purpose. Use the org's own
> hostname, not the instance's bare domain.

### Get an API key

Sign in to the admin as a **member of the organisation** whose private collection you want
to see, then **Settings ▸ API keys** (`/admin/api-keys/`) and create one. The secret starts
with `grv_` and is shown **once** — copy it immediately.

Membership is what widens the view, not the key itself: a key belonging to someone outside
the organisation authenticates fine and still sees only public layers. If §3 shows no
private layers, check the membership before suspecting the key.

---

## 1. Pre-flight (5 minutes, terminal only)

Do this before opening any GUI. If the service is not answering here, a client's silence
will tell you nothing about why.

```bash
# The one URL a legacy client is given. Should be XML beginning <Capabilities>.
curl -s "https://$HOST/titiler/$ORG/wmts?SERVICE=WMTS&VERSION=1.0.0&REQUEST=GetCapabilities" | head -5

# How many layers, and how big the document is (see §6 — this is a known risk).
curl -s "https://$HOST/titiler/$ORG/wmts?SERVICE=WMTS&VERSION=1.0.0&REQUEST=GetCapabilities" \
  | tee /tmp/caps.xml | grep -c "<Layer>"
wc -c /tmp/caps.xml

# The same document on the metadata plane, for tooling that does not speak KVP.
curl -s "https://$HOST/api/wmts/$ORG/WMTSCapabilities.xml" | head -5
```

Pick one layer identifier out of `/tmp/caps.xml` to use for the rest of the session — they
are spelt `catalog:collection:variable`:

```bash
grep -o "<ows:Identifier>[^<]*:[^<]*:[^<]*</ows:Identifier>" /tmp/caps.xml | head
```

Note its advertised `<Default>` values for `Time` and (if present) `Reftime`; you will check
them again in §4. Keep both to hand — later sections use them:

```
LAYER=<catalog:collection:variable>
TIME=<the layer's advertised Time default, e.g. 2026-03-01T12:00:00Z>
```

### The two URLs to hand a client

| Binding | URL |
|---|---|
| **KVP** (paste this into legacy clients and QGIS) | `https://$HOST/titiler/$ORG/wmts` |
| **REST capabilities** (tooling, curl, debugging) | `https://$HOST/api/wmts/$ORG/WMTSCapabilities.xml` |

With a key, append `?api_key=$KEY` to either. Both spellings then advertise URLs that carry
the key onward, which is the whole point of §3.

### What the service does and does not offer

Worth knowing before a client's error message confuses you — each of these is deliberate,
and a client refusing over one is a finding, not a bug in your setup:

- **TileMatrixSet:** `WebMercatorQuad` only.
- **Format:** `image/png` only.
- **InfoFormat:** `application/json` only ← **the biggest known risk, see §6.**
- **Dimensions:** `Time`, and `Reftime` on forecast layers only.
- **Style:** an empty `STYLE=` or `STYLE=default` both mean the layer's real default.

---

## 2. QGIS

> Criterion: *"QGIS: discovers layers, renders tiles, steps time, switches styles and runs
> identify from the one URL."*

Tested on QGIS 3.28 LTR or newer. Record the exact version you used.

1. **Connect.** Layer ▸ Add Layer ▸ Add WMS/WMTS Layer… ▸ **New**. Name it, paste the **KVP
   URL** from §1, **Connect**.
   - ☐ Does the layer list populate? How many layers, and does it match the pre-flight count?
   - ☐ Are layer titles readable (`Collection — Variable`) or bare identifiers?
2. **Render.** Add one layer to the map and zoom to its extent.
   - ☐ Do tiles draw? At which zoom levels? Note any that are blank or stalled.
   - ☐ Is the extent right, or does the layer land in the wrong place?
3. **Step time.** With a **forecast** layer, find the temporal controls (Layer Properties ▸
   Temporal, and the Temporal Controller panel).
   - ☐ Does QGIS expose the `Time` dimension at all?
   - ☐ Does it expose `Reftime`? *(This is where an arbitrary second dimension is most
     likely to be dropped — record precisely what you see.)*
   - ☐ Stepping time: do the tiles actually change?
4. **Switch styles.** On a variable with two or more styles, find the style picker for the
   WMTS layer.
   - ☐ Are the named styles listed, with the right one pre-selected as default?
   - ☐ Does picking another visibly change the rendering?
5. **Identify.** Select the Identify tool and click inside the layer's extent.
   - ☐ Does the Identify tool work on this layer at all, or is it greyed out?
   - ☐ If it returns something, is the value physical (in the variable's units) rather than
     a 0–255 pixel value?
   - ☐ Click a nodata area (sea beside a land-only grid): does it return empty cleanly, or
     does QGIS mark the layer broken?

---

## 3. Private layers with an API key

> Criterion: *"Private layers work end-to-end with an API key in a client that cannot edit
> URL templates."*

This is the criterion most likely to fail in a way that matters, because it depends on the
key being written into every URL *inside* the document — a legacy client can fill in
placeholders but cannot append parameters to a URL it was handed.

1. **Anonymous baseline.** Connect a fresh QGIS connection to the plain KVP URL.
   - ☐ Confirm the private collection's layers are **absent**.
2. **Keyed.** New connection, this time `https://$HOST/titiler/$ORG/wmts?api_key=$KEY`.
   - ☐ Do the private layers now appear?
   - ☐ **Do their tiles draw?** This is the real test — discovery succeeding while tiles
     404 means the key reached the document but not the tile URLs inside it.
3. **The propagation check.** Confirm the key is genuinely travelling inside the document
   rather than being re-appended by QGIS:

   ```bash
   curl -s "https://$HOST/api/wmts/$ORG/WMTSCapabilities.xml?api_key=$KEY" \
     | grep -o 'api_key=[^&"<]*' | sort -u | head
   ```

   - ☐ Every advertised URL (ResourceURL templates and the KVP endpoint) carries the key.
4. **Expiry behaviour.** Revoke the key in the admin, then reload tiles in the still-open
   QGIS session.
   - ☐ What does QGIS show the user? Does anything indicate a credential problem, or does
     the layer just silently stop drawing? *(Since #372 the server answers a parseable
     `401` ExceptionReport — this step is about whether the client surfaces it.)*

---

## 4. Dimension defaults

> Criterion: *"Dimension defaults give a dimension-ignorant client coherent latest-forecast
> tiles."*

The rule the service implements: `Reftime` lists every run newest-first and defaults to the
**newest**; `Time` lists **that run's** valid times and defaults to its **first** (the
analysis). So a client that ignores dimensions entirely gets one coherent latest forecast,
never a valid time from one run against another run's reference.

1. **Read the defaults** out of `/tmp/caps.xml` for a forecast layer and confirm they follow
   that rule against the actual newest run in the admin.
   - ☐ `Reftime` default is the newest run present.
   - ☐ `Time` default belongs to *that* run.
2. **Behave like a dimension-ignorant client** — ask for nothing:

   ```bash
   curl -s -o /tmp/tile.png -w '%{http_code} %{content_type} %{size_download}\n' \
     "https://$HOST/titiler/$ORG/wmts?SERVICE=WMTS&VERSION=1.0.0&REQUEST=GetTile\
&LAYER=$LAYER&STYLE=&TILEMATRIXSET=WebMercatorQuad&TILEMATRIX=6&TILEROW=32&TILECOL=38&FORMAT=image/png"
   ```

   **Expect a `400` `MissingParameterValue` with locator `TIME`** — that is the current
   behaviour, not a misconfiguration. The two bindings differ here, and the difference is
   worth understanding before you judge the criterion:

   - **REST** carries the defaults *into* the URL: the `ResourceURL` template contains a
     `{Time}` placeholder, and a client substitutes the advertised `<Default>` into it. The
     defaults do their job.
   - **KVP** requires `TIME` explicitly. A KVP client that sends no dimension at all gets a
     `400` rather than the default tile.

   So the criterion is satisfied for a client that reads dimension defaults and fills them
   in, and **not** for one that ignores dimensions entirely on the KVP binding.

   - ☐ Which kind is the legacy client in §7? If it sends no `TIME` and fails, that is a
     **finding to file** — the fix would be for the KVP shim to fall back to the advertised
     default, which needs Django's knowledge of what that default is. Record it; do not
     implement it here.
3. **Observation layer.** Check one in the document.
   - ☐ It advertises `Time` only, with **no** `Reftime`, and the default is the newest time.

---

## 5. Exceptions

> Not a criterion of its own, but story 17/18 of #354 — and the recent fix in
> [#372](https://github.com/wmo-raf/georiva/issues/372) — turn on whether clients can read
> these at all.

Each of these should return an **OGC `ExceptionReport` XML** body, never HTML and never JSON:

Two different parts of the system answer these, and it is worth knowing which, because
their reports differ in how much they say:

```bash
BASE="https://$HOST/titiler/$ORG/wmts?SERVICE=WMTS&VERSION=1.0.0"
TILE="&TILEMATRIXSET=WebMercatorQuad&TILEMATRIX=6&TILEROW=32&TILECOL=38&FORMAT=image/png"

# (a) Unknown STYLE on a real layer → the Titiler shim's own report, which names what
#     was wrong. Use the layer identifier from §1.
curl -s "$BASE&REQUEST=GetTile&LAYER=$LAYER&STYLE=no-such-style$TILE&TIME=$TIME" | head -3

# (b) Unknown layer → caught by the auth gate before the shim (it is not a layer you may
#     see), so this returns the gateway's deliberately vaguer report.
curl -s "$BASE&REQUEST=GetTile&LAYER=no:such:layer&STYLE=$TILE&TIME=$TIME" | head -3

# (c) Broken key → 401 + report   (gateway; fixed in #372)
curl -s -i "$BASE&REQUEST=GetCapabilities&api_key=grv_definitely-not-a-key" | head -3

# (d) Wrong organisation for this host → 404 + report   (gateway; fixed in #372)
curl -s -i "https://$HOST/titiler/some-other-org/wmts?SERVICE=WMTS&VERSION=1.0.0&REQUEST=GetCapabilities" | head -3
```

- ☐ All four are `application/xml` with an `<ExceptionReport>` body — none is HTML or JSON.
- ☐ (b) and (d) say nothing about *why*. That is deliberate: the gate answers "absent",
  "wrong host" and "not yours" identically so the private tier gives nothing away
  (ADR 0014). Do not file it as a vagueness bug.
- ☐ Then check what the **clients** do with them: does QGIS or the legacy client surface the
  `ExceptionText`, or just fail silently? *That* is the finding worth recording.

---

## 6. The two known risks — probe these deliberately

#354 called both out in advance. Confirm or clear each.

### Risk 1 — JSON-only InfoFormat may disable legacy identify tools

The service advertises `application/json` as the **only** `InfoFormat`. Older clients
commonly expect `text/plain`, `text/html` or `application/vnd.ogc.gml`, and a client that
recognises none of the offered formats typically **hides its identify tool entirely** rather
than reporting anything.

- ☐ In each client: is identify available, and does it show a usable value?
- ☐ If it is unavailable, record the client's exact version and behaviour. That evidence is
  what would justify adding a `text/plain` or GML InfoFormat — file it as a follow-up, do
  not implement it here.

### Risk 2 — capabilities-document size on deep archives

Time values are enumerated **completely and on purpose** (#354's accepted trade-off), so the
document grows with the archive. On a deep archive it may become slow or large enough to
choke an old parser.

- ☐ Record the byte size and layer count from §1.
- ☐ Record how long the connect step took in each client (wall clock is fine).
- ☐ If it is already uncomfortable, note the archive depth that produced it — that number is
  the useful part of the finding.

> The anonymous document is cached in Redis for `GEORIVA_WMTS_CAPABILITIES_CACHE_SECONDS`
> (default **300**), so a repeat connect will be faster than the first. Time the **first**
> one after a data change, or you will measure the cache.

---

## 7. A legacy KVP client

> Criterion: *"A legacy KVP client: discovers layers and renders tiles from the same URL."*

Use the software the target NMHS actually runs — that is the point, and it outranks any
substitute. Record its **name and version** prominently; a finding is only actionable if we
know what it came from.

If no target client is available, note that explicitly in the results and use the closest
available stand-in (an ArcMap-era desktop client, or another OGC desktop viewer of that
generation). Say which, and mark the criterion **partially met** — do not quietly let a
second modern client stand in for a legacy one.

- ☐ Paste the **KVP URL**. Does it discover layers?
- ☐ Do tiles render?
- ☐ Anything from §2's steps 3–5 that this client also supports — record what it does.
- ☐ If it refuses at any point, capture the **exact error text** and, if you can, the request
  it sent (a proxy log or the client's own log).

---

## 8. Record the results

Fill this in and paste it into #364 as a comment.

```markdown
## Validation session — <date>, <who>

Instance: <host>   Organisation: <slug>
QGIS: <version>    Legacy client: <name + version, or "none available — used X">

| # | Criterion | Result | Notes |
|---|---|---|---|
| 1 | QGIS discovers, renders, steps time, switches styles, identifies | pass / partial / fail | |
| 2 | Legacy KVP client discovers layers and renders tiles | pass / partial / fail | |
| 3 | Private layers end-to-end with an API key | pass / partial / fail | |
| 4 | Dimension defaults give coherent latest-forecast tiles | pass / partial / fail | |
| 5 | Findings filed as follow-up issues | list, or "none" | |

Risk 1 (JSON-only InfoFormat): confirmed / cleared — <evidence>
Risk 2 (document size): <bytes>, <layer count>, connect took <time> — concerning? y/n
```

### Filing follow-ups

One issue per finding, each independently actionable. Include the client name and version,
what you did, what happened, and what you expected — a finding without the client version is
not reproducible.

Use the canonical labels (see `docs/agents/triage-labels.md`) and reference #364 as the
parent. **"None" is a legitimate and valuable result** — record it explicitly rather than
leaving the criterion blank, so the next person knows it was checked and not skipped.
