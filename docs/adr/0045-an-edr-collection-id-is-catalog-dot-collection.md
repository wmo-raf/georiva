# An EDR collection id is `{catalog}.{collection}`

## Status

accepted — see wmo-raf/georiva#687, found in the #665 field trial. Corrects one sentence of
[ADR-0010](0010-pinned-collection-bindings-for-derived-products.md) (Considered Options). Keeps
[ADR-0012](0012-self-contained-per-org-api-roots.md)'s rule that the organisation lives in the hostname, never in an
id; qualifies "ids stay bare" for EDR only, by catalog.

## Context

EDR named a collection by its bare `Collection.slug`. A collection slug is unique only within its catalog
(`unique_together = ["catalog", "slug"]`), and a catalog slug only within its organisation. So one organisation can
hold two collections of the same slug — two feeds of one plugin in two catalogs, or, as in the field trial, two
harvests of `noaa-gfs-forecast-virtual` — and `/api/edr/collections/noaa-gfs-forecast-virtual/` was a 500
(`MultipleObjectsReturned`).

ADR-0010 rejected globally unique collection slugs partly because "STAC/EDR URLs already namespace by catalog". STAC
does (`/api/stac/collections/{catalog}/{collection}/…`). EDR did not.

## Decision

1. **An EDR collection id is `{catalog}.{collection}`.** It is the `id` in every EDR response (list and detail) and
   the path segment of every EDR route: the detail document now, and `position`, `area`, `locations` and `instances`
   when they exist. A dot cannot occur in a slug, so the id is unique within an organisation and splits back into
   the pair without ambiguity. `edr/ids.py` spells it; nothing else does — the API, the dataset pages and the
   Connect panel all go through it.

2. **A bare slug still answers while it names one collection.** It redirects (302) to the same path under the
   qualified id, keeping whatever follows the id — a sub-route, a query string — so existing links and clients keep
   working. The redirect is temporary on purpose: the bare slug's answer changes the day a second collection takes
   the same slug, and a client must not have cached a permanent one.

3. **A bare slug naming more than one collection is a 404 that lists them.** The body carries each candidate's
   qualified id, title and href, so a client or a person can choose.

4. **Only what the caller may see counts.** Organisation and visibility are applied before the slug is matched
   (`Collection.objects.visible_to`, then the organisation scope). A private or internal collection the caller may
   not read neither makes a slug ambiguous nor appears in the 404's list; another organisation's collection is never
   reached, whatever its id.

5. **`x-georiva.catalog_slug` and `x-georiva.collection_slug` are unchanged**, so a client that builds STAC, run or
   tile addresses from them keeps doing so.

## Considered Options

- **Collection slugs unique per organisation.** Rejected for the reason ADR-0010 rejected global uniqueness: it would
  stop one plugin running two catalogs of the same collection in an organisation.
- **A 409 on ambiguity alone, ids left bare.** Rejected: it reports the collision and leaves it. Every client of the
  ambiguous slug stays broken until an operator renames something.
- **`{catalog}/{collection}` in the path, as STAC does.** Rejected: an EDR collection id is one path segment, and
  OGC API clients treat it as an opaque id they echo back; a slash in it would not survive that.

## Consequences

- The EDR collection ids every client sees change. Clients that stored the bare slug follow the redirect until the
  slug becomes ambiguous; then they get a 404 that tells them the new ids.
- ADR-0010's sentence "STAC/EDR URLs already namespace by catalog" is now true of EDR too.
