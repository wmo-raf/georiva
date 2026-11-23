# A store collection's vector Variable is computed from its u and v when read

## Status

accepted — see wmo-raf/georiva#756. Extends [ADR-0044](0044-an-upstream-store-is-harvested-step-by-step-and-titiler-reads-the-store.md)
§3 (a Store asset names an array) and §9 (Titiler reads the store), and
[ADR-0046](0046-values-are-stored-as-produced-and-converted-when-read.md) §10 (one read module). Nothing is reversed.

## Context

On a COG collection, wind speed and direction are Variables with `transform_type` `vector_magnitude` /
`vector_direction` and two sources, `u_component` and `v_component`, naming the bands ingestion reads to compute them
(`ingestion/extractor.py`). The bytes written are the computed field.

A store harvest (ADR 0044) runs no extraction. It maps one upstream array to one Variable, and a Store asset names
one array. So a harvested store such as dynamical.org's AIFS, which holds `wind_u_10m` and `wind_v_10m`, could never
carry wind speed or direction: its map and point readers saw only the components.

ADR 0046's rule is that values are stored as produced and converted when read. A vector's magnitude and direction are
the same kind of thing as a unit conversion: a function of bytes that stay upstream, applied on the way out.

## Decision

**A store collection may carry a vector Variable over two of its mapped arrays. It has a Store asset of its own at
every step where both are held, and its values are computed from the two when it is read.**

### Declaring one

1. **Declared as a file-read wind Variable is.** `transform_type` is `vector_magnitude` or `vector_direction`, and the
   Variable's `sources` hold a `u_component` and a `v_component` whose `source_name` is an array's path in the store.
   No new column: in a file it is the name of a band, here the path of an array. `Variable.vector_components` reads
   them back.

2. **Both parts must be mapped arrays of the feed.** `Variable.clean()` refuses a vector Variable of a store collection
   that lacks either part, or names an array the feed's Variable mapping does not hold
   (`Collection.store_mapping`), or whose parts are stored in different units; a speed is stored in its parts' unit.
   The vector itself is not mapped: it is no array of the store. A Variable the feed does map shows its array as it
   is and takes no transform, and the feed's form refuses to unmap an array a vector names.

3. **The collection editor offers the transform and its sources on a store collection's Variables**, which other
   harvested and derived collections still hide.

4. **The Harvest Feed Wizard offers it.** When two fields chosen in step 4 are a wind's u and v — paths alike word
   for word but one, `u` in one where the other has `v` (`wind_u_10m` / `wind_v_10m`, `10u` / `10v`) — it offers to
   show the wind's speed and the direction it blows from too, each unticked until the operator ticks it. A speed is
   saved in its parts' units, coloured from 0 to the furthest either part's range reaches; a direction in degrees,
   0–360.

### Indexing it

5. **A vector Variable has a Store asset wherever both its arrays are held.** The harvest adds it beside its parts on
   the step's item: the store's href, `format` Zarr, and an empty `store_array` — it names no array of its own; its
   Variable names two. So STAC, the runs API and WMTS list it like any Variable, and nothing is computed until it is
   read. The timeseries API offers no store Variable yet, vector or not (wmo-raf/georiva#780).

6. **It is judged with its parts.** It counts as mapped: a step where one of its parts is no longer held loses its
   row, a run closes only when the vector holds every step both its parts are expected at, and a vector declared
   after its runs closed is completed on the next harvest, as a newly mapped Variable is (ADR 0044 §6). A vector over
   an array the feed does not map is left out of the harvest, with a warning.

### Reading it

7. **The read module computes it** (ADR 0046 §10). `values.session` reads a vector Store asset as the speed
   (`hypot(u, v)`) or the bearing the wind blows from (`atan2(u, v) + 180°`, `geoprocessing.wind`) of its two fields,
   both read at the same step from the one opening of the store the session holds. The field is then converted to the
   unit asked for like any other: a speed in m/s reads in km/h on request.

8. **Titiler computes it too.** The entry Django publishes for a vector Store asset names `vector: {u, v, transform}`
   in place of `array`. Titiler reads both fields through its open store and field cache, computes the same quantity
   with the same arithmetic, and keeps the result as it keeps any field. Its warm-up opens both arrays. An entry naming
   a transform it does not know, or lacking either part, is refused as misdescribed.

9. **STAC names the parts.** A vector Store asset carries `georiva:vector` (`u`, `v`, `transform`) in place of
   `georiva:array`, so a consumer's own tools can compute the same field.

## Consequences

- A harvested store can show wind speed and direction on every read plane with no bytes written.
- Changing a vector Variable's sources after a harvest changes what its existing rows mean, as changing its served
  unit does: they are read through the Variable, not frozen into the row.
- A vector field costs both parts' decodes the first time a session or a Titiler worker reads it. Both parts are then
  held, so a map of u, v and speed at one step decodes each part once.
- The two arithmetic implementations — core's `geoprocessing.wind` and Titiler's `app/store.py` — must agree; each
  side's tests pin the same 3-4-5 values.
- Boundary statistics, virtual-Zarr manifests and the timeseries API still do not follow a Store asset (ADR 0044),
  vector or not; the last is wmo-raf/georiva#780.

## Out of scope

- Vector Variables over a Remote COG harvest, whose Upstream already publishes one file per quantity.
- Vectors whose parts are on different grids or levels; the reader refuses parts whose grids differ.
- Any other derived quantity over a store's arrays (a difference, a sum, a de-accumulation).

## Alternatives considered

- **Two foreign keys to the component Variables.** A migration, and the parts named as Variables when the store
  names them as arrays; the existing `sources` already says "computed from these two".
- **A second key on the Variable mapping row.** Puts a Variable's meaning on the feed, where the collection editor
  cannot show it.
- **No asset for the vector; readers resolve it from its parts' assets.** Every surface that lists a Variable's
  assets — STAC, runs, WMTS dimensions — would need to learn the rule.
- **Compute and store the vector at harvest.** A store harvest reads no pixels (ADR 0042 §10), and the bytes would no
  longer be "as produced".
