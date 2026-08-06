# Retire the Monitoring menu and the org-wide Acquisition Feed

## Status

accepted

## Context

The admin sidebar carried a "Monitoring" submenu holding two org-wide live
pages: **Ingestion Activity** (a chronological SSE feed of `FileIngestion` and
job events) and **Acquisition Feed** (the same shape for `FetchRun` and
`UploadSession` events, per ADR 0001 and ADR 0003).

Since then, monitoring has moved to where the objects live. Each Data Feed's
dashboard carries acquisition and ingestion stat cards with linked per-feed
fetch-run and ingestion listing pages — the surface an operator investigating a
feed actually uses. The Collection Health Panel on the admin homepage gives the
fleet-level "is anything unhealthy" view with per-collection drill-downs
(including `UploadSession` history), and the Data Feeds list shows per-feed
health chips. The manual upload page streams its own in-flight progress over
the ingestion SSE stream.

That left the two Monitoring pages overlapping the per-feed surfaces. The
Acquisition Feed in particular had exactly one inbound link — the menu entry
being removed — and its only unique offering was a single live cross-feed
stream of acquisition events, a triage view the health chips already answer
without one.

## Decision

Remove the Monitoring submenu entirely; nothing replaces it in the sidebar.

Keep the Ingestion Activity page. It has a live inbound link (the Collection
Health Panel's "View all") and a distinct job: the org-wide event stream,
including feed-less ingestions (drop-zone files, manual uploads) that no feed
dashboard will ever show. Its SSE endpoint also powers the homepage panel and
the manual upload page, so it stays regardless.

Remove the Acquisition Feed wholesale — page, view, URL, template, its SSE
endpoint, and the snapshot builder — **and the publisher side with it**: the
signal handlers emitting `fetch_run.*`, `fetched_file.*`, `upload_session.*`
and `uploaded_file.*` events onto the Redis channel. Those event types were
forwarded only on the acquisition stream (the ingestion stream's allow-list
excludes them), so with the endpoint gone they reached nobody; code that
publishes events with no consumer is a false trail for the next reader. The
acquisition *models* and tracking (`FetchRun`, `UploadSession`, the per-feed
pages, the per-collection APIs) are untouched.

## Consequences

* No single live cross-feed acquisition view remains. "Is anything failing to
  fetch anywhere" is answered by the health chips on the Data Feeds list, not
  by watching a stream. If operators ask for a live view again, the pattern is
  one `git log` away — restore the publishers and an SSE endpoint over the
  shared channel.
* The Redis event channel now carries ingestion events only
  (`file_ingestion.*`, `job.*`).
* `sources/signals.py` is gone; `georiva.sources` registers no signal
  handlers.
* CONTEXT.md's "Operator-facing monitoring surfaces" section no longer names
  an Acquisition Feed; the term is retired from the ubiquitous language.
