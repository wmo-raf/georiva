# SSE for the Ingestion Activity Feed

The Ingestion Activity Feed needs real-time push from server to browser. We chose Server-Sent Events (SSE)
over polling and over WebSockets.

Polling was rejected because it loses intermediate `progress_state` steps — the step-by-step log accumulates
values between polls, so any state emitted and replaced before the next poll is silently dropped. SSE pushes
every event as it happens.

WebSockets were rejected because the data flow is strictly one-way (server → browser). Actions like cancel/retry
are standard HTTP POSTs; no bidirectional socket is needed. WebSockets would require Django Channels + ASGI
channel layer for a problem SSE solves with a plain async Django view.

## Consequences

- The web service must run under `gunicorn-asgi` (not `gunicorn-wsgi`). The `gunicorn-asgi` entrypoint already
  exists in `docker-entrypoint.sh`; this is a config switch, not new infrastructure.
- The ingestion pipeline must publish typed events to a Redis pub/sub channel (`ingestion:events`) at key
  transitions. Model-level events use Django signals on `DataArrival` and `FileIngestion`. Job-level progress
  events use a `PublishingProgress` wrapper (a `Progress` subclass) created in `FileIngestionJobType.run()`
  and threaded through `IngestionService` and `IngestionHandler` — this is how intermediate steps reach the
  browser without modifying the task_ferry package.
