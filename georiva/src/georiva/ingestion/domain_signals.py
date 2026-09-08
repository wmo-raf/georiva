"""
Ingestion-side domain signals (ADR 0026).

``run_ingestion_closed`` fires when one model run has finished arriving into one
collection; ``run_ingestion_reopened`` fires when a later file arrives for a run
that had already closed. Ingestion knows nothing about who listens — a
retention pass, a manifest builder or an exporter subscribes in its own
``AppConfig.ready()``, keeping the import direction ADR 0020 protects: the
producer never imports its consumers.

Kept apart from ``ingestion/signals.py`` deliberately. That module holds
*receivers* — the Django ``post_save`` hooks that fan live rows out to the SSE
stream. This one holds the *signals other apps subscribe to*. Both are "signals"
in Django's vocabulary and they are opposite halves of it, so they do not share
a module.

Kwargs sent by both: ``run`` (the ``RunIngestion``).
"""

import django.dispatch

#: One model run has finished arriving into one collection. Receivers must
#: tolerate being called again for the same run: a run reopens on any later
#: arrival and closes a second time, at a higher ``revision``.
run_ingestion_closed = django.dispatch.Signal()

#: A file arrived for a run that had already closed. ``run.revision`` has
#: already been bumped and the row is open again. Anything a receiver derived
#: from the earlier close is now provisional.
run_ingestion_reopened = django.dispatch.Signal()
