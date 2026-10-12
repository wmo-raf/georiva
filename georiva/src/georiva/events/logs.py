"""Put the chain's Correlation and Actor on every log line (ADR 0030).

The filter is attached to the *handler*, not to a logger: the processing and
derivation-invocation loggers do not propagate to the root, so a filter on the
root logger would miss exactly the noisiest part of the pipeline.

Plain text stays the default (a ``[cid=…]`` prefix); ``GEORIVA_LOG_FORMAT=json``
selects :class:`JsonFormatter`, which makes the id a queryable field.
"""

import json
import logging

from .context import current_context

NO_CONTEXT = "-"


class EventContextFilter(logging.Filter):
    def filter(self, record):
        context = current_context()
        record.correlation_id = str(context.correlation_id) if context else NO_CONTEXT
        record.actor = context.actor if context else NO_CONTEXT
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "time": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "function": record.funcName,
            "line": record.lineno,
            "message": record.getMessage(),
            "correlation_id": getattr(record, "correlation_id", NO_CONTEXT),
            "actor": getattr(record, "actor", NO_CONTEXT),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)
