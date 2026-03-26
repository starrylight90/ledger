from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime

from shared.correlation import get_correlation_id


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "service": getattr(record, "service", os.getenv("LEDGER_SERVICE_NAME", "unknown-service")),
            "message": record.getMessage(),
        }

        correlation_id = get_correlation_id()
        if correlation_id:
            payload["correlation_id"] = correlation_id

        if hasattr(record, "extra_fields") and isinstance(record.extra_fields, dict):
            payload["fields"] = record.extra_fields

        return json.dumps(payload, separators=(",", ":"))


class CorrelationContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        correlation_id = get_correlation_id()
        if correlation_id and not hasattr(record, "correlation_id"):
            setattr(record, "correlation_id", correlation_id)
        return True


def configure_json_logging(service_name: str) -> None:
    root = logging.getLogger()

    for existing in root.handlers:
        if getattr(existing, "_ledger_json_handler", False):
            return

    handler = logging.StreamHandler()
    handler._ledger_json_handler = True  # type: ignore[attr-defined]
    handler.setFormatter(JsonLogFormatter())
    handler.addFilter(CorrelationContextFilter())
    root.addHandler(handler)
    root.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
