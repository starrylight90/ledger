from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any


_correlation_id_ctx: ContextVar[str | None] = ContextVar("ledger_correlation_id", default=None)


def set_correlation_id(correlation_id: str | None) -> None:
    _correlation_id_ctx.set(correlation_id)


def get_correlation_id() -> str | None:
    return _correlation_id_ctx.get()


def clear_correlation_id() -> None:
    _correlation_id_ctx.set(None)


def event_correlation_id(envelope: dict[str, Any]) -> str | None:
    value = envelope.get("correlation_id")
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


@contextmanager
def correlation_scope(correlation_id: str | None):
    token = _correlation_id_ctx.set(correlation_id)
    try:
        yield
    finally:
        _correlation_id_ctx.reset(token)
