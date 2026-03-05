from __future__ import annotations

import json
from enum import Enum


class FailureKind(str, Enum):
    TRANSIENT = "transient"
    POISON = "poison"


def classify_error(exc: Exception) -> FailureKind:
    if isinstance(exc, (json.JSONDecodeError, KeyError, TypeError, ValueError)):
        return FailureKind.POISON

    if isinstance(exc, (ConnectionError, TimeoutError)):
        return FailureKind.TRANSIENT

    if isinstance(exc, RuntimeError):
        message = str(exc).lower()
        if any(keyword in message for keyword in ("timeout", "tempor", "connection", "unavailable")):
            return FailureKind.TRANSIENT

    return FailureKind.TRANSIENT
