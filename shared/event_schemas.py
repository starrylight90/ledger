from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class EventEnvelope(BaseModel):
    event_id: UUID
    event_type: str = Field(min_length=3, max_length=128)
    timestamp: datetime
    correlation_id: UUID
    payload: dict[str, Any]


class OrderCreatedPayload(BaseModel):
    order_id: str = Field(min_length=1, max_length=36)
    customer_id: str = Field(min_length=1, max_length=128)
    items: list[dict[str, Any]] = Field(default_factory=list)


def build_event(event_type: str, correlation_id: UUID, payload: dict[str, Any]) -> EventEnvelope:
    return EventEnvelope(
        event_id=uuid4(),
        event_type=event_type,
        timestamp=datetime.now(UTC),
        correlation_id=correlation_id,
        payload=payload,
    )
