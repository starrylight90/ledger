from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class OrderItem(BaseModel):
    sku: str = Field(min_length=1, max_length=128)
    qty: int = Field(ge=1, le=10000)


class CreateOrderRequest(BaseModel):
    customer_id: str = Field(min_length=1, max_length=128)
    idempotency_key: str = Field(min_length=8, max_length=128)
    items: list[OrderItem] = Field(default_factory=list)


class CreateOrderResponse(BaseModel):
    order_id: str
    status: Literal["PENDING"]
    created_at: datetime
    accepted_at: datetime
    correlation_id: str
    idempotency_key: str
