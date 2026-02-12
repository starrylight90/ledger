from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from fastapi import FastAPI, HTTPException, status

from schemas import CreateOrderRequest, CreateOrderResponse

app = FastAPI(title="ledger-order-service", version="0.1.0")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "order-service"}


@app.post("/orders", response_model=CreateOrderResponse, status_code=status.HTTP_202_ACCEPTED)
def create_order(payload: CreateOrderRequest) -> CreateOrderResponse:
    if not payload.items:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="items must not be empty")

    now = datetime.now(UTC)
    order_id = str(uuid4())

    return CreateOrderResponse(
        order_id=order_id,
        status="PENDING",
        created_at=now,
        accepted_at=now,
        correlation_id=str(uuid4()),
        idempotency_key=payload.idempotency_key,
    )
