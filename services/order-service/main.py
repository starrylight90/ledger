from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID
from uuid import uuid4

from fastapi import FastAPI, HTTPException, status
from sqlalchemy import select

from db import engine, get_session
from kafka_producer import KafkaProducerClient
from models import Base, Order
from schemas import CreateOrderRequest, CreateOrderResponse
from shared.event_schemas import OrderCreatedPayload, build_event

app = FastAPI(title="ledger-order-service", version="0.1.0")
producer = KafkaProducerClient()


@app.on_event("startup")
def startup() -> None:
    Base.metadata.create_all(bind=engine)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "order-service"}


@app.post("/orders", response_model=CreateOrderResponse, status_code=status.HTTP_202_ACCEPTED)
def create_order(payload: CreateOrderRequest) -> CreateOrderResponse:
    if not payload.items:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="items must not be empty")

    with get_session() as session:
        existing = session.execute(
            select(Order).where(Order.idempotency_key == payload.idempotency_key)
        ).scalar_one_or_none()
        if existing:
            return CreateOrderResponse(
                order_id=existing.order_id,
                status="PENDING",
                created_at=existing.created_at,
                accepted_at=datetime.now(UTC),
                correlation_id=existing.correlation_id,
                idempotency_key=existing.idempotency_key,
            )

        now = datetime.now(UTC)
        order = Order(
            order_id=str(uuid4()),
            correlation_id=str(uuid4()),
            customer_id=payload.customer_id,
            idempotency_key=payload.idempotency_key,
            status="PENDING",
            created_at=now,
        )
        session.add(order)
        session.flush()

        event_payload = OrderCreatedPayload(
            order_id=order.order_id,
            customer_id=order.customer_id,
            items=[item.model_dump() for item in payload.items],
        ).model_dump()
        event = build_event("OrderCreated", correlation_id=UUID(order.correlation_id), payload=event_payload)
        producer.publish(topic="order.created", key=order.order_id, payload=event.model_dump(mode="json"))

        return CreateOrderResponse(
            order_id=order.order_id,
            status="PENDING",
            created_at=order.created_at,
            accepted_at=now,
            correlation_id=order.correlation_id,
            idempotency_key=order.idempotency_key,
        )
