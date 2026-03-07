from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID
from uuid import uuid4

from fastapi import FastAPI, HTTPException, status
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy import select

from db import engine, get_session
from kafka_producer import KafkaProducerClient
from models import Base, Order
from schemas import CreateOrderRequest, CreateOrderResponse
from shared.dlq_inspector import build_replay_payload, parse_dlq_record, replay_target_topic
from shared.event_schemas import OrderCreatedPayload, build_event

app = FastAPI(title="ledger-order-service", version="0.1.0")
producer = KafkaProducerClient()


class DLQInspectRequest(BaseModel):
    source_topic: str
    failed_at: str
    failure_reason: str
    retry_count: int
    original_payload: dict


@app.on_event("startup")
def startup() -> None:
    Base.metadata.create_all(bind=engine)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "order-service"}


@app.get("/ops", response_class=HTMLResponse)
def ops_dashboard() -> str:
        return """
<!doctype html>
<html lang=\"en\">
<head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>Ledger Phase 1 Ops</title>
    <style>
        :root {
            --ink: #101418;
            --sand: #f5eee5;
            --copper: #b5633b;
            --forest: #1f4a45;
            --mist: #d8e3dc;
            --card: #fffdf9;
        }
        * { box-sizing: border-box; }
        body {
            margin: 0;
            font-family: "Georgia", "Times New Roman", serif;
            color: var(--ink);
            background:
                radial-gradient(1200px 480px at 10% -10%, #f6d7b5 0%, transparent 60%),
                radial-gradient(1000px 500px at 100% 20%, #b7d7cc 0%, transparent 55%),
                linear-gradient(180deg, #fffaf3 0%, #f6f0e8 100%);
            min-height: 100vh;
            padding: 28px;
        }
        .wrap { max-width: 1120px; margin: 0 auto; }
        h1 { margin: 0 0 8px; font-size: 2.1rem; }
        p.subtitle { margin: 0 0 20px; color: #3a4a4a; }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 14px;
            margin-top: 12px;
        }
        .card {
            background: var(--card);
            border: 1px solid #eadfce;
            border-radius: 14px;
            padding: 16px;
            box-shadow: 0 10px 30px rgba(16, 20, 24, 0.06);
            transform: translateY(8px);
            opacity: 0;
            animation: rise 500ms ease forwards;
        }
        .card:nth-child(2) { animation-delay: 80ms; }
        .card:nth-child(3) { animation-delay: 140ms; }
        .card:nth-child(4) { animation-delay: 200ms; }
        .label { font-size: 0.8rem; letter-spacing: 0.06em; text-transform: uppercase; color: #6a7479; }
        .value { margin-top: 8px; font-size: 1.4rem; font-weight: 700; color: var(--forest); }
        .badge {
            display: inline-block;
            margin-top: 20px;
            background: var(--mist);
            color: #1c3e3a;
            padding: 8px 12px;
            border-radius: 999px;
            font-size: 0.9rem;
        }
        @keyframes rise {
            to { transform: translateY(0); opacity: 1; }
        }
    </style>
</head>
<body>
    <main class=\"wrap\">
        <h1>Ledger Order Service - Phase 1 Ops</h1>
        <p class=\"subtitle\">Live checkpoints for the Order -> Kafka -> Inventory reservation flow.</p>
        <section class=\"grid\">
            <article class=\"card\">
                <div class=\"label\">Order Ingress</div>
                <div class=\"value\">/orders</div>
            </article>
            <article class=\"card\">
                <div class=\"label\">Health</div>
                <div class=\"value\">/health</div>
            </article>
            <article class=\"card\">
                <div class=\"label\">Primary Topic</div>
                <div class=\"value\">order.created</div>
            </article>
            <article class=\"card\">
                <div class=\"label\">Mode</div>
                <div class=\"value\">At-Least-Once</div>
            </article>
        </section>
        <div class=\"badge\">Next: Phase 2 saga choreography and compensating rollback</div>
    </main>
</body>
</html>
"""


@app.post("/ops/dlq/inspect")
def inspect_dlq(payload: DLQInspectRequest) -> dict:
    record = parse_dlq_record(payload.model_dump())
    return {
        "replay_topic": replay_target_topic(record),
        "replay_payload": build_replay_payload(record),
    }


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
