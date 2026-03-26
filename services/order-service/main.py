from __future__ import annotations

from datetime import UTC, datetime
import logging
import time
from uuid import UUID
from uuid import uuid4

from fastapi import FastAPI, HTTPException, status
from fastapi.responses import HTMLResponse
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from sqlalchemy import select

from db import engine, get_session
from grpc_client import InventoryGrpcClient, InventoryGrpcUnavailableError
from kafka_producer import KafkaProducerClient
from models import Base, Order
from schemas import CreateOrderRequest, CreateOrderResponse
from shared.correlation import correlation_scope
from shared.logging_utils import configure_json_logging
from shared.observability import get_registry
from shared.dlq_inspector import build_replay_payload, parse_dlq_record, replay_target_topic
from shared.event_schemas import OrderCreatedPayload, build_event

app = FastAPI(title="ledger-order-service", version="0.1.0")
producer = KafkaProducerClient()
inventory_grpc = InventoryGrpcClient()
metrics = get_registry("order-service")
logger = logging.getLogger(__name__)


class DLQInspectRequest(BaseModel):
    source_topic: str
    failed_at: str
    failure_reason: str
    retry_count: int
    original_payload: dict


@app.on_event("startup")
def startup() -> None:
    configure_json_logging("order-service")
    Base.metadata.create_all(bind=engine)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "order-service"}


@app.get("/metrics", response_class=PlainTextResponse)
def metrics_endpoint() -> str:
    return metrics.render_prometheus()


@app.get("/ops", response_class=HTMLResponse)
def ops_dashboard() -> str:
        return """
<!doctype html>
<html lang=\"en\">
<head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>Ledger Phase 4 Ops</title>
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
        .section-title {
            margin: 26px 0 10px;
            font-size: 1.2rem;
            color: #143a35;
        }
        .links {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 10px;
            margin-top: 8px;
        }
        .link-card {
            display: block;
            text-decoration: none;
            background: #fff;
            border: 1px solid #e6d8c3;
            border-radius: 12px;
            padding: 12px;
            color: #173d38;
            transition: transform 120ms ease, box-shadow 120ms ease;
        }
        .link-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 20px rgba(16, 20, 24, 0.08);
        }
        .mini-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
            gap: 10px;
        }
        .mini {
            background: #fffdf9;
            border: 1px solid #eadfce;
            border-radius: 10px;
            padding: 10px;
        }
        .mini .k {
            font-size: 0.78rem;
            text-transform: uppercase;
            color: #6a7479;
            letter-spacing: 0.04em;
        }
        .mini .v {
            margin-top: 4px;
            font-weight: 700;
            color: #1f4a45;
        }
        @keyframes rise {
            to { transform: translateY(0); opacity: 1; }
        }
    </style>
</head>
<body>
    <main class=\"wrap\">
        <h1>Ledger Order Service - Phase 4 Ops</h1>
        <p class="subtitle">Synchronous gRPC inventory admission checks plus async Kafka workflow execution.</p>
        <section class=\"grid\">
            <article class=\"card\">
                <div class="label">Order Ingress</div>
                <div class=\"value\">/orders</div>
            </article>
            <article class=\"card\">
                <div class="label">Health</div>
                <div class=\"value\">/health</div>
            </article>
            <article class=\"card\">
                <div class="label">Primary Topic</div>
                <div class=\"value\">order.created</div>
            </article>
            <article class=\"card\">
                <div class="label">Delivery Mode</div>
                <div class="value">At-Least-Once</div>
            </article>
            <article class="card">
                <div class="label">Retry Policy</div>
                <div class="value">Exponential</div>
            </article>
            <article class="card">
                <div class="label">DLQ Inspector</div>
                <div class="value">/ops/dlq/inspect</div>
            </article>
            <article class="card">
                <div class="label">Schema Guard</div>
                <div class="value">Avro + Registry</div>
            </article>
            <article class="card">
                <div class="label">gRPC Boundary</div>
                <div class="value">InventoryService.CheckAvailability</div>
            </article>
        </section>
        <h2 class="section-title">Observability Control Surface</h2>
        <div class="mini-grid">
            <div class="mini"><div class="k">HTTP Metrics</div><div class="v" id="http-metric">loading...</div></div>
            <div class="mini"><div class="k">Kafka Throughput</div><div class="v" id="kafka-metric">loading...</div></div>
            <div class="mini"><div class="k">gRPC Calls</div><div class="v" id="grpc-metric">loading...</div></div>
            <div class="mini"><div class="k">DLQ Signals</div><div class="v" id="dlq-metric">loading...</div></div>
        </div>

        <h2 class="section-title">External Surfaces</h2>
        <section class="links">
            <a class="link-card" href="http://localhost:9090/targets" target="_blank" rel="noreferrer">Prometheus Targets</a>
            <a class="link-card" href="http://localhost:3000/d/ledger-phase5/ledger-phase5-observability" target="_blank" rel="noreferrer">Grafana Phase 5 Dashboard</a>
            <a class="link-card" href="http://localhost:8080" target="_blank" rel="noreferrer">Redpanda Console</a>
            <a class="link-card" href="/metrics" target="_blank" rel="noreferrer">Order Service Raw Metrics</a>
            <a class="link-card" href="/health" target="_blank" rel="noreferrer">Order Service Health</a>
            <a class="link-card" href="/ops/dlq/inspect" target="_blank" rel="noreferrer">DLQ Replay Planner</a>
        </section>
        <div class="badge">Phase 5: Throughput, lag, p95 latency, and correlation-aware traces are now first-class.</div>
    </main>
    <script>
        async function hydrateMetrics() {
            try {
                const response = await fetch('/metrics');
                const text = await response.text();
                const getLine = (key) => (text.split('\n').find((line) => line.startsWith(key)) || 'n/a');

                document.getElementById('http-metric').textContent = getLine('ledger_http_requests_total');
                document.getElementById('kafka-metric').textContent = getLine('ledger_publish_total');
                document.getElementById('grpc-metric').textContent = getLine('ledger_grpc_client_total');
                document.getElementById('dlq-metric').textContent = getLine('ledger_dlq_publish_total');
            } catch (err) {
                document.getElementById('http-metric').textContent = 'metrics unavailable';
                document.getElementById('kafka-metric').textContent = 'metrics unavailable';
                document.getElementById('grpc-metric').textContent = 'metrics unavailable';
                document.getElementById('dlq-metric').textContent = 'metrics unavailable';
            }
        }

        hydrateMetrics();
    </script>
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
    request_started = time.perf_counter()
    if not payload.items:
        metrics.counter_inc(
            "ledger_http_requests_total",
            labels={"path": "/orders", "method": "POST", "status": "400"},
            description="HTTP requests by path, method, and status",
        )
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="items must not be empty")

    request_correlation_id = str(uuid4())
    with correlation_scope(request_correlation_id):
        logger.info(
            "order_create_received",
            extra={"extra_fields": {"idempotency_key": payload.idempotency_key, "items": len(payload.items)}},
        )

        for item in payload.items:
            grpc_started = time.perf_counter()
            try:
                available, current_stock, message = inventory_grpc.check_availability(item.sku, item.qty)
                metrics.counter_inc(
                    "ledger_grpc_client_total",
                    labels={"method": "CheckAvailability", "result": "success"},
                    description="gRPC client calls by method and result",
                )
            except InventoryGrpcUnavailableError as exc:
                metrics.counter_inc(
                    "ledger_grpc_client_total",
                    labels={"method": "CheckAvailability", "result": "error"},
                    description="gRPC client calls by method and result",
                )
                metrics.counter_inc(
                    "ledger_http_requests_total",
                    labels={"path": "/orders", "method": "POST", "status": "503"},
                    description="HTTP requests by path, method, and status",
                )
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail={
                        "code": "inventory_grpc_unavailable",
                        "sku": item.sku,
                        "qty": item.qty,
                        "message": str(exc),
                    },
                ) from exc
            finally:
                metrics.histogram_observe(
                    "ledger_grpc_client_latency_ms",
                    value=(time.perf_counter() - grpc_started) * 1000.0,
                    labels={"method": "CheckAvailability"},
                    description="gRPC client latency in milliseconds",
                )

            if not available:
                metrics.counter_inc(
                    "ledger_http_requests_total",
                    labels={"path": "/orders", "method": "POST", "status": "409"},
                    description="HTTP requests by path, method, and status",
                )
                logger.info(
                    "order_create_rejected_stock",
                    extra={"extra_fields": {"sku": item.sku, "requested_qty": item.qty, "stock": current_stock}},
                )
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "inventory_not_available",
                        "sku": item.sku,
                        "requested_qty": item.qty,
                        "current_stock": current_stock,
                        "message": message,
                    },
                )

    with get_session() as session:
        existing = session.execute(
            select(Order).where(Order.idempotency_key == payload.idempotency_key)
        ).scalar_one_or_none()
        if existing:
            metrics.counter_inc(
                "ledger_http_requests_total",
                labels={"path": "/orders", "method": "POST", "status": "202"},
                description="HTTP requests by path, method, and status",
            )
            logger.info(
                "order_create_idempotent_hit",
                extra={"extra_fields": {"order_id": existing.order_id, "idempotency_key": existing.idempotency_key}},
            )
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

        order_id = order.order_id
        correlation_id = order.correlation_id
        created_at = order.created_at
        idempotency_key = order.idempotency_key

    event_payload = OrderCreatedPayload(
        order_id=order_id,
        customer_id=payload.customer_id,
        items=[item.model_dump() for item in payload.items],
    ).model_dump()
    event = build_event("OrderCreated", correlation_id=UUID(correlation_id), payload=event_payload)

    with correlation_scope(correlation_id):
        try:
            producer.publish(topic="order.created", key=order_id, payload=event.model_dump(mode="json"))
        except Exception as exc:
            with get_session() as session:
                order = session.execute(select(Order).where(Order.order_id == order_id)).scalar_one_or_none()
                if order is not None:
                    order.status = "PUBLISH_FAILED"
                    session.add(order)
            metrics.counter_inc(
                "ledger_http_requests_total",
                labels={"path": "/orders", "method": "POST", "status": "503"},
                description="HTTP requests by path, method, and status",
            )
            logger.exception("order_create_publish_failed", extra={"extra_fields": {"order_id": order_id}})
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Order persisted but event publish failed: {exc}",
            ) from exc

        metrics.counter_inc(
            "ledger_http_requests_total",
            labels={"path": "/orders", "method": "POST", "status": "202"},
            description="HTTP requests by path, method, and status",
        )
        metrics.histogram_observe(
            "ledger_http_request_latency_ms",
            value=(time.perf_counter() - request_started) * 1000.0,
            labels={"path": "/orders", "method": "POST"},
            description="HTTP request latency in milliseconds",
        )
        logger.info("order_create_accepted", extra={"extra_fields": {"order_id": order_id, "out_topic": "order.created"}})

        return CreateOrderResponse(
            order_id=order_id,
            status="PENDING",
            created_at=created_at,
            accepted_at=datetime.now(UTC),
            correlation_id=correlation_id,
            idempotency_key=idempotency_key,
        )
