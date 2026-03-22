from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from consumer import PaymentConsumer
from db import engine
from models import Base
from shared.logging_utils import configure_json_logging
from shared.observability import get_registry

app = FastAPI(title="ledger-payment-service", version="0.1.0")
consumer = PaymentConsumer()
metrics = get_registry("payment-service")


@app.on_event("startup")
def startup() -> None:
    configure_json_logging("payment-service")
    Base.metadata.create_all(bind=engine)


@app.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "service": "payment-service",
        "topic": consumer.topic,
        "group_id": consumer.group_id,
    }


@app.get("/metrics", response_class=PlainTextResponse)
def metrics_endpoint() -> str:
    return metrics.render_prometheus()
