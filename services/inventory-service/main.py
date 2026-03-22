from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from consumer import InventoryConsumer
from db import engine
from grpc_server import start_grpc_server
from models import Base
from shared.logging_utils import configure_json_logging
from shared.observability import get_registry

app = FastAPI(title="ledger-inventory-service", version="0.1.0")
consumer = InventoryConsumer()
grpc_server = None
metrics = get_registry("inventory-service")


@app.on_event("startup")
def startup() -> None:
    global grpc_server
    configure_json_logging("inventory-service")
    Base.metadata.create_all(bind=engine)
    grpc_server = start_grpc_server()


@app.on_event("shutdown")
def shutdown() -> None:
    global grpc_server
    if grpc_server is not None:
        grpc_server.stop(grace=2)
        grpc_server = None


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "inventory-service", "topic": consumer.topic}


@app.get("/metrics", response_class=PlainTextResponse)
def metrics_endpoint() -> str:
    return metrics.render_prometheus()
