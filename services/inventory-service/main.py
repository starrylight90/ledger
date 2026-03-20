from __future__ import annotations

from fastapi import FastAPI

from consumer import InventoryConsumer
from db import engine
from grpc_server import start_grpc_server
from models import Base

app = FastAPI(title="ledger-inventory-service", version="0.1.0")
consumer = InventoryConsumer()
grpc_server = None


@app.on_event("startup")
def startup() -> None:
    global grpc_server
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
