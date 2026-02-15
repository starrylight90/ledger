from __future__ import annotations

from fastapi import FastAPI

from consumer import InventoryConsumer
from db import engine
from models import Base

app = FastAPI(title="ledger-inventory-service", version="0.1.0")
consumer = InventoryConsumer()


@app.on_event("startup")
def startup() -> None:
    Base.metadata.create_all(bind=engine)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "inventory-service", "topic": consumer.topic}
