from __future__ import annotations

from fastapi import FastAPI

from consumer import InventoryConsumer

app = FastAPI(title="ledger-inventory-service", version="0.1.0")
consumer = InventoryConsumer()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "inventory-service", "topic": consumer.topic}
