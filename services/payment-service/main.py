from __future__ import annotations

from fastapi import FastAPI

from consumer import PaymentConsumer
from db import engine
from models import Base

app = FastAPI(title="ledger-payment-service", version="0.1.0")
consumer = PaymentConsumer()


@app.on_event("startup")
def startup() -> None:
    Base.metadata.create_all(bind=engine)


@app.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "service": "payment-service",
        "topic": consumer.topic,
        "group_id": consumer.group_id,
    }
