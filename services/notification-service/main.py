from __future__ import annotations

from fastapi import FastAPI

from consumer import NotificationConsumer
from db import engine
from models import Base

app = FastAPI(title="ledger-notification-service", version="0.1.0")
consumer = NotificationConsumer()


@app.on_event("startup")
def startup() -> None:
    Base.metadata.create_all(bind=engine)


@app.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "service": "notification-service",
        "topics": ",".join(consumer.topics),
    }
