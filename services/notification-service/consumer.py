from __future__ import annotations

import json
import os
from typing import Any

from db import get_session
from models import NotificationLog


class NotificationConsumer:
    def __init__(
        self,
        broker: str | None = None,
        topics: list[str] | None = None,
        group_id: str = "notification-service",
    ) -> None:
        self.broker = broker or os.getenv("KAFKA_BROKER", "localhost:9092")
        self.topics = topics or ["payment.completed", "order.cancelled"]
        self.group_id = group_id
        self._consumer = None

    def _ensure(self) -> Any:
        if self._consumer is not None:
            return self._consumer

        try:
            from confluent_kafka import Consumer
        except ImportError as exc:
            raise RuntimeError(
                "confluent-kafka is required for notification consumption. Install project dependencies first."
            ) from exc

        self._consumer = Consumer(
            {
                "bootstrap.servers": self.broker,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self._consumer.subscribe(self.topics)
        return self._consumer

    def poll(self, timeout: float = 1.0) -> Any:
        consumer = self._ensure()
        return consumer.poll(timeout)

    def handle_message(self, raw_message: bytes | str) -> int:
        if isinstance(raw_message, bytes):
            decoded = raw_message.decode("utf-8")
        else:
            decoded = raw_message

        body = json.loads(decoded)
        payload = body.get("payload", {})
        order_id = str(payload.get("order_id", "unknown"))
        event_type = str(body.get("event_type", "unknown"))

        if event_type == "PaymentCompleted":
            message = f"Order {order_id} confirmed. Payment completed."
        elif event_type == "OrderCancelled":
            message = f"Order {order_id} cancelled after compensation."
        else:
            message = f"Order {order_id} received terminal event {event_type}."

        with get_session() as session:
            notification = NotificationLog(
                order_id=order_id,
                event_type=event_type,
                channel="webhook",
                message=message,
            )
            session.add(notification)
            session.flush()
            return notification.id
