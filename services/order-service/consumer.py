from __future__ import annotations

import json
import os
from typing import Any

from sqlalchemy import select

from db import get_session
from models import Order


class OrderStatusConsumer:
    def __init__(
        self,
        broker: str | None = None,
        topic: str = "payment.completed",
        group_id: str = "order-service",
    ) -> None:
        self.broker = broker or os.getenv("KAFKA_BROKER", "localhost:9092")
        self.topic = topic
        self.group_id = group_id
        self._consumer = None

    def _ensure(self) -> Any:
        if self._consumer is not None:
            return self._consumer

        try:
            from confluent_kafka import Consumer
        except ImportError as exc:
            raise RuntimeError(
                "confluent-kafka is required for order status consumption. Install project dependencies first."
            ) from exc

        self._consumer = Consumer(
            {
                "bootstrap.servers": self.broker,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self._consumer.subscribe([self.topic])
        return self._consumer

    def poll(self, timeout: float = 1.0) -> Any:
        consumer = self._ensure()
        return consumer.poll(timeout)

    def update_order_status(self, order_id: str, status: str) -> bool:
        with get_session() as session:
            order = session.execute(select(Order).where(Order.order_id == order_id)).scalar_one_or_none()
            if order is None:
                return False

            order.status = status
            session.add(order)
            return True

    def handle_message(self, raw_message: bytes | str) -> bool:
        if isinstance(raw_message, bytes):
            decoded = raw_message.decode("utf-8")
        else:
            decoded = raw_message

        body = json.loads(decoded)
        payload = body.get("payload", {})
        order_id = str(payload["order_id"])
        event_type = str(body.get("event_type", ""))

        if event_type == "PaymentCompleted":
            return self.update_order_status(order_id=order_id, status="CONFIRMED")
        if event_type == "OrderCancelled":
            return self.update_order_status(order_id=order_id, status="CANCELLED")

        return False
