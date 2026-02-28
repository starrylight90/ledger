from __future__ import annotations

import json
import os
from typing import Any

from sqlalchemy import select

from db import get_session
from models import Order, ProcessedOrderEvent


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

    def _is_processed(self, event_id: str) -> bool:
        with get_session() as session:
            existing = session.execute(
                select(ProcessedOrderEvent).where(ProcessedOrderEvent.event_id == event_id)
            ).scalar_one_or_none()
            return existing is not None

    def _mark_processed(self, event_id: str, event_type: str) -> None:
        with get_session() as session:
            session.add(ProcessedOrderEvent(event_id=event_id, event_type=event_type))

    def handle_message(self, raw_message: bytes | str) -> bool:
        if isinstance(raw_message, bytes):
            decoded = raw_message.decode("utf-8")
        else:
            decoded = raw_message

        body = json.loads(decoded)
        payload = body.get("payload", {})
        order_id = str(payload["order_id"])
        event_type = str(body.get("event_type", ""))
        event_id = str(body.get("event_id", ""))

        if event_id and self._is_processed(event_id):
            return True

        updated = False
        if event_type == "PaymentCompleted":
            updated = self.update_order_status(order_id=order_id, status="CONFIRMED")
        elif event_type == "OrderCancelled":
            updated = self.update_order_status(order_id=order_id, status="CANCELLED")

        if updated and event_id:
            self._mark_processed(event_id=event_id, event_type=event_type)

        return updated


class OrderCancelledConsumer(OrderStatusConsumer):
    def __init__(self, broker: str | None = None, group_id: str = "order-service") -> None:
        super().__init__(broker=broker, topic="order.cancelled", group_id=group_id)
