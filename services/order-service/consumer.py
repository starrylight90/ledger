from __future__ import annotations

import json
import logging
import os
from typing import Any

from sqlalchemy import select

from db import get_session
from models import Order, ProcessedOrderEvent
from shared.dlq_publisher import DLQPublisher
from shared.error_classification import FailureKind, classify_error
from shared.retry_policy import RetryExhaustedError, RetryPolicy, retry_with_backoff

logger = logging.getLogger(__name__)


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
        self._dlq = DLQPublisher(broker=self.broker)

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

    def process_polled_message(self, message: Any) -> bool | None:
        if message is None:
            return None

        if message.error():
            raise RuntimeError(str(message.error()))

        policy = RetryPolicy(max_attempts=int(os.getenv("CONSUMER_MAX_ATTEMPTS", "4")))
        try:
            updated = retry_with_backoff(
                lambda: self.handle_message(message.value()),
                policy=policy,
                should_retry=lambda exc: classify_error(exc) == FailureKind.TRANSIENT,
                on_retry=lambda attempt, delay, exc: logger.warning(
                    "order_consumer_retry",
                    extra={
                        "attempt": attempt,
                        "delay_seconds": delay,
                        "error": str(exc),
                        "failure_kind": classify_error(exc).value,
                    },
                ),
            )
            self._ensure().commit(message=message, asynchronous=False)
            return updated
        except RetryExhaustedError as exhausted:
            raw_value = message.value()
            if isinstance(raw_value, bytes):
                decoded = raw_value.decode("utf-8", errors="replace")
            else:
                decoded = str(raw_value)

            try:
                payload = json.loads(decoded)
            except json.JSONDecodeError:
                payload = {"raw": decoded}

            key = str(payload.get("payload", {}).get("order_id", "unknown"))
            self._dlq.publish(
                source_topic=self.topic,
                key=key,
                original_payload=payload,
                failure_reason=str(exhausted.last_error),
                retry_count=exhausted.attempts,
            )
            self._ensure().commit(message=message, asynchronous=False)
            return False

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
