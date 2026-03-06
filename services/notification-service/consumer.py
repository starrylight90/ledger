from __future__ import annotations

import json
import logging
import os
from typing import Any

from sqlalchemy import select

from db import get_session
from models import NotificationLog, ProcessedNotificationEvent
from shared.error_classification import FailureKind, classify_error
from shared.retry_policy import RetryPolicy, retry_with_backoff

logger = logging.getLogger(__name__)


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

    def process_polled_message(self, message: Any) -> int | None:
        if message is None:
            return None

        if message.error():
            raise RuntimeError(str(message.error()))

        policy = RetryPolicy(max_attempts=int(os.getenv("CONSUMER_MAX_ATTEMPTS", "4")))
        result = retry_with_backoff(
            lambda: self.handle_message(message.value()),
            policy=policy,
            should_retry=lambda exc: classify_error(exc) == FailureKind.TRANSIENT,
            on_retry=lambda attempt, delay, exc: logger.warning(
                "notification_consumer_retry",
                extra={
                    "attempt": attempt,
                    "delay_seconds": delay,
                    "error": str(exc),
                    "failure_kind": classify_error(exc).value,
                },
            ),
        )
        self._ensure().commit(message=message, asynchronous=False)
        return result

    def handle_message(self, raw_message: bytes | str) -> int:
        if isinstance(raw_message, bytes):
            decoded = raw_message.decode("utf-8")
        else:
            decoded = raw_message

        body = json.loads(decoded)
        event_id = str(body.get("event_id", ""))
        if event_id:
            with get_session() as session:
                existing = session.execute(
                    select(ProcessedNotificationEvent).where(ProcessedNotificationEvent.event_id == event_id)
                ).scalar_one_or_none()
                if existing is not None:
                    return existing.id

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
            if event_id:
                session.add(ProcessedNotificationEvent(event_id=event_id, event_type=event_type))
            notification = NotificationLog(
                order_id=order_id,
                event_type=event_type,
                channel="webhook",
                message=message,
            )
            session.add(notification)
            session.flush()
            return notification.id
