from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from sqlalchemy import select

from db import get_session
from models import NotificationLog, ProcessedNotificationEvent
from shared.avro_codec import AvroCodec
from shared.correlation import correlation_scope, event_correlation_id
from shared.dlq_publisher import DLQPublisher
from shared.error_classification import FailureKind, classify_error
from shared.observability import get_registry
from shared.retry_policy import RetryExhaustedError, RetryPolicy, retry_with_backoff

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
        self._dlq = DLQPublisher(broker=self.broker, service_name="notification-service")
        self._codec = AvroCodec()
        self._metrics = get_registry("notification-service")

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

        self._record_consumer_lag(message)
        started = time.perf_counter()

        policy = RetryPolicy(max_attempts=int(os.getenv("CONSUMER_MAX_ATTEMPTS", "4")))
        try:
            source_topic = self.topics[0]
            if hasattr(message, "topic"):
                source_topic = str(message.topic())

            result = retry_with_backoff(
                lambda: self.handle_message(message.value(), topic=source_topic),
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
            self._metrics.counter_inc(
                "ledger_consume_total",
                labels={"topic": source_topic, "result": "success"},
                description="Total consumed events by topic and result",
            )
            return result
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
            source_topic = self.topics[0]
            if hasattr(message, "topic"):
                source_topic = str(message.topic())

            self._dlq.publish(
                source_topic=source_topic,
                key=key,
                original_payload=payload,
                failure_reason=str(exhausted.last_error),
                retry_count=exhausted.attempts,
            )
            self._ensure().commit(message=message, asynchronous=False)
            self._metrics.counter_inc(
                "ledger_consume_total",
                labels={"topic": source_topic, "result": "dlq"},
                description="Total consumed events by topic and result",
            )
            return None
        finally:
            self._metrics.histogram_observe(
                "ledger_consume_latency_ms",
                value=(time.perf_counter() - started) * 1000.0,
                labels={"topic": source_topic if 'source_topic' in locals() else self.topics[0]},
                description="Kafka consume processing latency in milliseconds",
            )

    def _record_consumer_lag(self, message: Any) -> None:
        if not hasattr(message, "topic") or not hasattr(message, "partition") or not hasattr(message, "offset"):
            return

        if self._consumer is None or not hasattr(self._consumer, "get_watermark_offsets"):
            return

        try:
            from confluent_kafka import TopicPartition
        except ImportError:
            return

        try:
            tp = TopicPartition(message.topic(), message.partition())
            _low, high = self._consumer.get_watermark_offsets(tp, timeout=1.0)
            lag = max(0, int(high) - int(message.offset()) - 1)
            self._metrics.gauge_set(
                "ledger_consumer_lag",
                float(lag),
                labels={"topic": message.topic(), "partition": str(message.partition())},
                description="Estimated consumer lag by topic and partition",
            )
        except Exception:
            return

    def handle_message(self, raw_message: bytes | str, topic: str | None = None) -> int:
        if isinstance(raw_message, bytes):
            decoded = raw_message.decode("utf-8")
        else:
            decoded = raw_message

        if topic is None:
            probe = json.loads(decoded)
            event_type = str(probe.get("event_type", ""))
            if event_type == "PaymentCompleted":
                topic = "payment.completed"
            elif event_type == "OrderCancelled":
                topic = "order.cancelled"
            else:
                topic = self.topics[0]

        body = self._codec.deserialize_for_topic(topic, decoded)
        event_id = str(body.get("event_id", ""))
        correlation_id = event_correlation_id(body)
        with correlation_scope(correlation_id):
            if event_id:
                with get_session() as session:
                    existing = session.execute(
                        select(ProcessedNotificationEvent).where(ProcessedNotificationEvent.event_id == event_id)
                    ).scalar_one_or_none()
                    if existing is not None:
                        logger.info("notification_consumer_duplicate", extra={"extra_fields": {"event_id": event_id}})
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
                logger.info(
                    "notification_consumer_processed",
                    extra={"extra_fields": {"event_id": event_id, "order_id": order_id, "event_type": event_type}},
                )
                return notification.id
