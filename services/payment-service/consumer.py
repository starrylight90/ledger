from __future__ import annotations

import json
import logging
import os
import time
from typing import Any
from uuid import UUID

from sqlalchemy import select

from db import get_session
from kafka_producer import KafkaProducerClient
from models import ProcessedPaymentEvent
from shared.avro_codec import AvroCodec
from shared.correlation import correlation_scope, event_correlation_id
from shared.dlq_publisher import DLQPublisher
from shared.error_classification import FailureKind, classify_error
from shared.event_schemas import build_event
from shared.kafka_tuning import consumer_config
from shared.observability import get_registry
from shared.retry_policy import RetryExhaustedError, RetryPolicy, retry_with_backoff

logger = logging.getLogger(__name__)


class PaymentConsumer:
    def __init__(
        self,
        broker: str | None = None,
        topic: str = "inventory.reserved",
        group_id: str = "payment-service",
    ) -> None:
        self.broker = broker or os.getenv("KAFKA_BROKER", "localhost:9092")
        self.topic = topic
        self.group_id = group_id
        self._consumer = None
        self._producer = KafkaProducerClient(broker=self.broker)
        self._dlq = DLQPublisher(broker=self.broker, service_name="payment-service")
        self._codec = AvroCodec()
        self._metrics = get_registry("payment-service")

    def _ensure(self) -> Any:
        if self._consumer is not None:
            return self._consumer

        try:
            from confluent_kafka import Consumer
        except ImportError as exc:
            raise RuntimeError(
                "confluent-kafka is required for payment consumption. Install project dependencies first."
            ) from exc

        self._consumer = Consumer(consumer_config(self.broker, self.group_id))
        self._consumer.subscribe([self.topic])
        return self._consumer

    def poll(self, timeout: float = 1.0) -> Any:
        consumer = self._ensure()
        return consumer.poll(timeout)

    def process_polled_message(self, message: Any) -> str | None:
        if message is None:
            return None

        if message.error():
            raise RuntimeError(str(message.error()))

        self._record_consumer_lag(message)
        started = time.perf_counter()

        policy = RetryPolicy(max_attempts=int(os.getenv("CONSUMER_MAX_ATTEMPTS", "4")))
        try:
            status = retry_with_backoff(
                lambda: self.handle_message(message.value()),
                policy=policy,
                should_retry=lambda exc: classify_error(exc) == FailureKind.TRANSIENT,
                on_retry=lambda attempt, delay, exc: logger.warning(
                    "payment_consumer_retry",
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
                labels={"topic": self.topic, "result": "success"},
                description="Total consumed events by topic and result",
            )
            return status
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
            self._metrics.counter_inc(
                "ledger_consume_total",
                labels={"topic": self.topic, "result": "dlq"},
                description="Total consumed events by topic and result",
            )
            return "DLQ"
        finally:
            self._metrics.histogram_observe(
                "ledger_consume_latency_ms",
                value=(time.perf_counter() - started) * 1000.0,
                labels={"topic": self.topic},
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

    def should_fail_payment(self, payload: dict[str, Any]) -> bool:
        forced_customers = {
            value.strip()
            for value in os.getenv("PAYMENT_FAIL_CUSTOMER_IDS", "").split(",")
            if value.strip()
        }

        customer_id = str(payload.get("customer_id", "")).strip()
        if customer_id and customer_id in forced_customers:
            return True

        # Explicit per-message override for deterministic test/demo flows.
        return bool(payload.get("force_payment_failure", False))

    def _is_processed(self, event_id: str) -> bool:
        with get_session() as session:
            existing = session.execute(
                select(ProcessedPaymentEvent).where(ProcessedPaymentEvent.event_id == event_id)
            ).scalar_one_or_none()
            return existing is not None

    def _mark_processed(self, event_id: str, event_type: str) -> None:
        with get_session() as session:
            session.add(ProcessedPaymentEvent(event_id=event_id, event_type=event_type))

    def handle_message(self, raw_message: bytes | str) -> str:
        if isinstance(raw_message, bytes):
            decoded = raw_message.decode("utf-8")
        else:
            decoded = raw_message

        body = self._codec.deserialize_for_topic(self.topic, decoded)
        event_id = str(body.get("event_id", ""))
        correlation_id = event_correlation_id(body)

        with correlation_scope(correlation_id):
            if event_id and self._is_processed(event_id):
                logger.info("payment_consumer_duplicate", extra={"extra_fields": {"event_id": event_id}})
                return "DUPLICATE"

            payload = body.get("payload", {})
            order_id = str(payload["order_id"])
            failed = self.should_fail_payment(payload)

            status = "FAILED" if failed else "COMPLETED"
            event_type = "PaymentFailed" if failed else "PaymentCompleted"
            topic = "payment.failed" if failed else "payment.completed"

            outcome_payload = {
                "order_id": order_id,
                "customer_id": payload.get("customer_id"),
                "status": status,
                "reason": "deterministic-demo-failure" if failed else "ok",
            }

            event = build_event(
                event_type=event_type,
                correlation_id=UUID(body["correlation_id"]),
                payload=outcome_payload,
            )
            self._producer.publish(topic=topic, key=order_id, payload=event.model_dump(mode="json"))
            if event_id:
                self._mark_processed(event_id=event_id, event_type=event_type)

            logger.info(
                "payment_consumer_processed",
                extra={"extra_fields": {"event_id": event_id, "order_id": order_id, "status": status, "out_topic": topic}},
            )
            return status
