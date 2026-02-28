from __future__ import annotations

import json
import os
from typing import Any
from uuid import UUID

from sqlalchemy import select

from db import get_session
from kafka_producer import KafkaProducerClient
from models import ProcessedPaymentEvent
from shared.event_schemas import build_event


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

    def _ensure(self) -> Any:
        if self._consumer is not None:
            return self._consumer

        try:
            from confluent_kafka import Consumer
        except ImportError as exc:
            raise RuntimeError(
                "confluent-kafka is required for payment consumption. Install project dependencies first."
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

        body = json.loads(decoded)
        event_id = str(body.get("event_id", ""))
        if event_id and self._is_processed(event_id):
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
        return status
