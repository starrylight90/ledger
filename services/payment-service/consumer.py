from __future__ import annotations

import os
from typing import Any


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
