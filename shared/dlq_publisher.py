from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any


class DLQPublisher:
    def __init__(self, broker: str | None = None) -> None:
        self._broker = broker or os.getenv("KAFKA_BROKER", "localhost:9092")
        self._producer = None

    def _ensure(self) -> Any:
        if self._producer is not None:
            return self._producer

        try:
            from confluent_kafka import Producer
        except ImportError as exc:
            raise RuntimeError(
                "confluent-kafka is required for DLQ publishing. Install dependencies first."
            ) from exc

        self._producer = Producer({"bootstrap.servers": self._broker, "acks": "all"})
        return self._producer

    def publish(
        self,
        *,
        source_topic: str,
        key: str,
        original_payload: dict[str, Any],
        failure_reason: str,
        retry_count: int,
    ) -> str:
        dlq_topic = f"{source_topic}.dlq"
        envelope = {
            "source_topic": source_topic,
            "failed_at": datetime.now(UTC).isoformat(),
            "failure_reason": failure_reason,
            "retry_count": retry_count,
            "original_payload": original_payload,
        }

        producer = self._ensure()
        producer.produce(
            topic=dlq_topic,
            key=key.encode("utf-8"),
            value=json.dumps(envelope).encode("utf-8"),
        )
        producer.flush(timeout=5)
        return dlq_topic
