from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime
from typing import Any

from shared.observability import get_registry


class DLQPublisher:
    def __init__(self, broker: str | None = None, service_name: str | None = None) -> None:
        self._broker = broker or os.getenv("KAFKA_BROKER", "localhost:9092")
        self._producer = None
        self._metrics = get_registry(service_name or os.getenv("LEDGER_SERVICE_NAME", "shared"))

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
        started = time.perf_counter()
        dlq_topic = f"{source_topic}.dlq"
        envelope = {
            "source_topic": source_topic,
            "failed_at": datetime.now(UTC).isoformat(),
            "failure_reason": failure_reason,
            "retry_count": retry_count,
            "original_payload": original_payload,
        }

        producer = self._ensure()
        try:
            producer.produce(
                topic=dlq_topic,
                key=key.encode("utf-8"),
                value=json.dumps(envelope).encode("utf-8"),
            )
            producer.flush(timeout=5)
            self._metrics.counter_inc(
                "ledger_dlq_publish_total",
                labels={"source_topic": source_topic, "dlq_topic": dlq_topic, "result": "success"},
                description="Total DLQ publish attempts by source and result",
            )
        except Exception:
            self._metrics.counter_inc(
                "ledger_dlq_publish_total",
                labels={"source_topic": source_topic, "dlq_topic": dlq_topic, "result": "error"},
                description="Total DLQ publish attempts by source and result",
            )
            raise
        finally:
            self._metrics.histogram_observe(
                "ledger_dlq_publish_latency_ms",
                value=(time.perf_counter() - started) * 1000.0,
                labels={"source_topic": source_topic, "dlq_topic": dlq_topic},
                description="DLQ publish latency in milliseconds",
            )
        return dlq_topic
