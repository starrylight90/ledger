from __future__ import annotations

import os
import time
from typing import Any

from shared.avro_codec import AvroCodec
from shared.observability import get_registry


class KafkaProducerClient:
    def __init__(self, broker: str | None = None) -> None:
        self._broker = broker or os.getenv("KAFKA_BROKER", "localhost:9092")
        self._producer = None
        self._codec = AvroCodec()
        self._metrics = get_registry("order-service")

    def _ensure(self) -> Any:
        if self._producer is not None:
            return self._producer

        try:
            from confluent_kafka import Producer
        except ImportError as exc:
            raise RuntimeError(
                "confluent-kafka is required for event publishing. Install project dependencies first."
            ) from exc

        self._producer = Producer({"bootstrap.servers": self._broker, "acks": "all"})
        return self._producer

    def publish(self, topic: str, key: str, payload: dict[str, Any]) -> None:
        start = time.perf_counter()
        producer = self._ensure()
        serialized = self._codec.serialize_for_topic(topic, payload)
        try:
            producer.produce(topic=topic, key=key.encode("utf-8"), value=serialized)
            producer.flush(timeout=5)
            self._metrics.counter_inc(
                "ledger_publish_total",
                labels={"topic": topic, "result": "success"},
                description="Total published events by topic and result",
            )
        except Exception:
            self._metrics.counter_inc(
                "ledger_publish_total",
                labels={"topic": topic, "result": "error"},
                description="Total published events by topic and result",
            )
            raise
        finally:
            self._metrics.histogram_observe(
                "ledger_publish_latency_ms",
                value=(time.perf_counter() - start) * 1000.0,
                labels={"topic": topic},
                description="Kafka publish latency in milliseconds",
            )
