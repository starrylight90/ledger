from __future__ import annotations

import json
import os
from typing import Any

from shared.avro_codec import AvroCodec


class KafkaProducerClient:
    def __init__(self, broker: str | None = None) -> None:
        self._broker = broker or os.getenv("KAFKA_BROKER", "localhost:9092")
        self._producer = None
        self._codec = AvroCodec()

    def _ensure(self) -> Any:
        if self._producer is not None:
            return self._producer

        try:
            from confluent_kafka import Producer
        except ImportError as exc:
            raise RuntimeError(
                "confluent-kafka is required for payment event publishing. Install dependencies first."
            ) from exc

        self._producer = Producer({"bootstrap.servers": self._broker, "acks": "all"})
        return self._producer

    def publish(self, topic: str, key: str, payload: dict[str, Any]) -> None:
        producer = self._ensure()
        serialized = self._codec.serialize_for_topic(topic, payload)
        producer.produce(topic=topic, key=key.encode("utf-8"), value=serialized)
        producer.flush(timeout=5)
