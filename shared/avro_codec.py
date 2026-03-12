from __future__ import annotations

import json
from typing import Any

from shared.schema_registry import SchemaRegistryClient


class AvroCodec:
    def __init__(self, registry: SchemaRegistryClient | None = None) -> None:
        self.registry = registry or SchemaRegistryClient()

    def serialize_for_topic(self, topic: str, payload: dict[str, Any]) -> bytes:
        self.registry.ensure_registered(topic)
        self.registry.validate(topic, payload)
        return json.dumps(payload).encode("utf-8")

    def deserialize_for_topic(self, topic: str, raw: bytes | str) -> dict[str, Any]:
        if isinstance(raw, bytes):
            decoded = raw.decode("utf-8")
        else:
            decoded = raw
        payload = json.loads(decoded)
        self.registry.validate(topic, payload)
        return payload
