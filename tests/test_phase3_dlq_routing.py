from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def _load_module(module_name: str, file_path: Path):
    service_dir = str(file_path.parent)
    for local_name in ("db", "models", "schemas", "consumer", "kafka_producer"):
        sys.modules.pop(local_name, None)
    if service_dir in sys.path:
        sys.path.remove(service_dir)
    sys.path.insert(0, service_dir)

    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class FakeMessage:
    def __init__(self, value: bytes):
        self._value = value

    def error(self):
        return None

    def value(self):
        return self._value


def test_inventory_consumer_routes_exhausted_retries_to_dlq(tmp_path, monkeypatch):
    db_path = tmp_path / "phase3_dlq.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("KAFKA_BROKER", "localhost:9092")
    monkeypatch.setenv("CONSUMER_MAX_ATTEMPTS", "2")

    root = Path(__file__).resolve().parents[1]
    inventory_consumer_mod = _load_module(
        "inventory_consumer_dlq", root / "services" / "inventory-service" / "consumer.py"
    )

    consumer = inventory_consumer_mod.InventoryConsumer()

    commits = {"count": 0}

    class FakeKafkaConsumer:
        def commit(self, message, asynchronous=False):
            commits["count"] += 1

    consumer._consumer = FakeKafkaConsumer()

    dlq_calls: list[dict] = []
    monkeypatch.setattr(
        consumer._dlq,
        "publish",
        lambda **kwargs: dlq_calls.append(kwargs) or "order.created.dlq",
    )

    def always_fail(_raw):
        raise TimeoutError("temporary downstream failure")

    monkeypatch.setattr(consumer, "handle_message", always_fail)

    event = {
        "event_id": "d7ea9be1-8f49-4cc4-b1a2-4db04a38fcb2",
        "event_type": "OrderCreated",
        "correlation_id": "65a5461c-6ec3-4d2f-9be3-f8f80faef337",
        "payload": {"order_id": "order-dlq-1", "items": [{"sku": "x", "qty": 1}]},
    }
    message = FakeMessage(json.dumps(event).encode("utf-8"))

    result = consumer.process_polled_message(message)

    assert result == "DLQ"
    assert commits["count"] == 1
    assert dlq_calls and dlq_calls[0]["source_topic"] == "order.created"
    assert dlq_calls[0]["retry_count"] == 2
