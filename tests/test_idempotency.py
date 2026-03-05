from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from fastapi.testclient import TestClient


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


def test_duplicate_idempotency_key_returns_existing_order(tmp_path, monkeypatch):
    db_path = tmp_path / "idempotency.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")

    root = Path(__file__).resolve().parents[1]
    order_main = _load_module("order_main_idempotency", root / "services" / "order-service" / "main.py")
    order_models = _load_module("order_models_idempotency", root / "services" / "order-service" / "models.py")
    order_db = _load_module("order_db_idempotency", root / "services" / "order-service" / "db.py")

    order_models.Base.metadata.create_all(bind=order_db.engine)

    produced: list[dict] = []

    def fake_publish(topic: str, key: str, payload: dict):
        produced.append({"topic": topic, "key": key, "payload": payload})

    monkeypatch.setattr(order_main.producer, "publish", fake_publish)

    client = TestClient(order_main.app)
    payload = {
        "customer_id": "customer-dup",
        "idempotency_key": "idempotency-duplicate-0001",
        "items": [{"sku": "sku-dup", "qty": 1}],
    }

    first = client.post("/orders", json=payload)
    second = client.post("/orders", json=payload)

    assert first.status_code == 202
    assert second.status_code == 202

    first_body = first.json()
    second_body = second.json()

    assert first_body["order_id"] == second_body["order_id"]
    assert first_body["correlation_id"] == second_body["correlation_id"]
    assert len(produced) == 1
