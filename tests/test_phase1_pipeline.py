from __future__ import annotations

import importlib.util
import json
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


def test_order_to_inventory_pipeline_sqlite(tmp_path, monkeypatch):
    db_path = tmp_path / "phase1.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("KAFKA_BROKER", "localhost:9092")

    root = Path(__file__).resolve().parents[1]
    order_main = _load_module("order_main", root / "services" / "order-service" / "main.py")
    order_models = _load_module("order_models", root / "services" / "order-service" / "models.py")
    order_db = _load_module("order_db", root / "services" / "order-service" / "db.py")

    inventory_consumer_mod = _load_module(
        "inventory_consumer", root / "services" / "inventory-service" / "consumer.py"
    )
    inventory_models = _load_module("inventory_models", root / "services" / "inventory-service" / "models.py")
    inventory_db = _load_module("inventory_db", root / "services" / "inventory-service" / "db.py")

    order_models.Base.metadata.create_all(bind=order_db.engine)
    inventory_models.Base.metadata.create_all(bind=inventory_db.engine)

    with inventory_db.get_session() as session:
        session.add(inventory_models.InventoryStock(sku="sku-1", quantity_available=10))

    produced_by_order: list[dict] = []

    def fake_publish_order(topic: str, key: str, payload: dict):
        produced_by_order.append({"topic": topic, "key": key, "payload": payload})

    monkeypatch.setattr(order_main.producer, "publish", fake_publish_order)
    monkeypatch.setattr(order_main.inventory_grpc, "check_availability", lambda _sku, _qty: (True, 1000, "ok"))

    client = TestClient(order_main.app)
    response = client.post(
        "/orders",
        json={
            "customer_id": "customer-1",
            "idempotency_key": "idempotency-12345678",
            "items": [{"sku": "sku-1", "qty": 2}],
        },
    )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "PENDING"
    assert produced_by_order and produced_by_order[0]["topic"] == "order.created"

    produced_by_inventory: list[dict] = []
    consumer = inventory_consumer_mod.InventoryConsumer()

    def fake_publish_inventory(topic: str, key: str, payload: dict):
        produced_by_inventory.append({"topic": topic, "key": key, "payload": payload})

    monkeypatch.setattr(consumer._producer, "publish", fake_publish_inventory)

    message_json = json.dumps(produced_by_order[0]["payload"])
    status = consumer.handle_message(message_json)

    assert status == "RESERVED"
    assert produced_by_inventory and produced_by_inventory[0]["topic"] == "inventory.reserved"

    with inventory_db.get_session() as session:
        stock = session.query(inventory_models.InventoryStock).filter_by(sku="sku-1").one()
        assert stock.quantity_available == 8
