from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def _load_module(module_name: str, file_path: Path):
    service_dir = str(file_path.parent)
    for local_name in ("db", "models", "schemas", "consumer", "kafka_producer"):
        sys.modules.pop(local_name, None)
    if service_dir not in sys.path:
        sys.path.insert(0, service_dir)

    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_inventory_is_restored_after_payment_failed(tmp_path, monkeypatch):
    db_path = tmp_path / "phase2_inventory.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("KAFKA_BROKER", "localhost:9092")

    root = Path(__file__).resolve().parents[1]

    inventory_consumer_mod = _load_module(
        "inventory_consumer_restore", root / "services" / "inventory-service" / "consumer.py"
    )
    inventory_models = _load_module(
        "inventory_models_restore", root / "services" / "inventory-service" / "models.py"
    )
    inventory_db = _load_module("inventory_db_restore", root / "services" / "inventory-service" / "db.py")

    inventory_models.Base.metadata.create_all(bind=inventory_db.engine)

    with inventory_db.get_session() as session:
        session.add(inventory_models.InventoryStock(sku="sku-3", quantity_available=3))

    consumer = inventory_consumer_mod.InventoryConsumer()
    published: list[dict] = []
    monkeypatch.setattr(
        consumer._producer,
        "publish",
        lambda topic, key, payload: published.append({"topic": topic, "key": key, "payload": payload}),
    )

    order_created = {
        "event_id": "9dbe8f8a-48f9-4a6e-8cf5-24ba66d0f11d",
        "event_type": "OrderCreated",
        "timestamp": "2026-03-05T18:00:00Z",
        "correlation_id": "8b833be3-3277-46dd-97aa-4d17f947f74f",
        "payload": {
            "order_id": "order-restore-1",
            "customer_id": "cust-restore",
            "items": [{"sku": "sku-3", "qty": 2}],
        },
    }

    consumer.handle_message(json.dumps(order_created))

    with inventory_db.get_session() as session:
        stock = session.query(inventory_models.InventoryStock).filter_by(sku="sku-3").one()
        reservation = session.query(inventory_models.InventoryReservation).filter_by(order_id="order-restore-1").one()
        assert stock.quantity_available == 1
        assert reservation.status == "RESERVED"

    payment_failed = {
        "event_id": "0f99ea59-ec6e-4f3e-9dbb-b8d4420779fb",
        "event_type": "PaymentFailed",
        "timestamp": "2026-03-05T18:05:00Z",
        "correlation_id": "8b833be3-3277-46dd-97aa-4d17f947f74f",
        "payload": {
            "order_id": "order-restore-1",
            "customer_id": "cust-restore",
            "status": "FAILED",
            "reason": "deterministic-demo-failure",
        },
    }

    consumer.handle_payment_failed_message(json.dumps(payment_failed))

    with inventory_db.get_session() as session:
        stock = session.query(inventory_models.InventoryStock).filter_by(sku="sku-3").one()
        reservation = session.query(inventory_models.InventoryReservation).filter_by(order_id="order-restore-1").one()
        assert stock.quantity_available == 3
        assert reservation.status == "RELEASED"

    assert any(item["topic"] == "order.cancelled" for item in published)
