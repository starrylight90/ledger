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


def test_payment_failure_triggers_compensating_rollback(tmp_path, monkeypatch):
    db_path = tmp_path / "phase2_failure.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("KAFKA_BROKER", "localhost:9092")
    monkeypatch.setenv("PAYMENT_FAIL_CUSTOMER_IDS", "customer-fail")

    root = Path(__file__).resolve().parents[1]

    order_main = _load_module("order_main_failure", root / "services" / "order-service" / "main.py")
    order_models = _load_module("order_models_failure", root / "services" / "order-service" / "models.py")
    order_db = _load_module("order_db_failure", root / "services" / "order-service" / "db.py")
    order_consumer_mod = _load_module("order_consumer_failure", root / "services" / "order-service" / "consumer.py")

    inventory_consumer_mod = _load_module(
        "inventory_consumer_failure", root / "services" / "inventory-service" / "consumer.py"
    )
    inventory_models = _load_module("inventory_models_failure", root / "services" / "inventory-service" / "models.py")
    inventory_db = _load_module("inventory_db_failure", root / "services" / "inventory-service" / "db.py")

    payment_consumer_mod = _load_module("payment_consumer_failure", root / "services" / "payment-service" / "consumer.py")
    payment_models = _load_module("payment_models_failure", root / "services" / "payment-service" / "models.py")
    payment_db = _load_module("payment_db_failure", root / "services" / "payment-service" / "db.py")

    notification_consumer_mod = _load_module(
        "notification_consumer_failure", root / "services" / "notification-service" / "consumer.py"
    )
    notification_models = _load_module(
        "notification_models_failure", root / "services" / "notification-service" / "models.py"
    )
    notification_db = _load_module("notification_db_failure", root / "services" / "notification-service" / "db.py")

    order_models.Base.metadata.create_all(bind=order_db.engine)
    inventory_models.Base.metadata.create_all(bind=inventory_db.engine)
    payment_models.Base.metadata.create_all(bind=payment_db.engine)
    notification_models.Base.metadata.create_all(bind=notification_db.engine)

    with inventory_db.get_session() as session:
        session.add(inventory_models.InventoryStock(sku="sku-2", quantity_available=5))

    produced_from_order: list[dict] = []
    produced_from_inventory: list[dict] = []
    produced_from_payment: list[dict] = []
    produced_compensation: list[dict] = []

    monkeypatch.setattr(
        order_main.producer,
        "publish",
        lambda topic, key, payload: produced_from_order.append({"topic": topic, "key": key, "payload": payload}),
    )

    inventory_consumer = inventory_consumer_mod.InventoryConsumer()

    def inventory_publish(topic: str, key: str, payload: dict):
        record = {"topic": topic, "key": key, "payload": payload}
        if topic == "order.cancelled":
            produced_compensation.append(record)
        else:
            produced_from_inventory.append(record)

    monkeypatch.setattr(inventory_consumer._producer, "publish", inventory_publish)

    payment_consumer = payment_consumer_mod.PaymentConsumer()
    monkeypatch.setattr(
        payment_consumer._producer,
        "publish",
        lambda topic, key, payload: produced_from_payment.append({"topic": topic, "key": key, "payload": payload}),
    )

    order_cancelled_consumer = order_consumer_mod.OrderCancelledConsumer()
    notification_consumer = notification_consumer_mod.NotificationConsumer()

    client = TestClient(order_main.app)
    create_resp = client.post(
        "/orders",
        json={
            "customer_id": "customer-fail",
            "idempotency_key": "phase2-fail-123456",
            "items": [{"sku": "sku-2", "qty": 2}],
        },
    )
    assert create_resp.status_code == 202

    inventory_consumer.handle_message(json.dumps(produced_from_order[0]["payload"]))
    assert produced_from_inventory[0]["topic"] == "inventory.reserved"

    payment_consumer.handle_message(json.dumps(produced_from_inventory[0]["payload"]))
    assert produced_from_payment[0]["topic"] == "payment.failed"

    inventory_consumer.handle_payment_failed_message(json.dumps(produced_from_payment[0]["payload"]))
    assert produced_compensation and produced_compensation[0]["topic"] == "order.cancelled"

    order_cancelled_consumer.handle_message(json.dumps(produced_compensation[0]["payload"]))
    notification_id = notification_consumer.handle_message(json.dumps(produced_compensation[0]["payload"]))
    assert notification_id > 0

    order_id = create_resp.json()["order_id"]
    with order_db.get_session() as session:
        order = session.query(order_models.Order).filter_by(order_id=order_id).one()
        assert order.status == "CANCELLED"
