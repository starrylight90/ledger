from __future__ import annotations

import importlib.util
import json
import logging
import sys
from pathlib import Path

from shared.correlation import clear_correlation_id, correlation_scope, get_correlation_id
from shared.logging_utils import JsonLogFormatter


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


def test_json_logs_include_correlation_id_from_context():
    formatter = JsonLogFormatter()
    record = logging.LogRecord(
        name="ledger",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="event processed",
        args=(),
        exc_info=None,
    )

    with correlation_scope("corr-phase5-abc"):
        payload = json.loads(formatter.format(record))

    assert payload["correlation_id"] == "corr-phase5-abc"
    assert payload["message"] == "event processed"


def test_correlation_id_propagates_across_inventory_to_payment_events(tmp_path, monkeypatch):
    clear_correlation_id()
    db_path = tmp_path / "phase5_trace.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("KAFKA_BROKER", "localhost:9092")

    root = Path(__file__).resolve().parents[1]
    inventory_models = _load_module("inventory_models_phase5_trace", root / "services" / "inventory-service" / "models.py")
    inventory_db = _load_module("inventory_db_phase5_trace", root / "services" / "inventory-service" / "db.py")
    inventory_consumer_mod = _load_module("inventory_consumer_phase5_trace", root / "services" / "inventory-service" / "consumer.py")

    payment_models = _load_module("payment_models_phase5_trace", root / "services" / "payment-service" / "models.py")
    payment_db = _load_module("payment_db_phase5_trace", root / "services" / "payment-service" / "db.py")
    payment_consumer_mod = _load_module("payment_consumer_phase5_trace", root / "services" / "payment-service" / "consumer.py")

    inventory_models.Base.metadata.create_all(bind=inventory_db.engine)
    payment_models.Base.metadata.create_all(bind=payment_db.engine)
    with inventory_db.get_session() as session:
        session.add(inventory_models.InventoryStock(sku="sku-trace", quantity_available=5))

    inventory_consumer = inventory_consumer_mod.InventoryConsumer()
    payment_consumer = payment_consumer_mod.PaymentConsumer()

    produced_from_inventory: list[dict] = []
    produced_from_payment: list[dict] = []

    monkeypatch.setattr(
        inventory_consumer._producer,
        "publish",
        lambda topic, key, payload: produced_from_inventory.append({"topic": topic, "key": key, "payload": payload}),
    )
    monkeypatch.setattr(
        payment_consumer._producer,
        "publish",
        lambda topic, key, payload: produced_from_payment.append({"topic": topic, "key": key, "payload": payload}),
    )

    incoming = {
        "event_id": "4bbf1d14-f89c-4e49-a236-91fa0406f8e2",
        "event_type": "OrderCreated",
        "timestamp": "2026-03-19T22:00:00+00:00",
        "correlation_id": "65a5461c-6ec3-4d2f-9be3-f8f80faef337",
        "payload": {
            "order_id": "order-trace-1",
            "customer_id": "cust-trace-1",
            "items": [{"sku": "sku-trace", "qty": 2}],
        },
    }

    reserve_status = inventory_consumer.handle_message(json.dumps(incoming).encode("utf-8"))
    assert reserve_status == "RESERVED"
    assert produced_from_inventory
    assert produced_from_inventory[0]["payload"]["correlation_id"] == incoming["correlation_id"]

    payment_status = payment_consumer.handle_message(
        json.dumps(produced_from_inventory[0]["payload"]).encode("utf-8")
    )
    assert payment_status in {"COMPLETED", "FAILED"}
    assert produced_from_payment
    assert produced_from_payment[0]["payload"]["correlation_id"] == incoming["correlation_id"]
    assert get_correlation_id() is None
