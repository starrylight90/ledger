from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

from fastapi.testclient import TestClient


def _load_module(module_name: str, file_path: Path):
    service_dir = str(file_path.parent)
    for local_name in ("db", "models", "schemas", "consumer", "kafka_producer", "grpc_client", "grpc_server"):
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


def test_grpc_contract_and_order_precheck_flow(tmp_path, monkeypatch):
    db_path = tmp_path / "phase4_grpc.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("KAFKA_BROKER", "localhost:9092")

    root = Path(__file__).resolve().parents[1]

    inventory_models = _load_module("inventory_models_phase4", root / "services" / "inventory-service" / "models.py")
    inventory_db = _load_module("inventory_db_phase4", root / "services" / "inventory-service" / "db.py")
    grpc_server_mod = _load_module("inventory_grpc_server_phase4", root / "services" / "inventory-service" / "grpc_server.py")

    inventory_models.Base.metadata.create_all(bind=inventory_db.engine)
    with inventory_db.get_session() as session:
        session.add(inventory_models.InventoryStock(sku="sku-grpc", quantity_available=3))

    grpc_server = grpc_server_mod.build_server()
    port = grpc_server.add_insecure_port("127.0.0.1:0")
    grpc_server.start()

    try:
        monkeypatch.setenv("INVENTORY_GRPC_CLIENT_HOST", "127.0.0.1")
        monkeypatch.setenv("INVENTORY_GRPC_CLIENT_PORT", str(port))
        monkeypatch.setenv("INVENTORY_GRPC_TIMEOUT_SECONDS", "1.0")

        order_main = _load_module("order_main_phase4", root / "services" / "order-service" / "main.py")
        order_models = _load_module("order_models_phase4", root / "services" / "order-service" / "models.py")
        order_db = _load_module("order_db_phase4", root / "services" / "order-service" / "db.py")
        order_models.Base.metadata.create_all(bind=order_db.engine)

        published: list[dict] = []
        monkeypatch.setattr(
            order_main.producer,
            "publish",
            lambda topic, key, payload: published.append({"topic": topic, "key": key, "payload": payload}),
        )

        client = TestClient(order_main.app)

        ok = client.post(
            "/orders",
            json={
                "customer_id": "cust-grpc",
                "idempotency_key": "grpc-ok-123456",
                "items": [{"sku": "sku-grpc", "qty": 2}],
            },
        )
        assert ok.status_code == 202
        assert published and published[0]["topic"] == "order.created"

        conflict = client.post(
            "/orders",
            json={
                "customer_id": "cust-grpc",
                "idempotency_key": "grpc-conflict-123456",
                "items": [{"sku": "sku-grpc", "qty": 99}],
            },
        )
        assert conflict.status_code == 409
        assert conflict.json()["detail"]["code"] == "inventory_not_available"
    finally:
        grpc_server.stop(grace=1)


def test_grpc_unavailable_returns_503(tmp_path, monkeypatch):
    db_path = tmp_path / "phase4_unavailable.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("KAFKA_BROKER", "localhost:9092")
    monkeypatch.setenv("INVENTORY_GRPC_CLIENT_HOST", "127.0.0.1")
    monkeypatch.setenv("INVENTORY_GRPC_CLIENT_PORT", "59999")
    monkeypatch.setenv("INVENTORY_GRPC_TIMEOUT_SECONDS", "0.2")

    root = Path(__file__).resolve().parents[1]
    order_main = _load_module("order_main_phase4_unavailable", root / "services" / "order-service" / "main.py")

    client = TestClient(order_main.app)
    response = client.post(
        "/orders",
        json={
            "customer_id": "cust-grpc",
            "idempotency_key": "grpc-unavailable-123456",
            "items": [{"sku": "sku-grpc", "qty": 1}],
        },
    )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "inventory_grpc_unavailable"
