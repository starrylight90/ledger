from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

from fastapi.testclient import TestClient

from shared.observability import reset_registry


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


def test_order_metrics_expose_labeled_series_and_dashboard_queries(tmp_path, monkeypatch):
    reset_registry("order-service")

    db_path = tmp_path / "phase5_order_metrics.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("KAFKA_BROKER", "localhost:9092")

    root = Path(__file__).resolve().parents[1]
    order_main = _load_module("order_main_phase5_observability", root / "services" / "order-service" / "main.py")
    order_models = _load_module("order_models_phase5_observability", root / "services" / "order-service" / "models.py")
    order_db = _load_module("order_db_phase5_observability", root / "services" / "order-service" / "db.py")
    order_models.Base.metadata.create_all(bind=order_db.engine)

    monkeypatch.setattr(order_main.inventory_grpc, "check_availability", lambda _sku, _qty: (True, 1000, "ok"))

    class _FakeProducer:
        def produce(self, topic, key, value):
            return None

        def flush(self, timeout=5):
            return None

    monkeypatch.setattr(order_main.producer, "_ensure", lambda: _FakeProducer())

    client = TestClient(order_main.app)
    response = client.post(
        "/orders",
        json={
            "customer_id": "phase5-observe",
            "idempotency_key": "phase5-observe-123456",
            "items": [{"sku": "sku-phase5", "qty": 1}],
        },
    )
    assert response.status_code == 202

    metrics_text = client.get("/metrics").text
    assert 'ledger_http_requests_total{method="POST",path="/orders",service="order-service",status="202"}' in metrics_text
    assert 'ledger_grpc_client_total{method="CheckAvailability",result="success",service="order-service"}' in metrics_text
    assert 'ledger_publish_total{result="success",service="order-service",topic="order.created"}' in metrics_text
    assert "ledger_http_request_latency_ms_bucket" in metrics_text

    dashboard_path = root / "monitoring" / "grafana" / "dashboards" / "ledger-phase0-overview.json"
    dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
    expressions = [target.get("expr", "") for panel in dashboard.get("panels", []) for target in panel.get("targets", [])]

    assert any("ledger_publish_total" in expr for expr in expressions)
    assert any("ledger_consumer_lag" in expr for expr in expressions)
    assert any("ledger_dlq_publish_total" in expr for expr in expressions)
    assert any("histogram_quantile(0.95" in expr for expr in expressions)
