# Phase 5 Observability Triage Playbook

This playbook is the fastest way to determine whether Ledger runtime behavior is healthy.

## 1) External System Reachability

Check infrastructure first:

- Prometheus: `http://localhost:9090/-/healthy`
- Grafana: `http://localhost:3000/api/health`
- Redpanda Console: `http://localhost:8080`
- Schema Registry: `http://localhost:8081/subjects`

If any endpoint fails, inspect compose status:

- `docker compose ps`
- `docker logs <container-name> --tail 200`

## 2) Prometheus Target Health

Open `http://localhost:9090/targets` and verify jobs are `UP`:

- `ledger-order-service`
- `ledger-inventory-service`
- `ledger-payment-service`
- `ledger-notification-service`
- `redpanda`
- `prometheus`

If service jobs are `DOWN`, confirm the service process is running and listening on expected ports.

## 3) Grafana Dashboard Checks

Open dashboard UID `ledger-phase5`.

Expected healthy signals:

- Publish and consume throughput are non-zero during load
- p95 publish/consume latency remains stable
- max consumer lag trends back to zero
- DLQ events/sec remains zero in normal flows

## 4) High-Signal PromQL Queries

Use these queries during incidents:

- Publish throughput:
  - `sum(rate(ledger_publish_total{result="success"}[5m]))`
- Consume throughput:
  - `sum(rate(ledger_consume_total{result="success"}[5m]))`
- p95 consume latency:
  - `histogram_quantile(0.95, sum(rate(ledger_consume_latency_ms_bucket[5m])) by (le,service))`
- p95 publish latency:
  - `histogram_quantile(0.95, sum(rate(ledger_publish_latency_ms_bucket[5m])) by (le,service))`
- Maximum lag:
  - `max(ledger_consumer_lag)`
- DLQ rate:
  - `sum(rate(ledger_dlq_publish_total{result="success"}[5m]))`

## 5) Correlation-Driven Drill Down

When a request or order is problematic:

1. Capture correlation id from JSON logs.
2. Filter logs across services by that id.
3. Confirm path:
   - order accepted
   - inventory reservation event emitted
   - payment event emitted
   - notification event emitted
4. If flow diverges, correlate with lag and DLQ metrics for the same time window.

## 6) Typical Failure Patterns

- Throughput drops + lag rises:
  - consumer processing bottleneck or downstream dependency issue.
- DLQ increases + retries spike:
  - poison messages or schema mismatch.
- HTTP 503 spike on `/orders`:
  - gRPC inventory check unavailable.
- gRPC latency grows without errors:
  - inventory service degradation.
