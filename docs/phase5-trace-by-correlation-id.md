# Phase 5 Demo: Trace One Order By Correlation ID

This walkthrough demonstrates end-to-end traceability across the saga chain.

## 1) Start Infrastructure

- `docker compose up -d`
- Confirm:
  - `docker compose ps`

## 2) Start Services

Run each service locally (separate terminals):

- order-service on `:8000`
- inventory-service on `:8001`
- payment-service on `:8002`
- notification-service on `:8003`

## 3) Submit One Order

Example payload:

```json
{
  "customer_id": "demo-corr-1",
  "idempotency_key": "demo-corr-1-123456",
  "items": [{"sku": "sku-demo", "qty": 1}]
}
```

`POST http://localhost:8000/orders`

Capture response fields:

- `order_id`
- `correlation_id`

## 4) Follow Correlation Across Logs

Filter logs by `correlation_id` and verify sequence:

1. order-service emits `order.created`
2. inventory-service emits `inventory.reserved` or `inventory.reservation-failed`
3. payment-service emits `payment.completed` or `payment.failed`
4. notification-service records terminal notification

## 5) Validate Metrics Alignment

During the same time window, check:

- `ledger_publish_total`
- `ledger_consume_total`
- `ledger_consume_latency_ms_bucket`
- `ledger_consumer_lag`
- `ledger_dlq_publish_total`

Expected normal flow:

- publish/consume counters increment
- lag remains bounded and returns near zero
- DLQ counter does not increment

## 6) Failure Flow Variant

Force payment failure by setting customer id in `PAYMENT_FAIL_CUSTOMER_IDS`.

Expected trace:

- inventory reservation created
- payment failure event emitted
- inventory compensation publishes `order.cancelled`
- order status transitions to `CANCELLED`
- notification emits cancellation event

This proves correlation IDs remain stable through both happy and compensating paths.
