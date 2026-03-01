# Phase 2 Rollback Demo Runbook

## Objective

Demonstrate deterministic payment failure and compensating inventory rollback in an end-to-end saga flow.

## Preconditions

- Infrastructure running: `docker compose up -d`
- Services available:
  - order-service
  - inventory-service
  - payment-service
  - notification-service
- Environment variable set for deterministic failure:
  - `PAYMENT_FAIL_CUSTOMER_IDS=customer-fail`

## Demo Request

Submit order with forced-failure customer:

```json
{
  "customer_id": "customer-fail",
  "idempotency_key": "demo-rollback-001",
  "items": [{"sku": "sku-2", "qty": 2}]
}
```

## Expected Event Timeline

1. `order.created`
2. `inventory.reserved`
3. `payment.failed`
4. `order.cancelled`

## Expected Data Outcomes

- Order status becomes `CANCELLED`.
- Inventory reservation status becomes `RELEASED`.
- Stock quantity returns to pre-reservation level.
- Notification log contains cancellation event.

## Verification Queries (SQLite-compatible shape)

- Orders: `SELECT order_id, status FROM orders WHERE idempotency_key = 'demo-rollback-001';`
- Inventory reservations: `SELECT order_id, status, qty FROM inventory_reservations WHERE order_id = '<order_id>';`
- Notification logs: `SELECT order_id, event_type, message FROM notification_logs WHERE order_id = '<order_id>' ORDER BY created_at DESC;`

## Demo Narrative

"This order was accepted asynchronously and reserved inventory first. Payment then failed deterministically for this customer. The inventory service compensated by restoring stock and publishing order cancellation. The order service applied cancellation, and notification service emitted the terminal state."
