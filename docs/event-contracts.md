# Ledger Event Contracts And Topic Convention

This document defines the event envelope and topic naming contract for all services.

## Topic Naming Convention

- `order.created`
- `inventory.reserved`
- `inventory.reservation-failed`
- `payment.completed`
- `payment.failed`
- `order.cancelled`
- `notification.send`

Dead-letter queue topics append `.dlq` to the source topic name.

Examples:

- `order.created.dlq`
- `payment.failed.dlq`

## Event Envelope

All services must publish events using this envelope:

```json
{
  "event_id": "uuid",
  "event_type": "OrderCreated",
  "timestamp": "2026-02-18T22:50:00Z",
  "correlation_id": "uuid",
  "payload": {}
}
```

## Contract Rules

- `event_id` must be globally unique.
- `correlation_id` must be generated once by the entry service and propagated unchanged.
- `event_type` must be stable and versioned only through schema evolution policy.
- `payload` is service-specific but must be validated at producer and consumer boundaries.

## Delivery Guarantees

Phase 0-2 baseline is at-least-once processing.
Idempotent consumers are required to tolerate duplicates.
