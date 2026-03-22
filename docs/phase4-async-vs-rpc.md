# Phase 4 Architecture Decision: Async Messaging vs gRPC

## Decision Summary

Ledger uses both communication styles intentionally:

- gRPC for synchronous, latency-sensitive prechecks.
- Kafka events for asynchronous workflow progression and compensating transactions.

## Why gRPC For Inventory Precheck

Order acceptance is a user-facing action that needs an immediate answer:

- Is this SKU currently available?
- If not, how much stock remains?

A synchronous RPC call is the correct fit because the caller is blocked waiting for a deterministic answer.

## Why Kafka For The Rest Of The Flow

After order acceptance, the system benefits from event-driven decoupling:

- inventory reservation
- payment processing
- compensation and cancellation
- notification delivery

Kafka provides durability, replayability, and service independence for these stages.

## Trade-Off Matrix

- gRPC strengths:
  - low-latency request/response
  - explicit contract via `.proto`
  - clear per-call error semantics
- gRPC limits:
  - tighter service coupling
  - direct availability dependency at request time

- Kafka strengths:
  - loose coupling and async scaling
  - durable event log and replay patterns
  - natural fit for saga choreography
- Kafka limits:
  - eventual consistency
  - more complex failure debugging without good observability

## Applied Rule In Ledger

1. If the user request needs immediate admission control, use gRPC.
2. If the task is part of distributed workflow progression, use Kafka.
3. Keep contracts explicit in both paths:
   - `.proto` for gRPC
   - Avro schema + registry for events
