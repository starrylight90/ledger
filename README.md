# Ledger

Ledger is an event-driven order processing platform focused on production-grade distributed systems patterns:

- asynchronous workflow orchestration using Kafka-compatible messaging
- saga-based compensating transactions
- idempotent processing, retries, and DLQ handling
- schema-governed event contracts
- observability with metrics and dashboards

## Phase 0 Status

Phase 0 establishes local infrastructure and conventions:

- Redpanda broker
- PostgreSQL with service schemas
- Prometheus and Grafana
- Redpanda Console for topic visibility
- event/topic contract documentation
- smoke test script for producer-consumer round-trip

## Phase 3 Reliability Status

Phase 3 introduces production-grade reliability controls:

- transient-vs-poison failure classification
- bounded retries with exponential backoff
- per-topic dead-letter queue routing on retry exhaustion
- schema validation on producer and consumer boundaries
- DLQ inspection and replay planning via script and ops endpoint

Key tools:

- `python scripts/dlq_inspector.py --input <dlq-record.json>`
- `POST /ops/dlq/inspect`

Phase 3 docs:

- `docs/phase3-reliability-runbook.md`
- `docs/phase3-schema-versioning.md`
- `docs/phase3-break-demo.md`

## Quick Start

1. Copy environment template:

   - `cp .env.example .env` (macOS/Linux)
   - `Copy-Item .env.example .env` (PowerShell)

2. Start infrastructure:

   - `docker compose up -d`

3. Validate broker round trip:

   - `./scripts/smoke_roundtrip.ps1`

## Service Topology

- `redpanda` at `localhost:9092`
- `postgres` at `localhost:5432`
- `prometheus` at `localhost:9090`
- `grafana` at `localhost:3000`
- `redpanda-console` at `localhost:8080`

## Phase Progression

- Phase 0: environment scaffolding
- Phase 1: order ingest to inventory reservation pipeline
- Phase 2: payment/notification choreography and rollback
- Phase 3: retries, DLQ, schema registry
- Phase 4: gRPC synchronous boundary
- Phase 5: observability hardening
- Phase 6: load testing and release documentation

## Troubleshooting

- If `docker compose up` fails due to occupied ports, stop conflicting services or map alternate ports.
- If Redpanda topic commands fail, ensure containers are healthy using `docker compose ps`.
- If Grafana dashboard provisioning fails, restart Grafana after fixing JSON syntax or datasource configuration.
