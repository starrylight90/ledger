# Phase 3 Reliability Runbook

## Goals

- classify failures as transient vs poison
- retry transient failures with exponential backoff
- route exhausted failures to per-topic DLQ
- preserve consumer progress by committing offsets only after success or DLQ handoff

## Consumer Runtime Controls

- `CONSUMER_MAX_ATTEMPTS` (default `4`)
- `KAFKA_BROKER`
- `SCHEMA_REGISTRY_URL`

## Retry Policy Defaults

- attempts: 4
- delays: 1s, 2s, 4s
- cap: 8s

## Failure Classification

- Poison:
  - malformed JSON
  - missing required fields
  - type mismatch
- Transient:
  - network timeout
  - connection unavailable
  - temporary runtime backpressure

## DLQ Envelope Fields

Each DLQ message includes:

- `source_topic`
- `failed_at`
- `failure_reason`
- `retry_count`
- `original_payload`

## Operational Commands

Inspect and build replay payload from a captured DLQ record:

```powershell
python scripts/dlq_inspector.py --input path/to/dlq-record.json
```

Use ops endpoint for replay planning:

```http
POST /ops/dlq/inspect
```

Body:

```json
{
  "source_topic": "order.created",
  "failed_at": "2026-03-11T22:30:00Z",
  "failure_reason": "temporary downstream failure",
  "retry_count": 4,
  "original_payload": {}
}
```

## Incident Checklist

1. Confirm whether error is poison or transient.
2. Verify retries were attempted.
3. Confirm DLQ handoff happened.
4. Inspect original payload and repair upstream issue.
5. Replay only after root cause is fixed.
