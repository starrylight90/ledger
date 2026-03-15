# Phase 3 Intentional Break Demo

## Purpose

Show that Ledger can:

- retry transient failures
- route exhausted failures to DLQ
- reject schema-invalid events before publish

## Demo A: Consumer Failure -> DLQ

1. Set `CONSUMER_MAX_ATTEMPTS=2`.
2. Inject a transient failure in a consumer handler (for demo branch/harness).
3. Publish one valid event to source topic.
4. Confirm retries are attempted.
5. Confirm message appears on `<source_topic>.dlq` with:
   - `failure_reason`
   - `retry_count`
   - original payload

## Demo B: Schema Rejection Pre-Publish

1. Construct payload missing required field (example: missing `customer_id` in `order.created`).
2. Invoke producer publish path.
3. Verify `SchemaValidationError` is raised.
4. Confirm event is not emitted to source topic.

## Demo C: Replay Planning

1. Capture a DLQ record.
2. Run:

```powershell
python scripts/dlq_inspector.py --input .\sample-dlq-record.json
```

3. Or call `/ops/dlq/inspect` with the same envelope.
4. Verify replay target and payload include replay metadata.

## Success Criteria

- Retries and DLQ behavior are deterministic.
- Schema invalid data never reaches main topics.
- Operators can inspect and prepare replay without ad-hoc scripting.
