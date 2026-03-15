# Phase 3 Schema Versioning And Migration Guidance

## Contract Strategy

Ledger uses topic-bound schema contracts stored in `schemas/avro/` and registered with Schema Registry.

- subject format: `<topic>-value`
- schema source of truth: repository files
- registry registration: producer best-effort on publish path

## Versioning Rules

1. Backward compatibility first.
2. Never remove required fields in-place.
3. Add new fields as optional with defaults.
4. Use new event names only for semantic breaking changes.

## Safe Changes

- add nullable field with default
- add metadata section under payload with nullable/default values
- expand enum-like string values only if consumers tolerate unknown values

## Breaking Changes

- removing required field
- changing field type (`int` -> `string`)
- moving fields between envelope and payload

## Migration Pattern

1. Add optional field in schema and deploy producers first.
2. Deploy consumers that understand both old and new shapes.
3. Observe metrics and error rates.
4. Promote field to required only after complete consumer rollout.

## Testing Requirements

- backward compatibility test
- forward break detection test
- invalid payload rejection test before publish

## Rollback Guidance

- rollback producers first if new shape causes consumer errors
- keep compatibility layer in consumers for at least one release cycle
- use DLQ inspector to replay messages once compatible binaries are restored
