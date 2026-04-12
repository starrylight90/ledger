# Phase 6 Release Checklist (v1.0.0)

## Build and Runtime

- [ ] `docker compose up -d` reports healthy infrastructure.
- [ ] order/inventory/payment/notification services start cleanly.
- [ ] `/health` and `/metrics` endpoints respond for all services.

## Reliability and Contracts

- [ ] Full regression tests pass (phases 1-5).
- [ ] gRPC precheck behavior remains correct (`409` insufficient stock, `503` transport issues).
- [ ] Schema registry is reachable and event serialization path is validated.

## Performance and Observability

- [ ] Burst and staged ramp profiles are present and executable.
- [ ] Benchmark summaries are published in `load-test/reports`.
- [ ] Grafana dashboard `ledger-phase5` renders throughput, lag, DLQ, and p95 views.

## Documentation and Public Readiness

- [ ] README includes phase-by-phase what/why/how narrative.
- [ ] README links screenshots for each phase.
- [ ] Known limitations and roadmap are documented.
- [ ] Demo trace-by-correlation-id walkthrough is reproducible.

## Release Tagging

- [ ] Create annotated tag `v1.0.0` after final sign-off.
- [ ] Push tag and verify GitHub release notes include benchmark links.
