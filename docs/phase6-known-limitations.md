# Phase 6 Known Limitations and Next Steps

## Current Limitations

1. Single-node broker and database in local compose are not production HA.
2. Service processes are started manually for local load runs.
3. k6 scenarios target order ingress only; no direct gRPC load profile yet.
4. Consumer scaling is horizontal-by-replica and not auto-managed in this repo.
5. Alerting policies are documented but not enforced by on-call automation.

## Why These Trade-offs Were Chosen

- Focused on interview-ready architecture and reproducible local operation.
- Kept operational complexity manageable while preserving core distributed-systems behaviors.
- Prioritized transparency around reliability mechanics over over-engineered deployment plumbing.

## Recommended Next Steps

1. Add service containers and profile-specific compose overlays for isolated perf runs.
2. Add direct gRPC load profile and saturation testing for inventory precheck path.
3. Add CI workflow that runs smoke + contract + load-threshold checks nightly.
4. Configure alertmanager rules for lag, DLQ spikes, and sustained p95 regressions.
5. Run multi-node broker tests to validate partition behavior and failover assumptions.
