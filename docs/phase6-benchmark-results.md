# Phase 6 Benchmark Results

## Environment

- Host: Windows workstation (4 vCPU available to Docker Desktop)
- Broker: single-node Redpanda
- Storage: local Docker volumes
- Monitoring: Prometheus + Grafana
- Runtime shape: infra in Docker, services local Python processes

## Baseline Burst Profile

Source: `load-test/reports/baseline-summary.json`

| Metric | Value |
|---|---:|
| Requests | 1180 |
| Throughput (req/s) | 26.22 |
| Failure rate | 0.85% |
| p95 latency | 648.17 ms |
| p99 latency | 1112.44 ms |
| Max consumer lag | 17 |

## Failure-Mode Ramp Profile

Source: `load-test/reports/failure-mode-summary.json`

| Metric | Value |
|---|---:|
| Requests | 3492 |
| Throughput (req/s) | 21.16 |
| Failure rate | 2.16% |
| p95 latency | 978.52 ms |
| p99 latency | 1688.31 ms |
| Max consumer lag | 43 |
| DLQ events | 3 |

## Interpretation

- Baseline profile remains inside phase thresholds (`p95 < 800ms`, `p99 < 1500ms`).
- Failure profile intentionally increases latency and lag while keeping errors bounded.
- Compensation and DLQ behavior are observable and quantified under stress.

## Reproducibility

1. Start infra with `docker compose up -d`.
2. Run all four services locally.
3. Execute commands in `load-test/README.md`.
4. Compare resulting summary exports with this reference.
