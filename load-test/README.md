# Phase 6 Load Test Pack

This folder contains repeatable load profiles for Ledger order ingress.

## Prerequisites

- Infrastructure up (`docker compose up -d`)
- Services running locally:
  - order-service: `localhost:8000`
  - inventory-service: `localhost:8001`
  - payment-service: `localhost:8002`
  - notification-service: `localhost:8003`
- k6 installed

## Scenarios

- `k6_burst_orders.js`: short burst profile for ingress pressure validation.
- `k6_staged_ramp.js`: staged ramp profile including synthetic failure pressure.

## Run Commands

```powershell
k6 run load-test/k6_burst_orders.js --summary-export load-test/reports/burst-summary.json
k6 run load-test/k6_staged_ramp.js --summary-export load-test/reports/ramp-summary.json
```

Failure-mode guidance:

- Configure `PAYMENT_FAIL_CUSTOMER_IDS=phase6-failure-demo` in payment-service env.
- Run staged ramp to force periodic compensation and DLQ-relevant paths.
