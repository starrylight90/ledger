# Phase 4 gRPC Local Debug And Troubleshooting

## Components

- Proto contract: `proto/inventory.proto`
- Generated stubs: `shared/grpc_generated/`
- Inventory gRPC server: `services/inventory-service/grpc_server.py`
- Order gRPC client precheck: `services/order-service/grpc_client.py`

## Regenerate Stubs

```powershell
powershell -ExecutionPolicy Bypass -File scripts/generate_grpc.ps1
```

## Environment Defaults

- `INVENTORY_GRPC_HOST=0.0.0.0`
- `INVENTORY_GRPC_PORT=50051`
- `INVENTORY_GRPC_CLIENT_HOST=localhost`
- `INVENTORY_GRPC_CLIENT_PORT=50051`
- `INVENTORY_GRPC_TIMEOUT_SECONDS=1.5`

## Local Smoke Procedure

1. Start inventory service (includes gRPC server startup hook).
2. Seed stock for a SKU.
3. Start order service.
4. Submit order with available stock -> expect `202`.
5. Submit order exceeding stock -> expect `409` with `inventory_not_available`.
6. Stop inventory gRPC endpoint and submit order -> expect `503` with `inventory_grpc_unavailable`.

## Common Failures

### Error: inventory grpc call failed: UNAVAILABLE

Cause:
- inventory gRPC server not running or wrong host/port.

Fix:
- verify `INVENTORY_GRPC_CLIENT_HOST/PORT`.
- ensure inventory service startup completed.

### Error: schema validation fails on publish

Cause:
- event payload does not satisfy Avro contract.

Fix:
- validate required fields and types.
- regenerate stubs and confirm payload shape.

### Proto/stub mismatch

Cause:
- contract changed but stubs are stale.

Fix:
- rerun `scripts/generate_grpc.ps1`.
- commit updated generated files.
