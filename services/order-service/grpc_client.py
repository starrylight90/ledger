from __future__ import annotations

import os

import grpc

from shared.grpc_generated import inventory_pb2, inventory_pb2_grpc


class InventoryGrpcUnavailableError(RuntimeError):
    pass


class InventoryGrpcClient:
    def __init__(self) -> None:
        host = os.getenv("INVENTORY_GRPC_CLIENT_HOST", "localhost")
        port = int(os.getenv("INVENTORY_GRPC_CLIENT_PORT", "50051"))
        self._target = f"{host}:{port}"
        self._timeout_seconds = float(os.getenv("INVENTORY_GRPC_TIMEOUT_SECONDS", "1.5"))

    def check_availability(self, sku: str, qty: int) -> tuple[bool, int, str]:
        request = inventory_pb2.CheckAvailabilityRequest(sku=sku, qty=qty)

        try:
            with grpc.insecure_channel(self._target) as channel:
                stub = inventory_pb2_grpc.InventoryServiceStub(channel)
                response = stub.CheckAvailability(request, timeout=self._timeout_seconds)
                return bool(response.available), int(response.current_stock), str(response.message)
        except grpc.RpcError as exc:
            raise InventoryGrpcUnavailableError(f"inventory grpc call failed: {exc.code().name}") from exc
