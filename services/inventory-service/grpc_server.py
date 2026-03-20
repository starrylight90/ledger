from __future__ import annotations

import os
from concurrent import futures

import grpc
from sqlalchemy import select

from db import get_session
from models import InventoryStock
from shared.grpc_generated import inventory_pb2, inventory_pb2_grpc


class InventoryGrpcService(inventory_pb2_grpc.InventoryServiceServicer):
    def CheckAvailability(self, request, context):  # noqa: N802
        sku = request.sku
        qty = int(request.qty)

        with get_session() as session:
            stock = session.execute(select(InventoryStock).where(InventoryStock.sku == sku)).scalar_one_or_none()
            current_stock = int(stock.quantity_available) if stock else 0

        available = current_stock >= qty
        message = "available" if available else "insufficient_stock"
        return inventory_pb2.CheckAvailabilityResponse(
            available=available,
            current_stock=current_stock,
            message=message,
        )


def build_server() -> grpc.Server:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    inventory_pb2_grpc.add_InventoryServiceServicer_to_server(InventoryGrpcService(), server)
    return server


def start_grpc_server() -> grpc.Server:
    host = os.getenv("INVENTORY_GRPC_HOST", "0.0.0.0")
    port = int(os.getenv("INVENTORY_GRPC_PORT", "50051"))

    server = build_server()
    server.add_insecure_port(f"{host}:{port}")
    server.start()
    return server
