from __future__ import annotations

import os
from typing import Any

from sqlalchemy import select

from db import get_session
from models import InventoryReservation, InventoryStock


class InventoryConsumer:
    def __init__(self, broker: str | None = None, topic: str = "order.created", group_id: str = "inventory-service") -> None:
        self.broker = broker or os.getenv("KAFKA_BROKER", "localhost:9092")
        self.topic = topic
        self.group_id = group_id
        self._consumer = None

    def _ensure(self) -> Any:
        if self._consumer is not None:
            return self._consumer

        try:
            from confluent_kafka import Consumer
        except ImportError as exc:
            raise RuntimeError(
                "confluent-kafka is required for inventory consumption. Install project dependencies first."
            ) from exc

        self._consumer = Consumer(
            {
                "bootstrap.servers": self.broker,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self._consumer.subscribe([self.topic])
        return self._consumer

    def poll(self, timeout: float = 1.0) -> Any:
        consumer = self._ensure()
        return consumer.poll(timeout)

    def reserve_stock(self, order_id: str, sku: str, qty: int) -> str:
        with get_session() as session:
            existing = session.execute(
                select(InventoryReservation).where(InventoryReservation.order_id == order_id)
            ).scalar_one_or_none()
            if existing:
                return existing.status

            stock = session.execute(
                select(InventoryStock).where(InventoryStock.sku == sku)
            ).scalar_one_or_none()
            if stock is None:
                stock = InventoryStock(sku=sku, quantity_available=0)
                session.add(stock)
                session.flush()

            if stock.quantity_available >= qty:
                stock.quantity_available -= qty
                status = "RESERVED"
            else:
                status = "FAILED"

            reservation = InventoryReservation(
                order_id=order_id,
                sku=sku,
                qty=qty,
                status=status,
            )
            session.add(reservation)
            return status
