from __future__ import annotations

import json
import os
from typing import Any
from uuid import UUID

from sqlalchemy import select

from db import get_session
from kafka_producer import KafkaProducerClient
from models import InventoryReservation, InventoryStock, ProcessedInventoryEvent
from shared.event_schemas import build_event


class InventoryConsumer:
    def __init__(self, broker: str | None = None, topic: str = "order.created", group_id: str = "inventory-service") -> None:
        self.broker = broker or os.getenv("KAFKA_BROKER", "localhost:9092")
        self.topic = topic
        self.group_id = group_id
        self._consumer = None
        self._producer = KafkaProducerClient(broker=self.broker)

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

    def process_polled_message(self, message: Any) -> str | None:
        if message is None:
            return None

        if message.error():
            raise RuntimeError(str(message.error()))

        status = self.handle_message(message.value())
        self._ensure().commit(message=message, asynchronous=False)
        return status

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

    def _is_processed(self, event_id: str) -> bool:
        with get_session() as session:
            existing = session.execute(
                select(ProcessedInventoryEvent).where(ProcessedInventoryEvent.event_id == event_id)
            ).scalar_one_or_none()
            return existing is not None

    def _mark_processed(self, event_id: str, event_type: str) -> None:
        with get_session() as session:
            session.add(ProcessedInventoryEvent(event_id=event_id, event_type=event_type))

    def handle_message(self, raw_message: bytes | str) -> str:
        if isinstance(raw_message, bytes):
            decoded = raw_message.decode("utf-8")
        else:
            decoded = raw_message

        body = json.loads(decoded)
        event_id = str(body.get("event_id", ""))
        event_type = str(body.get("event_type", "OrderCreated"))
        if event_id and self._is_processed(event_id):
            return "DUPLICATE"

        payload = body.get("payload", {})
        items = payload.get("items", [])
        if not items:
            raise ValueError("OrderCreated payload must include at least one item")

        first = items[0]
        order_id = payload["order_id"]
        sku = first["sku"]
        qty = int(first["qty"])
        status = self.reserve_stock(order_id=order_id, sku=sku, qty=qty)

        event_type = "InventoryReserved" if status == "RESERVED" else "InventoryReservationFailed"
        topic = "inventory.reserved" if status == "RESERVED" else "inventory.reservation-failed"

        event_payload = {
            "order_id": order_id,
            "sku": sku,
            "qty": qty,
            "status": status,
        }
        envelope = build_event(
            event_type=event_type,
            correlation_id=UUID(body["correlation_id"]),
            payload=event_payload,
        )
        self._producer.publish(topic=topic, key=order_id, payload=envelope.model_dump(mode="json"))
        if event_id:
            self._mark_processed(event_id=event_id, event_type=event_type)
        return status

    def restore_reservation(self, order_id: str) -> bool:
        with get_session() as session:
            reservation = session.execute(
                select(InventoryReservation).where(InventoryReservation.order_id == order_id)
            ).scalar_one_or_none()
            if reservation is None:
                return False

            if reservation.status != "RESERVED":
                return True

            stock = session.execute(
                select(InventoryStock).where(InventoryStock.sku == reservation.sku)
            ).scalar_one_or_none()
            if stock is None:
                stock = InventoryStock(sku=reservation.sku, quantity_available=0)
                session.add(stock)
                session.flush()

            stock.quantity_available += reservation.qty
            reservation.status = "RELEASED"
            session.add(stock)
            session.add(reservation)
            return True

    def handle_payment_failed_message(self, raw_message: bytes | str) -> bool:
        if isinstance(raw_message, bytes):
            decoded = raw_message.decode("utf-8")
        else:
            decoded = raw_message

        body = json.loads(decoded)
        event_id = str(body.get("event_id", ""))
        if event_id and self._is_processed(event_id):
            return True

        payload = body.get("payload", {})
        order_id = str(payload["order_id"])
        restored = self.restore_reservation(order_id)
        if not restored:
            return False

        compensating_payload = {
            "order_id": order_id,
            "reason": "payment-failed-compensation",
            "source": "inventory-service",
        }
        envelope = build_event(
            event_type="OrderCancelled",
            correlation_id=UUID(body["correlation_id"]),
            payload=compensating_payload,
        )
        self._producer.publish(topic="order.cancelled", key=order_id, payload=envelope.model_dump(mode="json"))
        if event_id:
            self._mark_processed(event_id=event_id, event_type="PaymentFailed")
        return True
