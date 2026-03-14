from __future__ import annotations

import pytest

from shared.avro_codec import AvroCodec
from shared.schema_registry import SchemaValidationError


def test_invalid_order_created_payload_is_rejected():
    codec = AvroCodec()

    invalid = {
        "event_id": "123",
        "event_type": "OrderCreated",
        "timestamp": "2026-03-10T00:00:00Z",
        "correlation_id": "456",
        "payload": {
            "order_id": "order-1",
            # missing customer_id
            "items": [{"sku": "sku-1", "qty": 1}],
        },
    }

    with pytest.raises(SchemaValidationError):
        codec.serialize_for_topic("order.created", invalid)


def test_invalid_field_type_is_rejected():
    codec = AvroCodec()

    invalid = {
        "event_id": "123",
        "event_type": "InventoryReserved",
        "timestamp": "2026-03-10T00:00:00Z",
        "correlation_id": "456",
        "payload": {
            "order_id": "order-1",
            "customer_id": "customer-1",
            "sku": "sku-1",
            "qty": "2",
            "status": "RESERVED",
        },
    }

    with pytest.raises(SchemaValidationError):
        codec.serialize_for_topic("inventory.reserved", invalid)
