from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


class SchemaValidationError(ValueError):
    pass


TOPIC_SCHEMA_FILE = {
    "order.created": "order_created.avsc",
    "inventory.reserved": "inventory_reserved.avsc",
    "inventory.reservation-failed": "inventory_reserved.avsc",
    "payment.completed": "payment_failed.avsc",
    "payment.failed": "payment_failed.avsc",
    "order.cancelled": "order_cancelled.avsc",
}


class SchemaRegistryClient:
    def __init__(self, schema_dir: Path | None = None) -> None:
        root = Path(__file__).resolve().parents[1]
        self.schema_dir = schema_dir or (root / "schemas" / "avro")
        self._schema_cache: dict[str, dict[str, Any]] = {}
        self._registered: set[str] = set()

    def _schema_path_for_topic(self, topic: str) -> Path | None:
        file_name = TOPIC_SCHEMA_FILE.get(topic)
        if file_name is None:
            return None
        return self.schema_dir / file_name

    def load_schema_for_topic(self, topic: str) -> dict[str, Any] | None:
        path = self._schema_path_for_topic(topic)
        if path is None:
            return None

        cache_key = str(path)
        if cache_key in self._schema_cache:
            return self._schema_cache[cache_key]

        with open(path, encoding="utf-8") as handle:
            schema = json.load(handle)
        self._schema_cache[cache_key] = schema
        return schema

    def ensure_registered(self, topic: str) -> None:
        if topic in self._registered:
            return

        schema = self.load_schema_for_topic(topic)
        if schema is None:
            self._registered.add(topic)
            return

        url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081").rstrip("/")
        subject = f"{topic}-value"
        payload = {"schema": json.dumps(schema)}

        try:
            import requests

            requests.post(
                f"{url}/subjects/{subject}/versions",
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
                data=json.dumps(payload),
                timeout=2,
            )
        except Exception:
            # Offline/local test mode: schema registration is best-effort.
            pass

        self._registered.add(topic)

    def validate(self, topic: str, payload: dict[str, Any]) -> None:
        schema = self.load_schema_for_topic(topic)
        if schema is None:
            return
        _validate_record(schema, payload, "$")


def _validate_record(schema: dict[str, Any], value: Any, path: str) -> None:
    if not isinstance(value, dict):
        raise SchemaValidationError(f"{path}: expected object")

    fields = schema.get("fields", [])
    for field in fields:
        name = field["name"]
        field_path = f"{path}.{name}"
        if name not in value:
            raise SchemaValidationError(f"{field_path}: missing required field")
        _validate_type(field["type"], value[name], field_path)


def _validate_type(schema_type: Any, value: Any, path: str) -> None:
    if isinstance(schema_type, list):
        errors = []
        for option in schema_type:
            try:
                _validate_type(option, value, path)
                return
            except SchemaValidationError as exc:
                errors.append(str(exc))
        raise SchemaValidationError(f"{path}: no union type matched ({'; '.join(errors)})")

    if isinstance(schema_type, dict):
        type_name = schema_type.get("type")
        if type_name == "record":
            _validate_record(schema_type, value, path)
            return
        if type_name == "array":
            if not isinstance(value, list):
                raise SchemaValidationError(f"{path}: expected array")
            for idx, item in enumerate(value):
                _validate_type(schema_type.get("items"), item, f"{path}[{idx}]")
            return
        _validate_type(type_name, value, path)
        return

    primitive = str(schema_type)
    if primitive == "string":
        if not isinstance(value, str):
            raise SchemaValidationError(f"{path}: expected string")
        return
    if primitive == "int":
        if not isinstance(value, int):
            raise SchemaValidationError(f"{path}: expected int")
        return
    if primitive == "null":
        if value is not None:
            raise SchemaValidationError(f"{path}: expected null")
        return
