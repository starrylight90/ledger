from __future__ import annotations

import json
from pathlib import Path


def _load_schema(name: str) -> dict:
    root = Path(__file__).resolve().parents[1]
    with open(root / "schemas" / "avro" / name, encoding="utf-8") as handle:
        return json.load(handle)


def _field_names(schema: dict) -> set[str]:
    return {field["name"] for field in schema.get("fields", [])}


def _required_field_names(schema: dict) -> set[str]:
    required: set[str] = set()
    for field in schema.get("fields", []):
        field_type = field.get("type")
        has_default = "default" in field
        if isinstance(field_type, list) and "null" in field_type:
            continue
        if has_default:
            continue
        required.add(field["name"])
    return required


def test_backward_compatible_when_adding_optional_field_to_envelope():
    current = _load_schema("order_created.avsc")

    evolved = dict(current)
    evolved_fields = list(current["fields"])
    evolved_fields.append({"name": "trace_flags", "type": ["null", "string"], "default": None})
    evolved["fields"] = evolved_fields

    assert _required_field_names(current).issubset(_field_names(evolved))


def test_forward_compatibility_retains_required_fields():
    current = _load_schema("payment_failed.avsc")

    evolved = dict(current)
    evolved_fields = [field for field in current["fields"] if field["name"] != "timestamp"]
    evolved["fields"] = evolved_fields

    assert "timestamp" in _required_field_names(current)
    assert not _required_field_names(current).issubset(_field_names(evolved))
