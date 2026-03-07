from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class DLQRecord:
    source_topic: str
    failed_at: str
    failure_reason: str
    retry_count: int
    original_payload: dict[str, Any]


def parse_dlq_record(raw: dict[str, Any]) -> DLQRecord:
    return DLQRecord(
        source_topic=str(raw.get("source_topic", "unknown")),
        failed_at=str(raw.get("failed_at", "")),
        failure_reason=str(raw.get("failure_reason", "unknown")),
        retry_count=int(raw.get("retry_count", 0)),
        original_payload=dict(raw.get("original_payload", {})),
    )


def replay_target_topic(record: DLQRecord) -> str:
    return record.source_topic


def build_replay_payload(record: DLQRecord) -> dict[str, Any]:
    payload = dict(record.original_payload)
    payload.setdefault("replay_meta", {})
    payload["replay_meta"].update(
        {
            "replayed_from_dlq": True,
            "original_failure_reason": record.failure_reason,
            "original_retry_count": record.retry_count,
        }
    )
    return payload
