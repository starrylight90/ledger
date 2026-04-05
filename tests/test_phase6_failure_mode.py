from __future__ import annotations

import json
from pathlib import Path


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_phase6_failure_mode_thresholds_are_within_documented_bounds():
    root = Path(__file__).resolve().parents[1]
    failure = _load_json(root / "load-test" / "reports" / "failure-mode-summary.json")

    assert failure["http_req_duration_ms"]["p95"] < 1200
    assert failure["http_req_duration_ms"]["p99"] < 2200
    assert failure["http_req_failed_rate"] < 0.05
    assert failure["dlq_events_total"] >= 0
