from __future__ import annotations

import json
from pathlib import Path


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_phase6_report_files_exist_and_have_core_fields():
    root = Path(__file__).resolve().parents[1]
    baseline_path = root / "load-test" / "reports" / "baseline-summary.json"
    failure_path = root / "load-test" / "reports" / "failure-mode-summary.json"

    assert baseline_path.exists()
    assert failure_path.exists()

    baseline = _load_json(baseline_path)
    failure = _load_json(failure_path)

    for report in (baseline, failure):
        assert "scenario" in report
        assert "throughput_rps" in report
        assert "http_req_duration_ms" in report
        assert "p95" in report["http_req_duration_ms"]
        assert "p99" in report["http_req_duration_ms"]


def test_phase6_report_thresholds_are_within_documented_bounds():
    root = Path(__file__).resolve().parents[1]
    baseline = _load_json(root / "load-test" / "reports" / "baseline-summary.json")

    assert baseline["http_req_duration_ms"]["p95"] < 800
    assert baseline["http_req_duration_ms"]["p99"] < 1500
