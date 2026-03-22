from __future__ import annotations

import os
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable, Iterable


_DEFAULT_BUCKETS = (5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0)


@dataclass(frozen=True)
class _LabelSet:
    items: tuple[tuple[str, str], ...]

    @staticmethod
    def from_dict(labels: dict[str, str] | None = None) -> "_LabelSet":
        if not labels:
            return _LabelSet(items=())
        normalized = tuple(sorted((str(k), str(v)) for k, v in labels.items()))
        return _LabelSet(items=normalized)

    def render(self) -> str:
        if not self.items:
            return ""
        encoded = ",".join(f'{key}="{_escape(value)}"' for key, value in self.items)
        return "{" + encoded + "}"


class MetricsRegistry:
    def __init__(self, service: str) -> None:
        self.service = service
        self._lock = threading.Lock()
        self._counters: dict[str, dict[_LabelSet, float]] = defaultdict(lambda: defaultdict(float))
        self._gauges: dict[str, dict[_LabelSet, float]] = defaultdict(lambda: defaultdict(float))
        self._histograms: dict[str, dict[_LabelSet, list[float]]] = defaultdict(dict)
        self._descriptions: dict[str, tuple[str, str]] = {}

    def counter_inc(self, name: str, amount: float = 1.0, *, labels: dict[str, str] | None = None, description: str = "") -> None:
        label_set = _LabelSet.from_dict(_with_service(labels, self.service))
        with self._lock:
            self._descriptions.setdefault(name, ("counter", description or name))
            self._counters[name][label_set] += amount

    def gauge_set(self, name: str, value: float, *, labels: dict[str, str] | None = None, description: str = "") -> None:
        label_set = _LabelSet.from_dict(_with_service(labels, self.service))
        with self._lock:
            self._descriptions.setdefault(name, ("gauge", description or name))
            self._gauges[name][label_set] = value

    def histogram_observe(
        self,
        name: str,
        value: float,
        *,
        labels: dict[str, str] | None = None,
        description: str = "",
        buckets: Iterable[float] | None = None,
    ) -> None:
        label_set = _LabelSet.from_dict(_with_service(labels, self.service))
        selected_buckets = tuple(float(b) for b in (buckets or _DEFAULT_BUCKETS))
        with self._lock:
            self._descriptions.setdefault(name, ("histogram", description or name))
            hist = self._histograms[name].get(label_set)
            if hist is None:
                hist = [0.0] * (len(selected_buckets) + 2)
                self._histograms[name][label_set] = hist

            for idx, boundary in enumerate(selected_buckets):
                if value <= boundary:
                    hist[idx] += 1
            hist[len(selected_buckets)] += 1
            hist[len(selected_buckets) + 1] += value

    def timer(self, name: str, *, labels: dict[str, str] | None = None, description: str = ""):
        return _Timer(lambda elapsed_ms: self.histogram_observe(name, elapsed_ms, labels=labels, description=description))

    def render_prometheus(self) -> str:
        lines: list[str] = []
        with self._lock:
            names = sorted(self._descriptions)
            for name in names:
                metric_type, help_text = self._descriptions[name]
                lines.append(f"# HELP {name} {_escape(help_text)}")
                lines.append(f"# TYPE {name} {metric_type}")

                if metric_type == "counter":
                    for label_set, value in sorted(self._counters.get(name, {}).items(), key=lambda x: x[0].items):
                        lines.append(f"{name}{label_set.render()} {value}")
                elif metric_type == "gauge":
                    for label_set, value in sorted(self._gauges.get(name, {}).items(), key=lambda x: x[0].items):
                        lines.append(f"{name}{label_set.render()} {value}")
                elif metric_type == "histogram":
                    for label_set, values in sorted(self._histograms.get(name, {}).items(), key=lambda x: x[0].items):
                        buckets = _DEFAULT_BUCKETS
                        cumulative = 0.0
                        for idx, upper_bound in enumerate(buckets):
                            cumulative += values[idx]
                            lines.append(f"{name}_bucket{_merge_label(label_set, 'le', str(upper_bound))} {cumulative}")
                        lines.append(f"{name}_bucket{_merge_label(label_set, 'le', '+Inf')} {values[len(buckets)]}")
                        lines.append(f"{name}_count{label_set.render()} {values[len(buckets)]}")
                        lines.append(f"{name}_sum{label_set.render()} {values[len(buckets) + 1]}")

        return "\n".join(lines) + "\n"


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _with_service(labels: dict[str, str] | None, service: str) -> dict[str, str]:
    merged = dict(labels or {})
    merged.setdefault("service", service)
    return merged


def _merge_label(label_set: _LabelSet, key: str, value: str) -> str:
    merged = dict(label_set.items)
    merged[key] = value
    return _LabelSet.from_dict(merged).render()


class _Timer:
    def __init__(self, callback: Callable[[float], None]) -> None:
        self._callback = callback
        self._start = time.perf_counter()

    def stop(self) -> None:
        elapsed_ms = (time.perf_counter() - self._start) * 1000.0
        self._callback(elapsed_ms)


_REGISTRIES: dict[str, MetricsRegistry] = {}
_REGISTRIES_LOCK = threading.Lock()


def get_registry(service: str | None = None) -> MetricsRegistry:
    service_name = service or os.getenv("LEDGER_SERVICE_NAME", "unknown-service")
    with _REGISTRIES_LOCK:
        existing = _REGISTRIES.get(service_name)
        if existing is not None:
            return existing

        registry = MetricsRegistry(service=service_name)
        _REGISTRIES[service_name] = registry
        return registry


def reset_registry(service: str | None = None) -> None:
    with _REGISTRIES_LOCK:
        if service is None:
            _REGISTRIES.clear()
            return
        _REGISTRIES.pop(service, None)
