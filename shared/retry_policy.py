from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")


class RetryExhaustedError(RuntimeError):
    def __init__(self, attempts: int, last_error: Exception) -> None:
        super().__init__(f"Retry exhausted after {attempts} attempts: {last_error}")
        self.attempts = attempts
        self.last_error = last_error


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 4
    initial_delay_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    max_delay_seconds: float = 8.0


def retry_with_backoff(
    fn: Callable[[], T],
    *,
    policy: RetryPolicy,
    should_retry: Callable[[Exception], bool],
    on_retry: Callable[[int, float, Exception], None] | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> T:
    delay = policy.initial_delay_seconds
    attempt = 1

    while True:
        try:
            return fn()
        except Exception as exc:
            if attempt >= policy.max_attempts or not should_retry(exc):
                raise RetryExhaustedError(attempts=attempt, last_error=exc) from exc

            if on_retry is not None:
                on_retry(attempt, delay, exc)

            sleep_fn(delay)
            delay = min(delay * policy.backoff_multiplier, policy.max_delay_seconds)
            attempt += 1
