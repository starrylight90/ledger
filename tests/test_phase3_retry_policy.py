from __future__ import annotations

from shared.error_classification import FailureKind, classify_error
from shared.retry_policy import RetryExhaustedError, RetryPolicy, retry_with_backoff


def test_retry_succeeds_after_transient_failures():
    attempts = {"count": 0}
    sleeps: list[float] = []

    def flaky() -> str:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise TimeoutError("transient timeout")
        return "ok"

    result = retry_with_backoff(
        flaky,
        policy=RetryPolicy(max_attempts=4, initial_delay_seconds=1, backoff_multiplier=2, max_delay_seconds=4),
        should_retry=lambda exc: classify_error(exc) == FailureKind.TRANSIENT,
        sleep_fn=lambda seconds: sleeps.append(seconds),
    )

    assert result == "ok"
    assert attempts["count"] == 3
    assert sleeps == [1, 2]


def test_retry_stops_on_poison_error():
    attempts = {"count": 0}

    def poison() -> str:
        attempts["count"] += 1
        raise ValueError("malformed payload")

    try:
        retry_with_backoff(
            poison,
            policy=RetryPolicy(max_attempts=5),
            should_retry=lambda exc: classify_error(exc) == FailureKind.TRANSIENT,
            sleep_fn=lambda _seconds: None,
        )
    except RetryExhaustedError as err:
        assert attempts["count"] == 1
        assert isinstance(err.last_error, ValueError)
    else:
        raise AssertionError("Expected RetryExhaustedError for poison message")
