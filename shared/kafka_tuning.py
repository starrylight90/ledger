from __future__ import annotations

import os
from typing import Any


def producer_config(bootstrap_servers: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "acks": os.getenv("KAFKA_PRODUCER_ACKS", "all"),
        "compression.type": os.getenv("KAFKA_PRODUCER_COMPRESSION", "lz4"),
        "linger.ms": int(os.getenv("KAFKA_PRODUCER_LINGER_MS", "15")),
        "batch.num.messages": int(os.getenv("KAFKA_PRODUCER_BATCH_NUM_MESSAGES", "1000")),
        "request.timeout.ms": int(os.getenv("KAFKA_PRODUCER_REQUEST_TIMEOUT_MS", "30000")),
        "message.timeout.ms": int(os.getenv("KAFKA_PRODUCER_MESSAGE_TIMEOUT_MS", "60000")),
    }


def consumer_config(bootstrap_servers: str, group_id: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": os.getenv("CONSUMER_AUTO_OFFSET_RESET", "earliest"),
        "enable.auto.commit": False,
        "max.poll.interval.ms": int(os.getenv("CONSUMER_MAX_POLL_INTERVAL_MS", "300000")),
        "fetch.min.bytes": int(os.getenv("CONSUMER_FETCH_MIN_BYTES", "1")),
        "fetch.wait.max.ms": int(os.getenv("CONSUMER_FETCH_WAIT_MAX_MS", "500")),
        "queued.max.messages.kbytes": int(os.getenv("CONSUMER_QUEUE_MAX_KB", "65536")),
        "session.timeout.ms": int(os.getenv("CONSUMER_SESSION_TIMEOUT_MS", "10000")),
    }
