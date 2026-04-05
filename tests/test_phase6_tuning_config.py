from __future__ import annotations

from shared.kafka_tuning import consumer_config, producer_config


def test_producer_config_includes_perf_knobs(monkeypatch):
    monkeypatch.setenv("KAFKA_PRODUCER_COMPRESSION", "lz4")
    monkeypatch.setenv("KAFKA_PRODUCER_LINGER_MS", "15")
    monkeypatch.setenv("KAFKA_PRODUCER_BATCH_NUM_MESSAGES", "1000")

    cfg = producer_config("localhost:9092")

    assert cfg["bootstrap.servers"] == "localhost:9092"
    assert cfg["compression.type"] == "lz4"
    assert cfg["linger.ms"] == 15
    assert cfg["batch.num.messages"] == 1000


def test_consumer_config_includes_poll_and_fetch_knobs(monkeypatch):
    monkeypatch.setenv("CONSUMER_MAX_POLL_INTERVAL_MS", "300000")
    monkeypatch.setenv("CONSUMER_FETCH_WAIT_MAX_MS", "500")

    cfg = consumer_config("localhost:9092", "inventory-service")

    assert cfg["group.id"] == "inventory-service"
    assert cfg["max.poll.interval.ms"] == 300000
    assert cfg["fetch.wait.max.ms"] == 500
    assert cfg["enable.auto.commit"] is False
