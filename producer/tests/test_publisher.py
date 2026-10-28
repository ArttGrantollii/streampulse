from typing import Any

import pytest

from streampulse_producer.events import RawEvent
from streampulse_producer.publisher import DeliveryFailedError, KafkaPublisher, kafka_config


class FakeError:
    def name(self) -> str:
        return "_MSG_TIMED_OUT"

    def __str__(self) -> str:
        return "Local: Message timed out"


class FakeMessage:
    def latency(self) -> float:
        return 0.004


class FakeProducer:
    """Records produce() calls; delivers (or fails) on poll/flush like librdkafka."""

    def __init__(self, fail: bool = False, buffer_errors: int = 0) -> None:
        self.produced: list[dict[str, Any]] = []
        self._pending: list[Any] = []
        self._fail = fail
        self._buffer_errors = buffer_errors
        self.polls: list[float] = []

    def produce(self, topic: str, **kwargs: Any) -> None:
        if self._buffer_errors:
            self._buffer_errors -= 1
            raise BufferError("Local: Queue full")
        self.produced.append({"topic": topic, **kwargs})
        self._pending.append(kwargs["on_delivery"])

    def poll(self, timeout: float = 0) -> int:
        self.polls.append(timeout)
        return self._deliver()

    def flush(self, timeout: float = 0) -> int:
        self._deliver()
        return 0

    def _deliver(self) -> int:
        delivered, self._pending = self._pending, []
        for callback in delivered:
            callback(FakeError() if self._fail else None, FakeMessage())
        return len(delivered)


def live_event(data: bytes = b'{"wiki": "enwiki", "title": "Kafka", "meta": {"dt": "2026-10-28T10:00:00Z"}}'):
    return RawEvent(
        data=data,
        sse_id='[{"timestamp": 1}]',
        received_at_ms=1_793_181_601_500,
        mode="live",
        connection="live-1-1",
    )


def headers_of(call: dict[str, Any]) -> dict[str, bytes]:
    return dict(call["headers"])


def test_payload_is_forwarded_byte_for_byte_with_key_and_headers(metrics):
    producer = FakeProducer()
    event = live_event()
    KafkaPublisher(producer, "raw", metrics).publish(event)

    call = producer.produced[0]
    assert call["topic"] == "raw"
    assert call["value"] is event.data
    assert call["key"] == b"enwiki|Kafka"
    assert call["timestamp"] == event.received_at_ms
    headers = headers_of(call)
    assert headers["sp-ingest-ts"] == b"1793181601500"
    assert headers["sp-mode"] == b"live"
    assert headers["sp-sse-id"] == b'[{"timestamp": 1}]'
    assert headers["sp-connection"] == b"live-1-1"
    assert headers["sp-producer"].startswith(b"streampulse-producer/")
    assert "sp-replayed-at" not in headers


def test_replay_uses_current_time_as_kafka_timestamp_and_keeps_original_ingest_time(metrics):
    producer = FakeProducer()
    event = RawEvent(data=b"{}", sse_id=None, received_at_ms=1_000, mode="replay", connection="replay")
    KafkaPublisher(producer, "raw", metrics, now_ms=lambda: 9_999).publish(event)

    call = producer.produced[0]
    headers = headers_of(call)
    assert call["timestamp"] == 9_999  # retention works on record timestamps
    assert headers["sp-ingest-ts"] == b"1000"
    assert headers["sp-replayed-at"] == b"9999"
    assert "sp-sse-id" not in headers


def test_unparseable_payload_is_still_published_with_null_key(metrics, registry):
    producer = FakeProducer()
    KafkaPublisher(producer, "raw", metrics).publish(live_event(b"{truncated"))
    assert producer.produced[0]["key"] is None
    assert registry.get_sample_value("streampulse_producer_unparseable_events_total") == 1


def test_acknowledgements_are_counted_and_latency_observed(metrics, registry):
    producer = FakeProducer()
    publisher = KafkaPublisher(producer, "raw", metrics)
    publisher.publish(live_event())
    publisher.flush()
    assert registry.get_sample_value("streampulse_producer_events_produced_total", {"mode": "live"}) == 1
    assert registry.get_sample_value("streampulse_producer_produce_latency_seconds_count") == 1
    assert registry.get_sample_value("streampulse_producer_source_delay_seconds_sum") == pytest.approx(1.5)


def test_full_local_queue_blocks_and_retries_instead_of_dropping(metrics, registry):
    producer = FakeProducer(buffer_errors=2)
    KafkaPublisher(producer, "raw", metrics).publish(live_event())
    assert len(producer.produced) == 1
    assert registry.get_sample_value("streampulse_producer_local_queue_full_total") == 2
    assert 0.5 in producer.polls


def test_flush_raises_when_any_delivery_failed(metrics, registry):
    producer = FakeProducer(fail=True)
    publisher = KafkaPublisher(producer, "raw", metrics)
    publisher.publish(live_event())
    with pytest.raises(DeliveryFailedError):
        publisher.flush()
    assert (
        registry.get_sample_value("streampulse_producer_produce_errors_total", {"reason": "_MSG_TIMED_OUT"})
        == 1
    )


def test_kafka_config_is_safe_and_java_compatible():
    config = kafka_config("kafka:9092")
    assert config["enable.idempotence"] is True
    assert config["acks"] == "all"
    assert config["partitioner"] == "murmur2_random"
    assert config["compression.type"] == "zstd"
