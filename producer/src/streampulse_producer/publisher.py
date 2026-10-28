"""Publishes RawEvents to the raw Kafka topic and tracks delivery.

Kafka client settings and why (docs/ARCHITECTURE.md §5):
- enable.idempotence + acks=all: no duplicates from retries within a producer
  session, and a write counts only once all in-sync replicas have it.
- partitioner=murmur2_random: the Java client's key hashing. librdkafka's default
  (CRC32) would place the same key on a different partition than Java producers.
- compression zstd: ~6.5x on this data (measured, Research 0001).
"""

import time
from collections.abc import Callable
from typing import Any, Protocol

import structlog

from streampulse_producer import __version__
from streampulse_producer.envelope import inspect_event
from streampulse_producer.events import RawEvent
from streampulse_producer.metrics import Metrics

PRODUCER_NAME = f"streampulse-producer/{__version__}"


def kafka_config(bootstrap_servers: str, client_id: str = "streampulse-producer") -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "client.id": client_id,
        "enable.idempotence": True,
        "acks": "all",
        "partitioner": "murmur2_random",
        "compression.type": "zstd",
        "linger.ms": 50,
        # Give up on a message after 2 minutes; the producer then stops (see DeliveryFailedError).
        "delivery.timeout.ms": 120_000,
    }


class DeliveryFailedError(RuntimeError):
    """Kafka rejected or timed out at least one message; ingestion must not continue silently."""


class ProducerLike(Protocol):
    """The subset of confluent_kafka.Producer used here (lets tests use a fake)."""

    def produce(self, topic: str, **kwargs: Any) -> None: ...
    def poll(self, timeout: float = ...) -> int: ...
    def flush(self, timeout: float = ...) -> int: ...


class KafkaPublisher:
    def __init__(
        self,
        producer: ProducerLike,
        topic: str,
        metrics: Metrics,
        now_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._metrics = metrics
        self._now_ms = now_ms
        self._log = structlog.get_logger(component="publisher")
        self.failed = 0

    def publish(self, event: RawEvent) -> None:
        info = inspect_event(event.data)
        if not info.parseable:
            self._metrics.unparseable.inc()
        if info.is_canary:
            self._metrics.canary.inc()
        if info.emitted_at_ms is not None:
            self._metrics.last_event_emitted.set(info.emitted_at_ms / 1000)
            if event.mode == "live":
                self._metrics.source_delay.observe(max(0, event.received_at_ms - info.emitted_at_ms) / 1000)

        headers = [
            ("sp-ingest-ts", str(event.received_at_ms).encode()),
            ("sp-mode", event.mode.encode()),
            ("sp-producer", PRODUCER_NAME.encode()),
            ("sp-connection", event.connection.encode()),
        ]
        if event.sse_id is not None:
            headers.append(("sp-sse-id", event.sse_id.encode()))
        # Live: the record time is the ingest time. Replay: *now*, because Kafka retention
        # deletes by record timestamp and an old timestamp would expire the data at once.
        # The original ingest time always survives in sp-ingest-ts.
        timestamp = event.received_at_ms
        if event.mode == "replay":
            timestamp = self._now_ms()
            headers.append(("sp-replayed-at", str(timestamp).encode()))

        while True:
            try:
                self._producer.produce(
                    self._topic,
                    value=event.data,
                    key=info.key,
                    headers=headers,
                    timestamp=timestamp,
                    on_delivery=lambda err, msg, mode=event.mode: self._on_delivery(err, msg, mode),
                )
                break
            except BufferError:
                # Local queue full: Kafka is slower than the source. Block here, which stops
                # reading upstream (backpressure) instead of dropping events.
                self._metrics.queue_full.inc()
                self._producer.poll(0.5)
        self._producer.poll(0)  # serve delivery callbacks without blocking

    def _on_delivery(self, err: Any, msg: Any, mode: str) -> None:
        if err is not None:
            self.failed += 1
            self._metrics.produce_errors.labels(reason=err.name()).inc()
            self._log.error("delivery_failed", error=str(err), topic=self._topic, error_category="kafka")
            return
        self._metrics.events_produced.labels(mode=mode).inc()
        latency = msg.latency()
        if latency is not None:
            self._metrics.produce_latency.observe(latency)

    def flush(self, timeout_s: float = 30.0) -> None:
        """Wait for all in-flight messages; raise if any could not be delivered."""
        remaining = self._producer.flush(timeout_s)
        if remaining:
            raise DeliveryFailedError(f"{remaining} message(s) still undelivered after {timeout_s}s")
        if self.failed:
            raise DeliveryFailedError(f"{self.failed} message(s) failed delivery")
