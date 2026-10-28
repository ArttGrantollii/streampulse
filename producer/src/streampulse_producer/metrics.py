"""Prometheus metrics. Each one answers an operational question (docs/ARCHITECTURE.md §17)."""

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

# Covers sub-millisecond local acks up to multi-second stalls.
LATENCY_BUCKETS_S = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)
# Source delay is seconds-scale and includes clock offset between us and Wikimedia.
DELAY_BUCKETS_S = (0.25, 0.5, 1, 2, 3, 5, 10, 20, 30, 60, 120, 300)
# Wikimedia's documented maximum is 900 s; observed drops were 2-13 minutes.
LIFETIME_BUCKETS_S = (1, 10, 30, 60, 120, 300, 600, 900, 1200, 1800)


class Metrics:
    def __init__(self, registry: CollectorRegistry) -> None:
        p = "streampulse_producer"
        self.events_received = Counter(
            f"{p}_events_received", "Events read from the source", ["mode"], registry=registry
        )
        self.events_produced = Counter(
            f"{p}_events_produced", "Events acknowledged by Kafka", ["mode"], registry=registry
        )
        self.produce_errors = Counter(
            f"{p}_produce_errors", "Kafka delivery failures", ["reason"], registry=registry
        )
        self.queue_full = Counter(
            f"{p}_local_queue_full",
            "Times the client's local queue was full (Kafka slower than the source)",
            registry=registry,
        )
        self.unparseable = Counter(
            f"{p}_unparseable_events",
            "Payloads that are not JSON objects (forwarded with a null key)",
            registry=registry,
        )
        self.canary = Counter(
            f"{p}_canary_events",
            "Wikimedia canary events seen (forwarded; filtered downstream)",
            registry=registry,
        )
        self.reconnects = Counter(
            f"{p}_reconnects",
            "Upstream reconnects by cause: server_closed, stream_interrupted, connect_error, "
            "timeout, network_error, http_<status>",
            ["cause"],
            registry=registry,
        )
        self.connection_lifetime = Histogram(
            f"{p}_connection_lifetime_seconds",
            "How long each upstream connection lasted before it ended (any cause)",
            buckets=LIFETIME_BUCKETS_S,
            registry=registry,
        )
        self.produce_latency = Histogram(
            f"{p}_produce_latency_seconds",
            "produce() call until Kafka acknowledgement",
            buckets=LATENCY_BUCKETS_S,
            registry=registry,
        )
        self.source_delay = Histogram(
            f"{p}_source_delay_seconds",
            "Local receive time minus meta.dt (includes clock offset vs Wikimedia; see clock_offset)",
            buckets=DELAY_BUCKETS_S,
            registry=registry,
        )
        self.last_event_emitted = Gauge(
            f"{p}_last_event_emitted_timestamp_seconds",
            "meta.dt of the latest event received",
            registry=registry,
        )
        self.clock_offset = Gauge(
            f"{p}_clock_offset_seconds",
            "Estimated local clock offset vs NTP (positive: local clock is behind); NaN if unknown",
            registry=registry,
        )
        self.clock_offset.set(float("nan"))
