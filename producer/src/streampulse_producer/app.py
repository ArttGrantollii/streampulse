"""Wires a source (live or replay) to a sink (Kafka or a recording file) and runs it."""

import signal
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import httpx
import structlog
from confluent_kafka import KafkaError, Producer
from prometheus_client import CollectorRegistry, start_http_server

from streampulse_producer.clock import sntp_offset_seconds
from streampulse_producer.events import RawEvent
from streampulse_producer.kafka_position import read_last_records
from streampulse_producer.live import DEFAULT_URL, LiveSource, ResumeMismatchError, since_param
from streampulse_producer.metrics import Metrics
from streampulse_producer.publisher import DeliveryFailedError, KafkaPublisher, kafka_config
from streampulse_producer.recording import RecordingWriter, read_recording
from streampulse_producer.replay import ReplaySource
from streampulse_producer.resume import decide_resume


@dataclass(frozen=True, slots=True)
class Settings:
    mode: str  # live | replay | record
    bootstrap_servers: str
    topic: str
    user_agent: str
    url: str = DEFAULT_URL
    file: Path | None = None
    rate: float | None = None
    speed: float | None = None
    since_ms: int | None = None
    resume_margin_ms: int = 30_000
    # Refuse to start further back than this: a long catch-up must be a deliberate decision.
    max_resume_age_s: float = 24 * 3600
    max_events: int | None = None
    duration_s: float | None = None
    metrics_port: int | None = 8000
    ntp_server: str | None = "pool.ntp.org"


def run(settings: Settings) -> int:
    log = structlog.get_logger(component="producer", mode=settings.mode)
    registry = CollectorRegistry()
    metrics = Metrics(registry)
    if settings.metrics_port:
        start_http_server(settings.metrics_port, registry=registry)

    stop = threading.Event()

    def request_stop(signum: int, _frame: object) -> None:
        log.info("shutdown_requested", signal=signal.Signals(signum).name)
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    if settings.ntp_server and settings.mode != "replay":
        offset = sntp_offset_seconds(settings.ntp_server)
        if offset is not None:
            metrics.clock_offset.set(offset)
        log.info(
            "clock_offset",
            ntp_server=settings.ntp_server,
            offset_s=None if offset is None else round(offset, 3),
        )

    if settings.mode == "record":
        return _record(settings, metrics, stop, log)

    fatal: list[str] = []

    def on_client_error(error: KafkaError) -> None:
        # Fatal errors (e.g. idempotence sequence violations) end the producer instance.
        log.error("kafka_client_error", error=str(error), fatal=error.fatal(), error_category="kafka")
        if error.fatal():
            fatal.append(str(error))
            stop.set()

    producer = Producer({**kafka_config(settings.bootstrap_servers), "error_cb": on_client_error})
    publisher = KafkaPublisher(producer, settings.topic, metrics)

    if settings.mode == "live":
        since_ms = settings.since_ms if settings.since_ms is not None else _resume_since(settings, log)
        if since_ms is not None and time.time() * 1000 - since_ms > settings.max_resume_age_s * 1000:
            log.error(
                "resume_too_old",
                since=since_param(since_ms),
                max_resume_age_s=settings.max_resume_age_s,
                hint="raise --max-resume-age to catch up deliberately, or pass a newer --since",
                error_category="config",
            )
            return 2
        client = httpx.Client(timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0))
        source: Iterator[RawEvent] = LiveSource(
            client,
            metrics,
            stop,
            url=settings.url,
            user_agent=settings.user_agent,
            since_ms=since_ms,
            before_reconnect=publisher.flush,
        ).events()
    else:
        assert settings.file is not None
        source = ReplaySource(
            read_recording(settings.file), stop, rate=settings.rate, speed=settings.speed
        ).events()

    count = 0
    deadline = time.monotonic() + settings.duration_s if settings.duration_s else None
    exit_code = 0
    try:
        for event in source:
            metrics.events_received.labels(mode=event.mode).inc()
            publisher.publish(event)
            count += 1
            if (settings.max_events and count >= settings.max_events) or (
                deadline and time.monotonic() >= deadline
            ):
                break
    except DeliveryFailedError as error:
        log.error("delivery_failed_stopping", error=str(error), error_category="kafka")
        exit_code = 1
    except ResumeMismatchError as error:
        log.error("resume_mismatch_stopping", error=str(error), error_category="upstream")
        exit_code = 1
    finally:
        try:
            publisher.flush(30.0)
        except DeliveryFailedError as error:
            log.error("final_flush_failed", error=str(error), error_category="kafka")
            exit_code = 1
    if fatal:
        exit_code = 1
    log.info("producer_stopped", events=count, exit_code=exit_code)
    return exit_code


def _resume_since(settings: Settings, log: structlog.typing.FilteringBoundLogger) -> int | None:
    decision = decide_resume(
        read_last_records(settings.bootstrap_servers, settings.topic), settings.resume_margin_ms
    )
    log.info("resume_decision", since_ms=decision.since_ms, reason=decision.reason)
    return decision.since_ms


def _record(
    settings: Settings, metrics: Metrics, stop: threading.Event, log: structlog.typing.FilteringBoundLogger
) -> int:
    assert settings.file is not None
    deadline = time.monotonic() + settings.duration_s if settings.duration_s else None
    with (
        httpx.Client(timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)) as client,
        settings.file.open("w", encoding="utf-8") as out,
    ):
        writer = RecordingWriter(out)
        source = LiveSource(
            client,
            metrics,
            stop,
            url=settings.url,
            user_agent=settings.user_agent,
            since_ms=settings.since_ms,
        )
        for event in source.events():
            metrics.events_received.labels(mode="live").inc()
            writer.write(event)
            if (settings.max_events and writer.count >= settings.max_events) or (
                deadline and time.monotonic() >= deadline
            ):
                break
    log.info("recording_written", file=str(settings.file), events=writer.count)
    return 0
