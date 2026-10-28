"""Read the last record of every partition of the raw topic (input to resume.decide_resume)."""

import time
from collections.abc import Callable

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from streampulse_producer.resume import LastRecord

# Errors that say "try again" rather than "this is wrong". _DESTROY_BROKER happens when
# librdkafka retires its bootstrap connection after learning the broker's advertised
# address while a request is still in flight on it; seen on a cold broker in CI.
TRANSIENT_CODES = {
    KafkaError._DESTROY_BROKER,
    KafkaError._TRANSPORT,
    KafkaError._TIMED_OUT,
    KafkaError._ALL_BROKERS_DOWN,
    KafkaError.LEADER_NOT_AVAILABLE,
    KafkaError.NOT_LEADER_FOR_PARTITION,
}


def with_retries[T](
    call: Callable[[], T],
    attempts: int = 5,
    base_delay_s: float = 0.5,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Run `call`, retrying transient Kafka errors with exponential backoff; re-raise anything else."""
    log = structlog.get_logger(component="kafka-position")
    for attempt in range(1, attempts + 1):
        try:
            return call()
        except KafkaException as error:
            kafka_error = error.args[0] if error.args else None
            transient = isinstance(kafka_error, KafkaError) and (
                kafka_error.code() in TRANSIENT_CODES or kafka_error.retriable()
            )
            if not transient or attempt == attempts:
                raise
            delay = base_delay_s * 2 ** (attempt - 1)
            log.warning(
                "kafka_transient_error_retrying",
                error=str(kafka_error),
                attempt=attempt,
                delay_s=delay,
                error_category="kafka",
            )
            sleep(delay)
    raise AssertionError("unreachable")


def read_last_records(bootstrap_servers: str, topic: str, timeout_s: float = 10.0) -> list[LastRecord]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "streampulse-producer-resume",  # required by the client; nothing is committed
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = with_retries(lambda: consumer.list_topics(topic, timeout=timeout_s))
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            raise RuntimeError(f"topic {topic} is not available: {getattr(topic_metadata, 'error', None)}")
        records = []
        for partition in sorted(topic_metadata.partitions):
            low, high = with_retries(
                lambda p=partition: consumer.get_watermark_offsets(
                    TopicPartition(topic, p), timeout=timeout_s
                )
            )
            if high <= low:
                continue  # empty partition
            consumer.assign([TopicPartition(topic, partition, high - 1)])
            message = consumer.poll(timeout_s)
            if message is None or message.error() is not None:
                raise RuntimeError(f"could not read the last record of {topic}[{partition}]")
            headers = {name: value or b"" for name, value in (message.headers() or [])}
            records.append(LastRecord(partition=partition, headers=headers))
        return records
    finally:
        consumer.close()
