"""Against a real Kafka broker (the compose stack): `uv run pytest -m integration`.

Each test creates its own topic and deletes it afterwards, so tests never touch
the StreamPulse topics and can run while live ingestion is going on.
"""

import json
import os
import threading
import time
import uuid
from collections.abc import Iterator

import pytest
from confluent_kafka import OFFSET_BEGINNING, Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from streampulse_producer.app import Settings, run
from streampulse_producer.events import RawEvent
from streampulse_producer.kafka_position import read_last_records
from streampulse_producer.publisher import KafkaPublisher, kafka_config
from streampulse_producer.recording import read_recording
from streampulse_producer.replay import ReplaySource
from streampulse_producer.resume import decide_resume

pytestmark = pytest.mark.integration

BOOTSTRAP = os.environ.get("STREAMPULSE_TEST_KAFKA", "localhost:9094")
PARTITIONS = 3


def java_murmur2(data: bytes) -> int:
    """Kafka's org.apache.kafka.common.utils.Utils.murmur2, as an unsigned 32-bit value."""
    m, r = 0x5BD1E995, 24
    h = (0x9747B28C ^ len(data)) & 0xFFFFFFFF
    for i in range(len(data) // 4):
        k = int.from_bytes(data[i * 4 : i * 4 + 4], "little")
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> r
        k = (k * m) & 0xFFFFFFFF
        h = ((h * m) & 0xFFFFFFFF) ^ k
    tail, rest = len(data) & ~3, len(data) % 4
    if rest == 3:
        h ^= data[tail + 2] << 16
    if rest >= 2:
        h ^= data[tail + 1] << 8
    if rest >= 1:
        h ^= data[tail]
        h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h


def java_default_partition(key: bytes, partitions: int) -> int:
    return (java_murmur2(key) & 0x7FFFFFFF) % partitions  # Utils.toPositive(murmur2(key)) % n


@pytest.mark.parametrize(
    ("data", "java_int"),
    [
        # Every case of UtilsTest.testMurmur2 in Kafka 4.2.2 (clients/src/test/.../UtilsTest.java).
        (b"21", -973932308),
        (b"foobar", -790332482),
        (b"a-little-bit-long-string", -985981536),
        (b"a-little-bit-longer-string", -1486304829),
        (b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58897971),
        (b"abc", 479470107),
    ],
)
def test_java_murmur2_matches_kafkas_reference_values(data, java_int):
    assert java_murmur2(data) == java_int & 0xFFFFFFFF


@pytest.fixture
def topic() -> Iterator[str]:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    name = f"streampulse.test.{uuid.uuid4().hex[:12]}.v1"
    admin.create_topics([NewTopic(name, num_partitions=PARTITIONS, replication_factor=1)])[name].result(15)
    yield name
    admin.delete_topics([name])[name].result(15)


def consume_all(topic: str, expected: int, timeout_s: float = 60.0) -> list:
    """Read every record of `topic` from the start.

    Uses assign() with explicit partitions, not subscribe(): no consumer-group
    coordination, so it does not depend on how quickly a freshly started broker
    can create __consumer_offsets and elect a group coordinator.
    """
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "unused", "enable.auto.commit": False})
    consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in range(PARTITIONS)])
    messages = []
    deadline = time.monotonic() + timeout_s
    try:
        while len(messages) < expected and time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is not None and message.error() is None:
                messages.append(message)
    finally:
        consumer.close()
    return messages


def test_replay_lands_every_record_byte_for_byte_on_the_java_partition(topic, metrics, fixture_path):
    recording = list(read_recording(fixture_path))
    publisher = KafkaPublisher(Producer(kafka_config(BOOTSTRAP)), topic, metrics)
    for event in ReplaySource(recording, threading.Event()).events():
        publisher.publish(event)
    publisher.flush(30)

    messages = consume_all(topic, expected=len(recording))
    assert len(messages) == len(recording)

    # Match on the payload: several fixture events share a receive millisecond and even an
    # SSE id (the id carries a timestamp, not a unique offset), but payloads are unique.
    by_payload = {e.data.encode("utf-8"): e for e in recording}
    assert len(by_payload) == len(recording)
    for message in messages:
        headers = dict(message.headers())
        original = by_payload[message.value()]
        assert int(headers["sp-ingest-ts"]) == original.recv_ms
        assert message.value() == original.data.encode("utf-8")
        assert headers["sp-mode"] == b"replay"
        assert headers["sp-sse-id"] == original.sse_id.encode()
        event = json.loads(original.data)
        assert message.key() == f"{event['wiki']}|{event['title']}".encode()
        # librdkafka's murmur2_random must place keys exactly where a Java producer would.
        assert message.partition() == java_default_partition(message.key(), PARTITIONS)


def test_resume_reads_the_real_last_records_and_picks_the_oldest(topic, metrics):
    publisher = KafkaPublisher(Producer(kafka_config(BOOTSTRAP)), topic, metrics)
    # Many keys so every partition gets records; upstream timestamps increase with i.
    for i in range(60):
        sse_id = json.dumps(
            [{"topic": "eqiad.mediawiki.recentchange", "partition": 0, "timestamp": 1_000 + i}]
        )
        data = json.dumps({"wiki": "testwiki", "title": f"Page {i}"}).encode()
        publisher.publish(RawEvent(data, sse_id, received_at_ms=1_000 + i, mode="live", connection="c"))
    publisher.flush(30)

    last = read_last_records(BOOTSTRAP, topic)
    assert len(last) == PARTITIONS
    expected_oldest = min(json.loads(r.headers["sp-sse-id"])[0]["timestamp"] for r in last)

    decision = decide_resume(last, margin_ms=100)
    assert decision.since_ms == expected_oldest - 100


def test_cli_replay_end_to_end_exits_cleanly(topic, fixture_path):
    settings = Settings(
        mode="replay",
        bootstrap_servers=BOOTSTRAP,
        topic=topic,
        user_agent="StreamPulse-test",
        file=fixture_path,
        rate=None,
        speed=None,
        metrics_port=None,
        ntp_server=None,
    )
    assert run(settings) == 0
    assert len(consume_all(topic, expected=120)) == 120
