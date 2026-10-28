import json
import random

import pytest

from streampulse_producer.backoff import Backoff
from streampulse_producer.envelope import inspect_event
from streampulse_producer.resume import LastRecord, decide_resume, upstream_timestamp_ms

# --- envelope ---------------------------------------------------------------------------


def test_key_is_wiki_and_title():
    info = inspect_event(
        json.dumps({"wiki": "enwiki", "title": "Kafka", "meta": {"dt": "2026-10-28T10:00:00Z"}}).encode()
    )
    assert info.key == b"enwiki|Kafka"
    assert info.emitted_at_ms == 1_793_181_600_000
    assert info.parseable and not info.is_canary


def test_unicode_titles_are_encoded_as_utf8():
    info = inspect_event(json.dumps({"wiki": "dewiki", "title": "Schwarz-Weiß"}).encode())
    assert info.key == "dewiki|Schwarz-Weiß".encode()


@pytest.mark.parametrize("payload", [b"{not json", b"\xff\xfe", b"[1, 2]", b'"a string"'])
def test_unparseable_payloads_get_a_null_key_but_are_not_rejected(payload):
    info = inspect_event(payload)
    assert info.key is None and not info.parseable


def test_missing_title_gives_null_key_but_still_parseable():
    info = inspect_event(b'{"wiki": "enwiki", "meta": {"dt": "garbage"}}')
    assert info.key is None and info.parseable and info.emitted_at_ms is None


def test_canary_is_detected():
    assert inspect_event(b'{"meta": {"domain": "canary"}}').is_canary


# --- backoff ----------------------------------------------------------------------------


def test_backoff_ceiling_doubles_up_to_the_cap():
    backoff = Backoff(base_s=1, cap_s=8, rng=random.Random(0))
    ceilings = [min(8, 2**attempt) for attempt in range(6)]
    for ceiling in ceilings:
        assert 0 <= backoff.next_delay() <= ceiling


def test_backoff_reset_starts_again_from_base():
    backoff = Backoff(base_s=1, cap_s=60, rng=random.Random(0))
    for _ in range(5):
        backoff.next_delay()
    backoff.reset()
    assert backoff.next_delay() <= 1


# --- resume -----------------------------------------------------------------------------


def sse_id(timestamp: int) -> bytes:
    return json.dumps(
        [
            {"topic": "eqiad.mediawiki.recentchange", "partition": 0, "timestamp": timestamp},
            {"topic": "codfw.mediawiki.recentchange", "partition": 0, "offset": -1},
        ]
    ).encode()


def live(partition: int, timestamp: int) -> LastRecord:
    return LastRecord(partition, {"sp-mode": b"live", "sp-sse-id": sse_id(timestamp)})


def test_upstream_timestamp_uses_the_entry_that_has_one():
    assert upstream_timestamp_ms(sse_id(1_000).decode()) == 1_000
    assert upstream_timestamp_ms("not json") is None
    assert upstream_timestamp_ms('[{"topic": "x", "offset": -1}]') is None


def test_resume_from_oldest_partition_minus_margin():
    decision = decide_resume([live(0, 5_000), live(1, 3_000), live(2, 4_000)], margin_ms=500)
    assert decision.since_ms == 2_500


def test_empty_topic_starts_from_now():
    assert decide_resume([], margin_ms=500).since_ms is None


def test_replayed_data_is_not_a_live_position():
    replayed = LastRecord(1, {"sp-mode": b"replay", "sp-sse-id": sse_id(9_000)})
    decision = decide_resume([live(0, 5_000), replayed], margin_ms=500)
    assert decision.since_ms is None and "replay" in decision.reason


def test_record_without_sse_id_is_not_usable():
    decision = decide_resume([LastRecord(0, {"sp-mode": b"live"})], margin_ms=500)
    assert decision.since_ms is None
