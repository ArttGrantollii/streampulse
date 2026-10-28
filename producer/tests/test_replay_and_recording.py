import io
import json
import threading

import pytest

from streampulse_producer.events import RawEvent
from streampulse_producer.recording import RecordedEvent, RecordingError, RecordingWriter, read_recording
from streampulse_producer.replay import ReplaySource


class FakeClock:
    """A monotonic clock that only moves when the replay 'waits'."""

    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


class FakeStop(threading.Event):
    def __init__(self, clock: FakeClock) -> None:
        super().__init__()
        self.clock = clock
        self.waits: list[float] = []

    def wait(self, timeout: float | None = None) -> bool:
        self.waits.append(timeout or 0)
        self.clock.now += timeout or 0
        return self.is_set()


def recorded(*recv_ms: int) -> list[RecordedEvent]:
    return [RecordedEvent(recv_ms=t, sse_id=f"id-{t}", data=f'{{"n": {t}}}') for t in recv_ms]


def emit_times(source: ReplaySource, clock: FakeClock) -> list[float]:
    times = []
    for _ in source.events():
        times.append(round(clock.now - 100.0, 6))
    return times


def test_speed_keeps_recorded_spacing_scaled():
    clock = FakeClock()
    source = ReplaySource(recorded(0, 1_000, 3_000), FakeStop(clock), speed=2.0, clock=clock)
    assert emit_times(source, clock) == [0.0, 0.5, 1.5]


def test_fixed_rate_ignores_recorded_spacing():
    clock = FakeClock()
    source = ReplaySource(recorded(0, 1, 50_000), FakeStop(clock), rate=4.0, clock=clock)
    assert emit_times(source, clock) == [0.0, 0.25, 0.5]


def test_no_pacing_never_waits():
    clock = FakeClock()
    stop = FakeStop(clock)
    assert len(list(ReplaySource(recorded(0, 5_000, 10_000), stop, clock=clock).events())) == 3
    assert stop.waits == []


def test_replay_preserves_original_receive_time_and_marks_mode():
    clock = FakeClock()
    event = next(ReplaySource(recorded(1_234), FakeStop(clock), clock=clock).events())
    assert event == RawEvent(
        data=b'{"n": 1234}', sse_id="id-1234", received_at_ms=1_234, mode="replay", connection="replay"
    )


def test_stop_ends_replay_early():
    clock = FakeClock()
    stop = FakeStop(clock)
    source = ReplaySource(recorded(0, 1_000, 2_000), stop, speed=1.0, clock=clock)
    events = source.events()
    next(events)
    stop.set()
    assert list(events) == []


@pytest.mark.parametrize(("rate", "speed"), [(1.0, 1.0), (0, None), (None, -1.0)])
def test_invalid_pacing_is_rejected(rate, speed):
    with pytest.raises(ValueError):
        ReplaySource([], threading.Event(), rate=rate, speed=speed)


def test_fixture_is_a_valid_recording(fixture_path):
    events = list(read_recording(fixture_path))
    assert len(events) == 120
    assert all(json.loads(e.data)["meta"]["stream"] == "mediawiki.recentchange" for e in events)


def test_recording_round_trip(tmp_path):
    out = io.StringIO()
    writer = RecordingWriter(out)
    writer.write(
        RawEvent(data='{"t": "Weiß"}'.encode(), sse_id="x", received_at_ms=7, mode="live", connection="c")
    )
    path = tmp_path / "rec.ndjson"
    path.write_text(out.getvalue(), encoding="utf-8")
    assert list(read_recording(path)) == [RecordedEvent(recv_ms=7, sse_id="x", data='{"t": "Weiß"}')]


def test_malformed_recording_line_names_the_line(tmp_path):
    path = tmp_path / "bad.ndjson"
    path.write_text('{"recv_ms": 1, "sse_id": null, "data": "{}"}\nnot json\n', encoding="utf-8")
    with pytest.raises(RecordingError, match=r"bad\.ndjson:2"):
        list(read_recording(path))
