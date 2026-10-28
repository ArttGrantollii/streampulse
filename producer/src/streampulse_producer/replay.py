"""Replay source: re-emits a recording through the same publishing path as live mode.

Pacing options:
- speed=X: keep the recording's own timing, X times faster (1.0 = real time);
- rate=N: a fixed N events per second, ignoring the recorded timing;
- rate=None and speed=None: as fast as the publisher accepts (benchmarks).
Original receive times are preserved (`received_at_ms`), so validation that is
relative to ingest time gives the same answer on every replay.
"""

import threading
import time
from collections.abc import Callable, Iterable, Iterator

from streampulse_producer.events import RawEvent
from streampulse_producer.recording import RecordedEvent


class ReplaySource:
    def __init__(
        self,
        recording: Iterable[RecordedEvent],
        stop: threading.Event,
        *,
        rate: float | None = None,
        speed: float | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if rate is not None and speed is not None:
            raise ValueError("use either rate or speed, not both")
        if (rate is not None and rate <= 0) or (speed is not None and speed <= 0):
            raise ValueError("rate and speed must be positive")
        self._recording = recording
        self._stop = stop
        self._rate = rate
        self._speed = speed
        self._clock = clock

    def events(self) -> Iterator[RawEvent]:
        start = self._clock()
        first_recv_ms: int | None = None
        for index, recorded in enumerate(self._recording):
            if first_recv_ms is None:
                first_recv_ms = recorded.recv_ms
            due = self._due_offset_s(index, recorded.recv_ms - first_recv_ms)
            if due is not None:
                wait_s = start + due - self._clock()
                if wait_s > 0 and self._stop.wait(wait_s):
                    return
            if self._stop.is_set():
                return
            yield RawEvent(
                data=recorded.data.encode("utf-8"),
                sse_id=recorded.sse_id,
                received_at_ms=recorded.recv_ms,
                mode="replay",
                connection="replay",
            )

    def _due_offset_s(self, index: int, elapsed_recorded_ms: int) -> float | None:
        """Seconds after the start at which event `index` should be emitted, or None for no pacing."""
        if self._rate is not None:
            return index / self._rate
        if self._speed is not None:
            return elapsed_recorded_ms / 1000 / self._speed
        return None
