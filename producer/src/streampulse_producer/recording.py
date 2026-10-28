"""Recording format (NDJSON), shared by `record` mode, `replay` mode and scripts/research.

One JSON object per line:
    {"recv_ms": <epoch ms received>, "sse_id": "<raw SSE id or null>", "data": "<raw SSE data>"}

`data` is kept as a string exactly as received, so a recording can also carry
deliberately malformed payloads for DLQ tests.
"""

import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

from streampulse_producer.events import RawEvent


class RecordingError(ValueError):
    """A line of a recording file is not in the recording format."""


@dataclass(frozen=True, slots=True)
class RecordedEvent:
    recv_ms: int
    sse_id: str | None
    data: str


def read_recording(path: Path) -> Iterator[RecordedEvent]:
    with path.open(encoding="utf-8") as lines:
        for number, line in enumerate(lines, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
                recv_ms, sse_id, data = row["recv_ms"], row.get("sse_id"), row["data"]
            except (ValueError, KeyError, TypeError) as error:
                raise RecordingError(f"{path}:{number}: not a recording line ({error})") from error
            if not isinstance(recv_ms, int) or not isinstance(data, str):
                raise RecordingError(f"{path}:{number}: recv_ms must be an integer and data a string")
            yield RecordedEvent(recv_ms=recv_ms, sse_id=sse_id, data=data)


class RecordingWriter:
    def __init__(self, out: TextIO) -> None:
        self._out = out
        self.count = 0

    def write(self, event: RawEvent) -> None:
        row = {"recv_ms": event.received_at_ms, "sse_id": event.sse_id, "data": event.data.decode("utf-8")}
        self._out.write(json.dumps(row, ensure_ascii=False) + "\n")
        self.count += 1
