"""The unit that flows from every event source to every sink."""

from dataclasses import dataclass
from typing import Literal

Mode = Literal["live", "replay"]


@dataclass(frozen=True, slots=True)
class RawEvent:
    data: bytes  # the upstream SSE `data` payload, untouched
    sse_id: str | None  # upstream resume position (JSON array of topic/partition/timestamp)
    received_at_ms: int  # when the producer received it; preserved by replay
    mode: Mode
    connection: str  # which upstream connection delivered it (attributes duplicates to reconnects)
