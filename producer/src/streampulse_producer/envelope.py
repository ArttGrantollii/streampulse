"""What the producer needs to know about an event, without changing it.

The raw payload is forwarded to Kafka byte for byte (docs/adr/0005). The producer
only peeks at two things: the partition key and the upstream emission time used
for the source-delay metric. A payload that cannot be parsed is still forwarded,
with a null key, so the Flink job can route it to the DLQ with full context.
"""

import json
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class EventInfo:
    key: bytes | None
    emitted_at_ms: int | None  # meta.dt, when Wikimedia emitted the event
    parseable: bool
    is_canary: bool


UNPARSEABLE = EventInfo(key=None, emitted_at_ms=None, parseable=False, is_canary=False)


def inspect_event(data: bytes) -> EventInfo:
    try:
        event = json.loads(data)
    except ValueError:  # includes UnicodeDecodeError and JSONDecodeError
        return UNPARSEABLE
    if not isinstance(event, dict):
        return UNPARSEABLE

    meta = event.get("meta") if isinstance(event.get("meta"), dict) else {}
    wiki, title = event.get("wiki"), event.get("title")
    # Key by page: per-page ordering within a partition and an even spread (docs/adr/0004).
    key = f"{wiki}|{title}".encode() if isinstance(wiki, str) and isinstance(title, str) else None
    return EventInfo(
        key=key,
        emitted_at_ms=_iso_to_ms(meta.get("dt")),
        parseable=True,
        is_canary=meta.get("domain") == "canary",
    )


def _iso_to_ms(value: object) -> int | None:
    if not isinstance(value, str):
        return None
    try:
        return int(datetime.fromisoformat(value).timestamp() * 1000)
    except ValueError:
        return None
