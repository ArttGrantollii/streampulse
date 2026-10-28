"""Where to resume upstream after a producer restart (docs/adr/0012).

The raw topic itself records how far ingestion got: every record carries the
upstream SSE id in its `sp-sse-id` header. On startup we read the last record of
each partition and resume from the *oldest* of those positions minus a safety
margin. The idempotent producer keeps order per partition only, so after a crash
one partition may hold acknowledged records newer than records still in flight to
another; resuming from the newest position could skip those. The margin trades a
few duplicates (removed by Flink dedup) for never skipping an event.
"""

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LastRecord:
    partition: int
    headers: Mapping[str, bytes]


@dataclass(frozen=True, slots=True)
class ResumeDecision:
    since_ms: int | None  # None: no usable position, start from "now"
    reason: str


def upstream_timestamp_ms(sse_id: str) -> int | None:
    """Latest upstream timestamp in an SSE id.

    Wikimedia ids list one entry per datacenter topic, e.g.
    [{"topic": "eqiad...", "partition": 0, "timestamp": 1793029347980},
     {"topic": "codfw...", "partition": 0, "offset": -1}].
    Only the active datacenter carries a timestamp.
    """
    try:
        entries = json.loads(sse_id)
    except ValueError:
        return None
    if not isinstance(entries, list):
        return None
    stamps = [e["timestamp"] for e in entries if isinstance(e, dict) and isinstance(e.get("timestamp"), int)]
    return max(stamps) if stamps else None


def decide_resume(last_records: Sequence[LastRecord], margin_ms: int) -> ResumeDecision:
    if not last_records:
        return ResumeDecision(None, "raw topic is empty")

    positions = []
    for record in last_records:
        mode = record.headers.get("sp-mode", b"").decode()
        if mode != "live":
            # Replayed data says nothing about how far *live* ingestion got.
            return ResumeDecision(None, f"partition {record.partition} ends with a '{mode or '?'}' record")
        raw_id = record.headers.get("sp-sse-id")
        stamp = upstream_timestamp_ms(raw_id.decode()) if raw_id else None
        if stamp is None:
            return ResumeDecision(None, f"partition {record.partition}: last record has no usable sp-sse-id")
        positions.append(stamp)

    oldest = min(positions)
    return ResumeDecision(
        oldest - margin_ms,
        f"oldest last-record position across {len(positions)} partition(s) minus {margin_ms} ms",
    )
