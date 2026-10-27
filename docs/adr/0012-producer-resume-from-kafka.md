# ADR-0012 — The producer resumes from the position stored in Kafka

- **Status:** Proposed. Validated in Phase 2 (R-1).
- **Date:** 2026-10-26

## Context
- The producer must resume after reconnects (normal, at least every 15 minutes) and after its own restarts.
- Upstream supports resuming via `Last-Event-ID` or `since`.
- Where should "how far we got" be stored, and what does the stored position actually mean?

## Decision
- **In-process (reconnects):** keep the last *acknowledged-by-Kafka* SSE id in memory, and reconnect with `Last-Event-ID` set to it.
- **On startup (restarts):** read the **last record of each raw partition** and parse its `sp-sse-id` header. Resume from the **minimum** per-partition upstream timestamp, minus a safety margin `M` (initially 30 s), using `since=` or a constructed `Last-Event-ID`. Duplicates from the margin are removed by Flink dedup ([ADR-0007](0007-deduplication.md)).
- **If raw is empty:** start at "now" by default, or at `--since <time>` if given.

**Why the minimum across partitions.** The idempotent producer keeps ordering *per partition*. After a crash, partition A may hold acknowledged records that are newer than records still in flight to partition B. Resuming from the global maximum could skip B's unacknowledged records. The minimum, minus a margin, can't skip them.

## Alternatives
- **Local state file.** It can diverge from what Kafka actually holds (the file says X, but the crash happened before the ack), and it is lost with the container unless a volume is mounted.
- **Commit the position to a separate compacted Kafka topic.** Correct and common, but it is a second write path. Our raw records already carry the position for free.
- **Always start from "now".** Loses every event during downtime.

## Consequences
- Wikimedia → Kafka is **at-least-once**, provided downtime is shorter than upstream retention (7–31 days). Beyond that, the gap is permanent upstream data loss, which is detectable as a jump in the time series.
- Multi-DC caveat: the SSE id contains entries for both `eqiad` and `codfw`. On a Wikimedia datacenter switchover the active topic changes, so we store the full array and pass it back unchanged instead of interpreting a single entry.
