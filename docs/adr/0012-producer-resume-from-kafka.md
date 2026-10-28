# ADR-0012 — The producer resumes from the position stored in Kafka

- **Status:** Accepted. Implemented and tested in Phase 2 (2026-10-28); the duplicate rate over 24 h is in [Research 0002](../research/0002-ingestion-behaviour.md).
- **Date:** 2026-10-26, revised 2026-10-28

## Context
- The producer must resume after reconnects (normal, at least every 15 minutes) and after its own restarts.
- Upstream supports resuming via `Last-Event-ID` or `since`.
- Where should "how far we got" be stored, and what does the stored position actually mean?

## Decision
- **In-process (reconnects):** keep the last *acknowledged-by-Kafka* SSE id in memory, and reconnect with `Last-Event-ID` set to it.
- **On startup (restarts):**
  - Read the **last record of each raw partition** and parse its `sp-sse-id` header.
  - Resume from the **minimum** per-partition upstream timestamp, minus a safety margin `M` (30 s), with `?since=` **in ISO-8601 format**.
  - Duplicates from the margin are removed by Flink dedup ([ADR-0007](0007-deduplication.md)).
- **Guards (added 2026-10-28 after a real incident, see below):**
  - The first event of a `since` connection must be at most 5 min earlier than requested; otherwise the producer stops (exit 1).
  - The producer refuses to resume from more than `--max-resume-age` ago (default 24 h, exit 2).
- **If raw is empty, or ends with replayed records:** start at "now" by default, or at `--since <time>` if given. Replayed data says nothing about how far live ingestion got.

**Incident that shaped the guards (2026-10-28).**
- The first implementation sent `since` as epoch milliseconds.
- Wikimedia ignored it *without an error* and streamed from its oldest retained event, 8 days back. About 85,000 historical events were ingested in under a minute before the test was stopped.
- Fix: send ISO-8601, which a probe showed is honoured to within 0.0 s, and add the guards above. With them, the same failure stops the producer at the first event.
- Lesson: when an upstream can silently fall back, verify the effect, not just the request.

**Why the minimum across partitions.** The idempotent producer keeps ordering *per partition*. After a crash, partition A may hold acknowledged records that are newer than records still in flight to partition B. Resuming from the global maximum could skip B's unacknowledged records. The minimum, minus a margin, can't skip them.

## Alternatives
- **Local state file.** It can diverge from what Kafka actually holds (the file says X, but the crash happened before the ack), and it is lost with the container unless a volume is mounted.
- **Commit the position to a separate compacted Kafka topic.** Correct and common, but it is a second write path. Our raw records already carry the position for free.
- **Always start from "now".** Loses every event during downtime.

## Consequences
- Wikimedia → Kafka is **at-least-once**, provided downtime is shorter than upstream retention (7–31 days). Beyond that, the gap is permanent upstream data loss, which is detectable as a jump in the time series.
- Multi-DC caveat: the SSE id contains entries for both `eqiad` and `codfw`. On a Wikimedia datacenter switchover the active topic changes, so we store the full array and pass it back unchanged instead of interpreting a single entry. **Observed:** the active datacenter changed from eqiad (2026-10-26) to codfw (2026-10-28), and resume worked unchanged.
- **Measured (one test):** after a restart, the overlap equalled the margin exactly (30.0 s), with 949 duplicates and no gap.
