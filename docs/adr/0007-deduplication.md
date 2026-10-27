# ADR-0007 — Deduplication

- **Status:** Proposed. The horizon is confirmed by R-1 (reconnect overlap) and R-3 (rcid duplicates).
- **Date:** 2026-10-26

## Context
Duplicates can enter `raw` through:
- (a) the overlap on every SSE reconnect, which happens at least every 15 minutes;
- (b) the resume safety margin after a producer restart ([ADR-0012](0012-producer-resume-from-kafka.md));
- (c) Kafka producer retries across producer sessions (idempotence only covers a single session);
- (d) deliberate operator replays.

Cases (a)–(c) arrive **close together in arrival time** (seconds to minutes). Case (d) is unbounded, and is handled by idempotent results ([ADR-0008](0008-postgres-sink-semantics.md)), not by dedup.

## Decision
- **Key:** `meta.id` (a UUID that the schema defines as the unique event ID; 0 collisions in the sample).
- **Mechanism:** `keyBy(meta.id)` → `KeyedProcessFunction` with `ValueState<Boolean> seen`. The first occurrence passes; later ones are counted (`duplicates_dropped`) and discarded.
- **Bounding:** state TTL (`StateTtlConfig`, `OnCreateAndWrite`, `NeverReturnExpired`) with **horizon H = 1 h** (ASSUMPTION), plus RocksDB compaction-filter cleanup.
- **Why processing-time TTL is right here:** Flink state TTL is processing-time based. Usually that is a drawback, but the duplicates we target are created by ingestion mechanics, which also operate in processing time.
- **State size estimate:** 47 ev/s × 3,600 s ≈ 170 k keys. At about 100 B each, that is ≈ 17 MB. Trivial.

## Alternatives
- **Dedup inside each event-time window** (count distinct IDs). This only catches duplicates that fall in the same window, and it ties dedup to analytics.
- **Event-time timers for cleanup.** Deterministic under replay, but needs one timer per key. TTL is simpler for the same effect at our horizon.
- **Bloom filter (operator state).** Tiny memory, but it has false positives (it would drop real events) and is hard to rescale.
- **Dedup in PostgreSQL** with a unique constraint on the event ID. It would require storing every event, which is not the serving model.

## Consequences
- Keying by `meta.id` shuffles every event across the network. That is cheap at our rate, and the analytics re-key anyway.
- A duplicate arriving after more than H is counted twice. H is tuned with metrics: we record duplicate age at detection.
- R-3 may reveal duplicates with *different* `meta.id` but the same `(wiki, rcid)`. If so, a second key is added for types where rcid is non-null.
