# ADR-0006 — Event time, watermarks and late data

- **Status:** Proposed. The strategy is decided; numeric parameters wait for experiment R-2 (24 h disorder measurement, Phase 2).
- **Date:** 2026-10-26
- **Evidence:** [Research 0001 §5](../research/0001-eventstreams-source-validation.md#5-timestamps-and-out-of-orderness)

## Context
- `timestamp` (rc_timestamp, 1 s resolution) is when the edit was saved.
- `meta.dt` (ms) is when the event was emitted, 30 ms–50 s later.
- In stream order, `timestamp` is out of order by up to ~50 s (p99 8 s, p99.9 26 s in a 2-minute sample).
- Processing-time analytics would attribute an edit to whenever *we* happened to receive it, and would give different answers on replay.

## Decision
1. **Event time = `timestamp` × 1000.** Fall back to `meta.dt` only if `timestamp` is absent (it is optional in the schema). We record which source was used (`event_time_source`).
2. **Watermarks:** `WatermarkStrategy.forBoundedOutOfOrderness(B)` assigned **in the Kafka source**, so watermarks are tracked *per partition* and merged by taking the minimum.
3. **Idleness:** `withIdleness(I)` so that a partition with no data doesn't stall progress. It mainly matters for replay and small fixtures, since live traffic keeps all 3 partitions busy.
4. **Allowed lateness L** on windows: late-but-allowed events update an already-emitted window result. The upsert sink makes the update safe ([ADR-0008](0008-postgres-sink-semantics.md)).
5. **Beyond L:** side-output to `wikimedia.recentchange.late.v1` and count in a metric. Never drop silently.
6. **Sanity bounds** (DLQ, not late): event time more than **+5 min** in the future, or more than **31 days** in the past, relative to `sp-ingest-ts`. 31 days is the maximum upstream retention, so nothing older can be legitimately replayed. Both are measured against the *original* ingest time, which replay preserves, so validation is deterministic.

**Initial parameters (ASSUMPTION until R-2):** B = 10 s (≈ p99 observed), L = 2 min (covers the observed max of 50 s with margin), I = 30 s.

**Early evidence (2026-10-28, PRELIMINARY):**
- Disorder per Kafka partition for **edits only** reached p99 25 s, p99.9 108 s and max 195 s in a 10-minute sample ([Research 0002 §1.7](../research/0002-ingestion-behaviour.md)), so B = 10 s is likely too small.
- A legitimate Commons log event carried a 2005 timestamp (§1.5), so sanity bounds must not treat all old timestamps as invalid.
- Parameters will be set from the 24-hour run.

**Selection rule once data exists:** B ≈ p99 and L ≈ p99.99 of measured disorder. Results are emitted early, and the tail corrects them.

## Alternatives
- **`meta.dt` as event time.** Much less disorder, so a smaller B and lower latency. Rejected: it measures when Wikimedia's pipeline emitted the event, not when the edit happened. That is the same mistake as using processing time, one hop upstream. This trade-off is documented in the interview notes.
- **Huge B (e.g. 60 s), no allowed lateness.** Simpler, but every result waits a minute.
- **Processing time.** Non-deterministic and wrong on replay.

## Consequences and known hazards
- **Results latency ≥ window size + B.** A 1-minute window closes at about end + 10 s. This is a deliberate trade-off.
- **Determinism hazard (FACT about Flink mechanics):**
  - Watermarks are emitted periodically (`pipeline.auto-watermark-interval`, 200 ms by default) and merged across partitions. Whether a borderline event counts as "late" can therefore depend on arrival timing and replay speed.
  - Deterministic correctness tests must use either fixtures whose lateness is unambiguous (far inside or far beyond L) or a single partition.
- **Recovery hazard (EXPERIMENT, Phase 5):**
  - After restoring from a checkpoint, the source restarts watermark generation from scratch. Events that were late before the failure might be accepted after it, creating a fresh window holding a partial count.
  - With overwrite-upserts, that partial count could replace a complete row.
  - Phase 5 must test this explicitly. Candidate mitigations: a per-key "max closed window" guard in state, or merge semantics in the sink.
