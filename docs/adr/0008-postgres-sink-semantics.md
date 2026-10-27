# ADR-0008 — PostgreSQL sink semantics: idempotent UPSERTs

- **Status:** Accepted
- **Date:** 2026-10-26

## Context
Enabling checkpointing gives Flink **exactly-once state consistency**: after recovery, operator state reflects each input record once. External effects are a separate question. When a job restores to checkpoint *n*, everything it emitted after checkpoint *n* is emitted **again**. End-to-end guarantees therefore depend on the sink.

## Options compared

| | **A. JDBC XA exactly-once sink** | **B. At-least-once JDBC + idempotent UPSERT** | **C. Kafka exactly-once output → materializer → PostgreSQL** |
|---|---|---|---|
| Mechanism | Two-phase commit: rows are written inside an XA transaction, prepared on the checkpoint barrier, and committed when the checkpoint completes | Rows carry deterministic primary keys, e.g. `(window_start, wiki, type, is_bot)`. `INSERT … ON CONFLICT DO UPDATE` with deterministic values makes replays rewrite identical rows | Flink writes results transactionally to Kafka. A separate consumer (`read_committed`) writes to PostgreSQL, storing consumed offsets in the same DB transaction |
| Guarantee | End-to-end exactly-once *for the restored lineage* | **Effectively-once results** (exactly-once *effect*) for deterministic aggregates. Delivery itself is at-least-once | End-to-end exactly-once if the materializer is transactional |
| Requirements and caveats (from the docs) | PostgreSQL `max_prepared_transactions > 0`; `withTransactionPerConnection(true)` (PostgreSQL allows one XA transaction per connection, so one connection per transaction); **`maxRetries = 0`** or duplicates are possible; Java only | Deterministic keys **and** deterministic values; a stable window definition | Kafka `transaction.timeout.ms` > max checkpoint duration + restart time (or data loss); a unique `transactionalIdPrefix`; an extra service |
| Visibility latency | Results become visible only when a checkpoint completes (≥ the checkpoint interval) | As soon as a batch flushes (seconds) | Visible after the checkpoint, plus materializer lag |
| Failure modes | Orphaned prepared transactions hold locks and block VACUUM if the job never recovers. Restoring an *older* savepoint still duplicates non-idempotent writes | A non-deterministic value (e.g. `now()` in the value, or processing-time logic) breaks idempotency. Late updates must recompute the *full* value, not a delta | Two systems to reason about. Operationally the heaviest |
| Operational cost | Medium–high | **Low** | High |

## Decision
**Option B.** Every result table has a natural deterministic primary key, and the sink writes **absolute values** (the window's full count), never increments (`count = count + x`).
- Window results are re-emitted when late-but-allowed events arrive. The re-emission overwrites the row with the corrected full value.
- Bookkeeping columns such as `updated_at` are excluded from idempotency claims: they record *when* the row was last written, not *what* it says.
- Spike events use a deterministic ID derived from `(wiki, title, window_end)` and are inserted with `ON CONFLICT DO NOTHING`.

**Exact claim we will make:** *"After a failure and recovery, the result tables converge to the same values as a failure-free run, verified by an automated crash test."* We will **not** claim "end-to-end exactly-once delivery".

## Alternatives considered further
- **A** is the textbook answer, and it is used as an interview talking point. For aggregate tables it buys nothing that B doesn't already give, while adding commit latency, prepared-transaction hazards and the `maxRetries = 0` fragility. It would be the right choice for *non-idempotent* effects, such as appending rows that have no natural key.
- **C** is how a platform with many consumers would do it. It becomes worthwhile together with a `clean` or analytics topic ([ADR-0004](0004-kafka-topic-topology.md)). Kept as a possible Phase 6 experiment.

## Consequences
- Correctness depends on determinism. Keys and values must never use processing time. This is enforced by tests and by the recovery hazard test in [ADR-0006](0006-event-time-and-watermarks.md).
- The Phase 5 crash test compares tables between a failure-free run and a run with a crash, on the same replay input.
- DLQ and late Kafka sinks use `AT_LEAST_ONCE` (duplicates are acceptable there; they carry `meta.id`).
