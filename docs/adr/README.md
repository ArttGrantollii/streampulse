# Architecture Decision Records

Each ADR records one significant decision: its context, the alternatives considered, and the consequences. Statuses:

- **Accepted**: the decision has been made, though the implementing phase may not have started yet.
- **Proposed**: the direction is chosen, but parameters depend on a measurement listed in the ADR.
- **Superseded**: replaced by a later ADR. The old text stays for history.

| # | Decision | Status | Implemented in |
|---|---|---|---|
| [0001](0001-source-stream-selection.md) | Use `mediawiki.recentchange` as the V1 source | Accepted | Phase 2 |
| [0002](0002-flink-java-over-pyflink.md) | Flink jobs in Java (DataStream API), not PyFlink | Accepted | Phase 1+ |
| [0003](0003-version-baseline.md) | Version baseline: Flink 2.2.1, Kafka 4.2.2, PostgreSQL 18 | Accepted | Phase 1 |
| [0004](0004-kafka-topic-topology.md) | Three topics: raw, DLQ, late. No clean or analytics topics in V1 | Accepted | Phase 1–3 |
| [0005](0005-serialization-and-schema-contracts.md) | JSON, raw payload stored verbatim, typed Java contract, no Schema Registry | Accepted | Phase 2–3 |
| [0006](0006-event-time-and-watermarks.md) | Event time = `timestamp`; bounded out-of-orderness; side-output late events | Proposed (parameters pending R-2) | Phase 3 |
| [0007](0007-deduplication.md) | Dedup on `meta.id` with processing-time state TTL | Proposed (horizon pending R-1) | Phase 3 |
| [0008](0008-postgres-sink-semantics.md) | Idempotent UPSERTs with deterministic keys (not XA) | Accepted | Phase 3 |
| [0009](0009-state-backend-and-checkpoint-storage.md) | RocksDB + incremental checkpoints on a shared Docker volume, no MinIO | Accepted | Phase 1 and 5 |
| [0010](0010-spike-detection.md) | Poisson tail test against an EWMA baseline with a minimum volume | Proposed | Phase 4 |
| [0011](0011-postgresql-over-clickhouse.md) | PostgreSQL, not ClickHouse, for V1 serving | Accepted | Phase 3 |
| [0012](0012-producer-resume-from-kafka.md) | Producer resumes from the SSE id stored in Kafka record headers | Proposed (validate R-1) | Phase 2 |
| [0013](0013-public-showcase.md) | Public showcase is a static site (live in-browser view + recorded results), not a hosted pipeline | Accepted | Phase 6 |
