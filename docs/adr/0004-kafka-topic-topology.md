# ADR-0004 — Kafka topic topology

- **Status:** Accepted (retention and partitions are re-evaluated with measurements in Phase 5)
- **Date:** 2026-10-26

## Context
The specification proposed seven topics (raw, clean, dlq, late, plus three analytics outputs). Every topic carries a cost: storage, an extra serialization hop, schemas to version, and consumers to operate. A topic is justified only when it has a consumer or a replay requirement.

## Decision
Naming convention: `<source>.<entity>.<stage>.v<major>`. `v<major>` is bumped only for a breaking change to the *record contract* on that topic.

| Topic | Producer | Consumer(s) | Key | Partitions | Retention | Why it exists |
|---|---|---|---|---|---|---|
| `wikimedia.recentchange.raw.v1` | Python producer | Flink analytics job | `wiki` + `\|` + `title` (null if the payload is unparseable) | 3 | 7 days, plus a `retention.bytes` safety cap | Immutable, replayable source of truth. Holds exactly what Wikimedia sent |
| `wikimedia.recentchange.dlq.v1` | Flink | Humans / debugging tools | null | 1 | 14 days | Invalid records, with error context. They must not crash the job or disappear silently |
| `wikimedia.recentchange.late.v1` | Flink | Audit, reprocessing, metrics | same as raw | 1 | 14 days | Valid events that arrived after the allowed lateness. They are kept, not dropped |

**Common settings:** `compression.type=producer` (the producer compresses with zstd), `cleanup.policy=delete`, `min.insync.replicas=1`, replication factor **1 locally** (single broker). Production would use RF=3, `min.insync.replicas=2` and `acks=all`. Topics are created by an idempotent init container; nobody clicks anything in a UI.

## Why no `clean` topic
In V1, validation, dedup and analytics run in **one Flink job**, so a clean topic would have no consumer. Writing it would double storage and add a transactional hop. Clean events are re-derivable at any time from raw plus the job code, which is the point of an immutable raw log. **We would add it** when a second, independently deployed consumer needs validated events (another team or job). Exactly-once production into it would then make sense ([ADR-0008](0008-postgres-sink-semantics.md), Option C).

## Why no analytics output topics
Results go straight to PostgreSQL through idempotent upserts ([ADR-0008](0008-postgres-sink-semantics.md)). Output topics only make sense with a real consumer, such as an alerting service for spikes. `wikimedia.spikes.v1` is a documented stretch goal if notifications are built.

## Why 3 raw partitions
- Throughput doesn't require it: ~47 events/s × 1.4 KB ≈ 64 KB/s, which one partition handles easily.
- The choice is about **parallelism and learning**. Kafka partitions cap source parallelism, so 3 partitions let us run the source at parallelism 1–3 and demonstrate rescaling, per-partition watermarks and idle partitions.
- Kafka can add partitions later but can never remove them, and adding them remaps keys. Choosing 3 now avoids that.

## Why key by `(wiki, title)`
Per-page ordering within a partition, and an even spread. Keying by `wiki` alone would put about 36% of events (commonswiki) on one partition.

**Important:** the Python client must use `partitioner=murmur2_random` so that partitioning matches Java clients. librdkafka defaults to CRC32.

## Sizing (measured, Research 0001)
~0.8 GB/day after zstd, so ≈ 5.5 GB for 7 days. `retention.bytes` caps each partition so a laptop disk can't fill up.

## Consequences
- Rebuilding everything is "reset the consumer to offset 0 on raw" (or restart the job without state), going back up to 7 days.
- The DLQ and late topics are written at-least-once, so duplicates are possible there and acceptable. Each record carries the original `meta.id` and raw coordinates.
