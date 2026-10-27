# ADR-0011 — PostgreSQL rather than ClickHouse for V1 serving

- **Status:** Accepted
- **Date:** 2026-10-26

## Context
The serving layer stores **pre-aggregated** windowed results, not raw events:
- **Activity facts**: ~93 wikis × a few types × 2 bot values per minute, which is thousands of rows per hour.
- **Top-N and trending snapshots**: bounded by N.
- **Spike events**: tens per day.

Grafana queries are recent time ranges by key.

## Decision
**PostgreSQL 18** with:
- versioned migrations;
- natural primary keys used for idempotent upserts ([ADR-0008](0008-postgres-sink-semantics.md));
- BRIN or B-tree indexes on time;
- retention by deletion or time partitioning. Partitioning is decided in Phase 3 from measured table growth.

## Alternatives
- **ClickHouse.** Superb for scanning billions of raw events with columnar compression, and its `ReplacingMergeTree` gives a form of idempotency. But:
  - those dedup semantics are *eventual* (resolved at merge time, or with `FINAL` at query time), which is a weaker and subtler contract than a PostgreSQL primary key;
  - we don't store raw events in the serving layer;
  - it is one more specialised system.

  It is the right choice if we later serve **event-level** ad-hoc analytics, and a ClickHouse comparison is a stretch goal.
- **TimescaleDB.** Hypertables and continuous aggregates fit the use case, but it is an extension that requires a non-stock image, and its value is small at our row counts.
- **Apache Druid / Pinot.** Real-time OLAP at a scale we don't have.

## Consequences
- ACID upserts with real primary keys make the correctness story simple to prove.
- PostgreSQL limits (write amplification, vacuum) are measured in Phase 5 instead of assumed.
