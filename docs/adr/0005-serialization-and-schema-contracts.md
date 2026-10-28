# ADR-0005 — Serialization and schema contracts

- **Status:** Accepted
- **Date:** 2026-10-26

## Context
- Upstream publishes JSON Schemas, but events already drift from them: events declare `1.0.0` while `1.0.1` is the latest, and they contain five undocumented fields.
- Only `$schema`, `meta`, `meta.dt` and `meta.stream` are required.
- There are exactly two producers (our Python producer and the Flink job) and few consumers.

## Decision
1. **Raw topic value = the upstream `data` bytes, verbatim.** The producer does not parse-and-re-serialize, because that would silently normalise or lose fields. Our metadata travels in **Kafka headers**:
   - `sp-ingest-ts`: producer receive time, epoch ms
   - `sp-sse-id`: the upstream SSE id JSON, used for resume
   - `sp-producer`: producer name and version
   - `sp-mode`: `live` or `replay`
   - `sp-connection`: which upstream connection delivered it (`live-<session>-<n>` or `replay`), so duplicates can be attributed to reconnects or restarts
   - `sp-replayed-at`: replay only

   The Kafka record timestamp (`CreateTime`) is the ingest time in live mode. **In replay mode it's the replay time:** Kafka retention deletes by record timestamp, so a recording's old timestamps would expire immediately. The original ingest time survives in `sp-ingest-ts`. (Implemented in Phase 2.)
2. **The Wikimedia schema is vendored** under `schemas/wikimedia/` (pinned 1.0.1) and used in tests and fixtures, not at runtime.
3. **The internal contract is a typed Java record `EditEvent` (v1),** produced by one parser with an explicit validation step. The parser **ignores unknown fields** and **tolerates missing optional ones**. It rejects a record only when *identity or time* is unusable:
   - invalid JSON
   - missing `meta.id`
   - missing both `timestamp` and `meta.dt`
   - missing `wiki` or `title` on an edit
   - a timestamp outside sanity bounds relative to ingest time
4. **Records we publish (DLQ, late)** have versioned JSON Schemas in `schemas/streampulse/`. Each record carries `schema_version`. Changes within a major version are additive only; a breaking change creates a new topic `.v2`.
5. **No Schema Registry and no Avro/Protobuf in V1.**

## Alternatives
| Option | Assessment |
|---|---|
| Avro/Protobuf + Schema Registry | Gives compile-time contracts, compact encoding and registry-enforced compatibility. But upstream is JSON, so we would *translate* and lose the verbatim raw record. It adds a stateful service to operate for two producers. Worth it with many teams and producers; at our scale it is architecture decoration. Revisit if a `clean` topic gets several consumers |
| JSON Schema validation at runtime in Flink | Validating a full draft-07 schema per record is slow, and upstream's own events don't match its latest schema exactly. Explicit validation of the fields we depend on is faster and more honest |
| Pydantic models in the producer | The producer does not need the fields. Parsing would only be needed to extract the partition key, which uses a minimal extraction. Pydantic is reserved for tests or tools if it proves useful |

## Consequences
- Upstream schema evolution is absorbed in exactly one place, the parser, and is covered by fixture tests: unknown field, missing optional field, type change.
- Malformed records still land in raw (with a null key), so the DLQ path can be demonstrated end-to-end.
- Readers of raw must handle JSON. That is acceptable at about 50 events/s.
