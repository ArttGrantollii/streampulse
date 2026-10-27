# ADR-0002 — Flink jobs in Java (DataStream API), not PyFlink

- **Status:** Accepted
- **Date:** 2026-10-26

## Context
Flink is the core learning component. The owner is more fluent in Python, but the portfolio aims to demonstrate production-grade stateful streaming, including precise delivery semantics. The facts below were checked against the Flink 2.2 and JDBC connector 4.1.0 documentation.

| Capability | Java DataStream | PyFlink DataStream |
|---|---|---|
| Kafka source/sink | First-class | Available (wraps the Java connectors) |
| JDBC **exactly-once (XA) sink** | Supported (`buildExactlyOnce`) | **"Still not supported in Python API"** (connector docs) |
| Keyed state, timers, side outputs, windows | Full | Supported in both execution modes |
| Async I/O, window joins, interval joins, iterate | Full | **Not supported** in either Python execution mode |
| Execution model | In the JVM | Default **PROCESS** mode: Python workers in separate processes, with records serialized across a gRPC boundary. THREAD mode embeds Python through PEMJA, still has the GIL, and silently falls back to PROCESS where unsupported |
| Testing | `flink-test-utils`, operator test harnesses, MiniCluster | Less mature, so tests usually end up being end-to-end |
| Type/serializer control | POJO/record serializers, explicit `TypeInformation`, state schema evolution | Pickled or Row types. State evolution is harder to control |
| Debugging/profiling | Standard JVM tools and the Flink UI map 1:1 to code | Two runtimes to debug |
| Industry usage for core streaming jobs | Dominant | Common for ML/feature pipelines and the Table API |

## Decision
Write all Flink jobs in **Java 17** using the **DataStream API**. Python stays where it is the better tool: the ingestion producer, research scripts and test tooling.

## Alternatives
- **PyFlink DataStream.** Rejected. It lacks the XA sink and several operators, adds serialization overhead on every record in default mode, and has weaker test tooling. It would hide exactly the JVM/runtime behaviour (serializers, state backends, operator chaining) that this project is meant to teach.
- **Flink SQL / Table API (Java or Python).** It makes windows and top-N concise (`TUMBLE`, `ROW_NUMBER`). It hides state, timers and watermark mechanics, and custom stateful logic (dedup horizon, spike baseline) ends up in UDFs anyway. We may use SQL later for one comparison query, but not for the core.
- **Kotlin/Scala.** Rejected: the Scala API was removed in Flink 2.0, and Java is the lingua franca for Flink jobs.

## Consequences
- The owner learns Java/Flink idioms. Code stays plain Java: records, no frameworks, small classes and thorough tests.
- The repository becomes polyglot (Python producer, Java jobs), which CI must handle (Phase 5).
- Exactly-once options remain open (see [ADR-0008](0008-postgres-sink-semantics.md)).
