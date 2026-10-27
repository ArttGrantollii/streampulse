# ADR-0003 — Version baseline

- **Status:** Accepted
- **Date:** 2026-10-26
- **Details:** [COMPATIBILITY.md](../COMPATIBILITY.md)

## Context
Distributed streaming stacks fail on version mismatches: connector ↔ Flink minor, client ↔ broker, JVM ↔ runtime. We need a baseline that is current, stable and *mutually* compatible.

## Decision
**Flink 2.2.1** on Java 17, with Kafka connector **5.0.0-2.2** and JDBC connector **4.1.0-2.2**. **Kafka 4.2.2** in KRaft mode. **PostgreSQL 18.6**, **Prometheus 3.13.x LTS**, **Grafana 13.2.x**, **Python 3.14** for the producer. All images are pinned to exact tags. No `latest` tags, and no release younger than about two weeks.

## Alternatives
- **Flink 2.3.0** (newest). Rejected because no released Kafka or JDBC connector targets 2.3. Using the 2.2 connector builds on 2.3 is unsupported, and the connector suffix encodes exactly this.
- **Flink 1.20 LTS.** Very stable with connectors available, but it teaches APIs that 2.x removed (e.g. legacy `SourceFunction`/`SinkFunction`, `flink-conf.yaml`). A 2026 portfolio should show 2.x.
- **Kafka 4.3.x.** Supported and fine, but the Flink connector is built against kafka-clients 4.2.0. Matching the broker to the tested client line removes one variable.

## Consequences
- Upgrade path: when connectors for Flink 2.3 are released, the upgrade becomes a documented exercise (savepoint → upgrade → restore). That is a good Phase 5 or Phase 6 topic.
- Every phase re-verifies the versions it introduces (patch bumps for security fixes are fine).
