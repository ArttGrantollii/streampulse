# Technology compatibility matrix

Researched **2026-10-26** (Phase 0). Rows marked ✅ **run in our stack** (Phase 1, 2026-10-27). Decision record: [ADR-0003](adr/0003-version-baseline.md).

**Selection rules:**
1. Use the newest *stable* release that is compatible with every connector we need.
2. Never use the `latest` tag, and never use a release less than about two weeks old.
3. Pin exact patch versions in Docker tags and build files.
4. Re-verify each row when the phase that first uses it begins. A component is not "verified" until it runs in our stack.

| Component | Pinned version | First used | Source checked | Rationale |
|---|---|---|---|---|
| **Apache Flink** ✅ | **2.2.1** (2026-05-15) | Phase 1 | [flink.apache.org/downloads](https://flink.apache.org/downloads/) | **2.3.0 (2026-06-25) is newer, but no official Kafka or JDBC connector supports 2.3 yet.** Kafka connector 5.0.0 supports 2.1.x/2.2.x and JDBC connector 4.1.0 supports 2.1.x/2.2.x. 2.2.1 is the newest patch release in the newest line that both connectors support. 1.20.x LTS was rejected: it would lock the project onto the legacy 1.x API |
| Flink Docker image ✅ | `flink:2.2.1-scala_2.12-java17` | Phase 1 | Docker Hub | Official image. Runs OpenJDK 17.0.20, ships the RocksDB backend in `flink-dist` and the Prometheus reporter in `plugins/metrics-prometheus/`. Java 17 is the default and recommended JVM for Flink 2.x ([Java compatibility](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/java_compatibility/)). Java 21 is "experimental" |
| **Java (Flink jobs)** ✅ | **17** (Temurin) | Phase 1 | Flink Java compatibility docs | Must match the runtime image. We compile with `--release 17`. Builds can run on a newer local JDK (the dev machine has 21) |
| Build tool ✅ | Maven 3.9.16 (`maven:3.9.16-eclipse-temurin-17`) | Phase 1 | Docker Hub | Flink's own quickstarts and connectors use Maven. The shade plugin builds the job uber-jar. Builds run in a container, so no local JDK is required |
| **flink-connector-kafka** ✅ | **5.0.0-2.2** | Phase 1 | Maven Central + downloads page | Compatible with Flink 2.1.x/2.2.x. **Built against kafka-clients 4.2.0** (connector parent POM `kafka.version`; confirmed in our shaded jar) |
| **flink-connector-jdbc-core / -postgres** | **4.1.0-2.2** | Phase 3 | Maven Central + jar inspection | Compatible with Flink 2.1.x/2.2.x. Exposes the Sink V2 API `JdbcSink.builder()…buildAtLeastOnce()/buildExactlyOnce()`. The connector docs still show the legacy `addSink` API, so we follow the jar |
| PostgreSQL JDBC driver | 42.7.x (latest 42.7.14) | Phase 3 | Maven Central | Supplied by us (the connector declares it `provided`). Pin a patch ≥ 2 weeks old at implementation time |
| flink-metrics-prometheus | ships with Flink 2.2.1 | Phase 5 | [Metric reporters](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/metric_reporters/) | `PrometheusReporterFactory`, default port 9249. **Flink histograms are exported as Prometheus *summaries*** (per-instance quantiles that can't be aggregated). This shapes the latency design |
| **Apache Kafka (broker)** ✅ | **4.2.2** (`apache/kafka:4.2.2`, 2026-09-28) | Phase 1 | [kafka.apache.org/community/downloads](https://kafka.apache.org/community/downloads/), Docker Hub | Kafka 4.x is KRaft-only (no ZooKeeper). Supported lines: 4.3.1, 4.2.2, 4.1.2. We chose **4.2.2** because it matches the 4.2 client line the Flink connector is built and tested against, and it is the most recently patched supported release that is more than two weeks old. 4.3.x is acceptable but brings no feature we need |
| Kafka image ✅ | `apache/kafka:4.2.2` | Phase 1 | Docker Hub | The official Apache image (JVM-based), running as `appuser`. `KAFKA_*` environment variables map to broker properties, and KRaft mode requires `CLUSTER_ID` |
| **Python (producer)** | **3.14** line (`python:3.14.x-slim`) | Phase 2 | python.org | Matches the dev machine (3.14.4). 3.14.9 was released 2026-10-21, too recently, so we pin the newest patch that is ≥ 2 weeks old when Phase 2 starts |
| confluent-kafka (librdkafka) | 2.16.x | Phase 2 | PyPI | The mature Python Kafka client with cp314 wheels. **Its defaults differ from the Java client:** `enable.idempotence=false` and `partitioner=consistent_random` (CRC32, not Java's murmur2). We override both. 2.16.0 is 19 days old at the time of writing |
| pydantic | 2.x | Phase 2 (tests) | PyPI | Only if typed producer-side models prove useful. The producer forwards raw bytes, see [ADR-0005](adr/0005-serialization-and-schema-contracts.md) |
| prometheus-client (Python) | 0.26.x | Phase 2 | PyPI | Native Prometheus histograms for producer latency |
| **PostgreSQL** ✅ | **18.6** (`postgres:18.6-bookworm`, released 2026-08-13) | Phase 1 | [Versioning policy](https://www.postgresql.org/support/versioning/) | The newest major, supported until Nov 2030. We use Debian rather than Alpine for glibc locale/collation predictability. **The 18 image moved data to `/var/lib/postgresql/18/docker`; mount the volume at `/var/lib/postgresql`** |
| Migration tool ✅ | **Flyway 13.10.0** (`flyway/flyway:13.10.0-alpine`, 2026-10-08) | Phase 1 | GitHub releases, Docker Hub | Industry-standard versioned SQL migrations, run as a one-shot container. Flyway releases weekly, so 13.10.0 was the newest release more than two weeks old. The alpine variant is about 130 MB smaller |
| Java test stack ✅ | JUnit **5.11.4**, AssertJ **3.27.3**, Log4j **2.24.3** + `log4j-slf4j-impl`, SLF4J **1.7.36** (provided) | Phase 1 | flink-parent 2.2.1 POM | Aligned with what Flink's own test utilities and runtime use, to avoid classpath conflicts. JUnit 6 is newer but not what `flink-test-utils` is built on |
| Maven plugins ✅ | compiler **3.16.0**, surefire **3.5.6**, shade **3.6.2** | Phase 1 | Maven Central `Last-Modified` dates | Newest releases more than two weeks old (surefire 3.6.1 was 5 days old) |
| **Prometheus** | **v3.13.4** (2026-10-02) | Phase 5 | [Release cycle](https://prometheus.io/docs/introduction/release-cycle/) | **3.13 is the current LTS** (supported until 2027-07-31). 3.15.x is newer but not LTS, and 3.15.1/3.13.5 are less than two weeks old |
| **Grafana** | **13.2.3** (2026-09-29) | Phase 3 | GitHub releases | The current major. 13.2.4 (2026-10-13) is under two weeks old, so we bump it when Phase 3 starts |
| kafka-exporter | `danielqsj/kafka-exporter:v1.10.0` | Phase 5 | Docker Hub | Topic offsets and consumer-group lag. v1.11.0 is 3 days old |
| postgres-exporter | pinned in Phase 5 | Phase 5 | — | Connection and database health |

## Compatibility facts worth remembering

- **Kafka client ↔ broker:** modern Kafka clients work with brokers ≥ 2.1 ([Flink Kafka connector docs](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/datastream/kafka/)). Client 4.2.0 with broker 4.2.2 is the tested combination.
- **Connector artifact naming:** `<connector-version>-<flink-minor>`, e.g. `5.0.0-2.2`. The suffix must match the Flink minor version.
- **Flink 2.x configuration:** the cluster file is `config.yaml` (YAML 1.2), and the legacy `flink-conf.yaml` is gone. Checkpoint keys live under `execution.checkpointing.*` (e.g. `execution.checkpointing.dir`, not the 1.x `state.checkpoints.dir`).
- **PyFlink gaps** (relevant to [ADR-0002](adr/0002-flink-java-over-pyflink.md)): the JDBC exactly-once sink is "Still not supported in Python API". Python UDFs run in a separate process by default (gRPC per record batch).
