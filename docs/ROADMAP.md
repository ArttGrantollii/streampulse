# StreamPulse roadmap

The project is built in phases. Each phase ends with:
- tests run;
- `ARCHITECTURE.md`, ADRs, `INTERVIEW_QUESTIONS.md` and `LEARNING_JOURNAL.md` updated;
- a stop for review.

**Status:** ✅ done · 🔜 next · ⏳ planned

## Product framing (revised 2026-10-26)
**Headline question:** *Can Wikipedia editing activity detect breaking news, and how quickly?*

The engineering (event time, dedup, delivery guarantees, crash recovery) is what makes the answer trustworthy. The public showcase ([ADR-0013](adr/0013-public-showcase.md)) is a zero-install static site with a live in-browser view, recorded real results and the engineering proof.

## Scope revision (Phase 0 review)
The first draft had 12 phases. It was cut to **6** to make sure the project ships:

| Change | Why |
|---|---|
| Old 3 + 4 + 5 merged into **Phase 3** | Parsing, event time, dedup and the PostgreSQL sink form one correctness story, and are tested together |
| Spike detection becomes the **headline (Phase 4)**, with external validation | It is the only output a visitor finds interesting. Validation turns a tutorial-style idea into an original result |
| Observability, CI and benchmarks are **folded into Phases 2–5** | They support the proof rather than being destinations in themselves |
| Full backpressure study, savepoint upgrade demo, rescaling demo and cloud mapping → **stretch** | Valuable but not needed for the story. They can be added later |
| New **Phase 6: public showcase** | Visitors never run Docker ([ADR-0013](adr/0013-public-showcase.md)) |
| Still removed: clean topic, analytics topics, Schema Registry, MinIO | No consumer or benefit (ADR-0004, 0005, 0009) |

---

## Phase 0 — Research & architecture ✅
- **Delivered:** research with live-data evidence, a version baseline, 13 ADRs, the architecture, this roadmap and the living docs.

## Phase 1 — Infrastructure foundation ✅
- **Goal:** `docker compose up -d` brings up Kafka (KRaft), a Flink JobManager and TaskManager, and PostgreSQL. Topics and migrations are created automatically, and a Java smoke job proves Flink 2.2.1 can read Kafka 4.2.2 and checkpoint durably.
- **Learned:** KRaft, listeners, topic configuration, Flink session clusters and slots, health checks, named volumes, and Flyway migrations.
- **Acceptance:** see [§ Phase 1 acceptance criteria](#phase-1-acceptance-criteria). All 10 were met on 2026-10-27, except that `make` itself awaits installation on the dev machine (the Makefile targets wrap commands that were each run and verified directly).

## Phase 2 — Ingestion ✅ (24-hour measurement running)
- **Goal:** a Python producer with `live`, `replay` and `record` modes sharing one publishing path.
  - Live mode: SSE parsing, reconnects with jittered backoff, and resume ([ADR-0012](adr/0012-producer-resume-from-kafka.md)).
  - Kafka producer settings: idempotence, `acks=all`, zstd, and the murmur2 partitioner.
  - Graceful shutdown, structured logs, and a `/metrics` endpoint.
- **Experiments:** a 24 h capture, then measured disorder, reconnect overlap, clock offset and canaries (R-1…R-5). The 24 h recording also becomes the `make demo` dataset (trimmed) and the replay fixture.
- **CI:** GitHub Actions runs lint and unit tests for the producer.
- **Learned:** partitions, keys, ordering, acknowledgements, idempotence scope, backoff and graceful shutdown.
- **Acceptance:**
  - unit tests for the SSE parser and key extraction;
  - an integration test for replay against a real broker;
  - a forced disconnect resumes without gaps;
  - `docs/research/0002-*` is published.
- **Status (2026-10-28):**
  - ✅ 65 unit tests and 9 integration tests, green in CI;
  - ✅ restart resume measured (overlap = margin, no gap);
  - ✅ graceful `docker stop`;
  - ✅ CI workflow;
  - ✅ Research 0002 published with initial findings.
  - ⏳ The 24-hour section of Research 0002 is due after 2026-10-29 13:10 UTC.
- **Change from plan: no Testcontainers.** Integration tests run against the *same* pinned broker and topic setup as development (`docker compose up kafka kafka-init`, locally and in CI). That's one fewer framework, and it tests the real configuration rather than a look-alike.

## Phase 3 — Correct stream processing ⏳
- **Goal:** the Java Flink job.
  - Parse and validate into `EditEvent`, with a DLQ carrying the reason, and a canary filter.
  - Event time and watermarks using the measured parameters ([ADR-0006](adr/0006-event-time-and-watermarks.md)).
  - Dedup with TTL ([ADR-0007](adr/0007-deduplication.md)).
  - Allowed lateness, plus a `late.v1` side output.
  - 1-minute activity windows (by wiki, project family, human vs bot), written to PostgreSQL via **idempotent upserts** ([ADR-0008](adr/0008-postgres-sink-semantics.md)), plus a sitematrix-based `dim_wiki`.
  - A local Grafana with one provisioned analytics dashboard.
- **Learned:** sources and splits, watermarks and idleness, side outputs, keyed state and TTL, windows and allowed lateness, idempotency, operator test harnesses.
- **Acceptance:**
  - parser and validation unit tests;
  - operator-harness tests for dedup and lateness classification;
  - a fixture replay yields exactly the expected window counts;
  - **replaying the same fixture twice leaves the tables identical**;
  - CI runs the Java tests.

## Phase 4 — Breaking-news detection (the headline) ⏳
- **Goal:**
  - per-page sliding counts and a two-phase top-N;
  - a trending score;
  - the spike detector ([ADR-0010](adr/0010-spike-detection.md)), with spikes written to PostgreSQL.
- **Validation:** run the real pipeline for several days, then evaluate detected spikes against an independent signal. The candidate is Wikimedia pageview data (to be researched and verified at the start of the phase), plus a manually reviewed sample against news. The metrics are precision, lead time ("edits spiked X minutes before pageviews"), and false-positive classes (edit wars, bot-like humans, cleanup campaigns).
- **Learned:** sliding windows and their cost, top-N patterns, event-time timers, statistics on sparse counts, and evaluating a detector honestly.
- **Acceptance:**
  - unit tests on synthetic sequences;
  - `docs/research/0003-spike-validation.md` with measured precision and lead time, and examples of hits and misses.

## Phase 5 — Reliability proof ⏳
- **Goal:**
  - tuned checkpointing ([ADR-0009](adr/0009-state-backend-and-checkpoint-storage.md));
  - an automated **crash test**: deterministic replay → kill a TaskManager mid-run → recovery → compare tables with a failure-free run → **PASS/FAIL**, with recovery time and the restored checkpoint ID;
  - the watermark-after-restore hazard test;
  - Prometheus plus one local operations dashboard (throughput, lag, checkpoints, DLQ/late/duplicates, latency);
  - one measured throughput ceiling (maximum replay rate, and the first bottleneck);
  - CI runs selected integration tests.
- **Learned:** checkpoint barriers, recovery, restart strategies, metric types, consumer lag vs `pendingRecords`, latency clock domains.
- **Acceptance:** `make failure-test` prints PASS reproducibly, and the measured numbers are recorded in `docs/research/0004-*`.

## Phase 6 — Public showcase & release ⏳
- **Goal:** a static site on GitHub Pages ([ADR-0013](adr/0013-public-showcase.md)):
  - **Live:** the browser connects to Wikimedia, with an interactive watermark, window, late and duplicate illustration, clearly labelled.
  - **Results:** spikes and validation exported as static JSON from the real run.
  - **Engineering:** architecture, crash-test result, measured numbers, links to code.
- **Also:** `make demo` for engineers, the final README, a CV bullet built only from measured numbers, and an interview walkthrough.
- **Acceptance:**
  - it loads fast on a phone, needs no install, and every number links to its evidence;
  - a fresh clone runs `make up && make demo` successfully;
  - CI deploys the site.

## Stretch goals (each needs a trigger)
| Item | Trigger |
|---|---|
| Full backpressure study, savepoint upgrade and rescaling demos | Time after the release; strong interview material |
| `page_change` join for stable page IDs | Page-move splits are visible in results |
| `clean` topic + exactly-once Kafka output (ADR-0008 Option C) | A second consumer exists |
| Schema Registry + Avro | Several independent producers or consumers |
| MinIO/S3 checkpoints, Kubernetes + Flink Operator, cloud mapping | Multi-host or cloud deployment |
| ClickHouse comparison | Event-level ad-hoc analytics is needed |

---

## Phase 1 acceptance criteria

Phase 1 is complete when **all** of the following are true:

1. **One command:** `docker compose up -d` (or `make up`) starts `kafka`, `flink-jobmanager`, `flink-taskmanager` and `postgres`. The one-shot `kafka-init` (topics) and `db-migrate` (Flyway) containers then exit with code 0.
2. **Health:** every long-running service has a Compose health check and reports `healthy`. Dependencies use `depends_on: condition: service_healthy` or `service_completed_successfully`.
3. **Kafka:** single-node KRaft (combined broker and controller) with separate internal and host listeners. The three topics from [ADR-0004](adr/0004-kafka-topic-topology.md) exist with the specified partitions, retention and compression. Re-running init is a no-op.
4. **PostgreSQL:** the Flyway baseline migration has been applied (`flyway_schema_history` shows it). There is no manual SQL.
5. **Flink:** the UI is reachable at `localhost:8081` with 1 TaskManager registered. `config.yaml` comes from the repository (RocksDB, the checkpoint dir on the `flink-data` volume, the restart strategy).
6. **Connector smoke test:** a minimal Java job (Maven build in Docker, Flink 2.2.1, kafka connector 5.0.0-2.2) reads a smoke topic and completes at least 2 checkpoints, with checkpoint files present in the volume. `make smoke-test` produces N records, verifies that the job consumed N, and prints **PASS**.
7. **Durability:** after `docker compose restart`, Kafka data, migration history and checkpoint directories all still exist.
8. **Hygiene:** every image is pinned exactly; `.env.example` is complete, `.env` is gitignored, and no secrets are committed; shell scripts use LF (`.gitattributes`).
9. **Developer interface:** `make up | down | logs | ps | smoke-test | clean`. Each target is a thin wrapper over a documented `docker compose` command, so it also works from PowerShell.
10. **Documentation:** `ARCHITECTURE.md` (status markers), `INTERVIEW_QUESTIONS.md` (Phase 1), `LEARNING_JOURNAL.md` (Phase 1) and the README quick start are updated.
