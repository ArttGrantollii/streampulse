# StreamPulse

Real-time analytics over Wikimedia's worldwide live change stream, built with Kafka, Flink (Java), PostgreSQL, Prometheus and Grafana.

**Headline question:** *can Wikipedia editing activity detect breaking news, and how quickly?*

The project is designed to show *how* streaming correctness is reasoned about:
- event time with measured lateness bounds;
- stateful deduplication;
- per-boundary delivery guarantees;
- crash recovery verified by an automated test.

> **Status: Phase 1 complete (local infrastructure).** Kafka, Flink and PostgreSQL run locally with one command, verified end to end by a smoke test. No Wikimedia data flows yet. That starts in Phase 2. See the [roadmap](docs/ROADMAP.md).

## Quick start

**Prerequisites:** Docker Desktop (with WSL integration on Windows), about 6 GB of free memory for Docker, and `make` (`sudo apt install make` in WSL/Ubuntu). No local JDK or Maven is needed; builds run in containers.

```bash
make up            # start Kafka, Flink, PostgreSQL; create topics; run migrations; wait until healthy
make build-jobs    # build + unit-test the Flink job jar (Maven in Docker)
make smoke-test    # prove Kafka -> Flink -> RocksDB state -> durable checkpoints; prints PASS
make help          # all commands
```

- Flink UI: http://localhost:8081
- Kafka from the host: `localhost:9094`
- PostgreSQL: `localhost:5432` (credentials in `.env`, created from `.env.example` on first `make up`)

<details>
<summary>Without make (e.g. PowerShell)</summary>

| make | Equivalent |
|---|---|
| `make up` | copy `.env.example` to `.env`, then `docker compose up -d` and wait until `docker compose ps` shows the long-running services healthy (`scripts/wait_for_stack.sh` does this in bash) |
| `make build-jobs` | `docker compose --profile build run --rm flink-jobs-build` |
| `make smoke-test` | `bash scripts/smoke_test.sh` (needs bash: WSL or Git Bash) |
| `make down` | `docker compose down` (data is kept) |
| `make clean CONFIRM=yes` | `docker compose down -v` (**deletes** all data volumes) |

</details>

## What exists today
- **Local platform** ([ARCHITECTURE §3.1](docs/ARCHITECTURE.md#31-local-platform)):
  - single-node KRaft Kafka 4.2.2 with the three topics created and their configs enforced;
  - PostgreSQL 18.6 with Flyway migrations;
  - a Flink 2.2.1 session cluster with RocksDB and checkpoints on a persistent volume.

  Health-gated startup; everything pinned.
- **Source validation from live data:** [Research 0001](docs/research/0001-eventstreams-source-validation.md). A 2-minute capture overturned four assumptions: the stream covers all Wikimedia projects (not just Wikipedia), `recentchange` has no page IDs, `meta.dt` is emission time rather than edit time, and most events are not edits.
- **Verified version baseline:** [COMPATIBILITY.md](docs/COMPATIBILITY.md).
- **Architecture:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). Decisions: [docs/adr/](docs/adr/README.md).
- **Interview prep:** [docs/INTERVIEW_QUESTIONS.md](docs/INTERVIEW_QUESTIONS.md). Journal: [docs/LEARNING_JOURNAL.md](docs/LEARNING_JOURNAL.md).

## Planned architecture
```text
Wikimedia EventStreams (SSE) ─▶ Python producer ─▶ Kafka raw.v1 ─▶ Flink job ─▶ PostgreSQL ─▶ Grafana
                                    (live|replay)          │            ├─▶ dlq.v1 (invalid)
                                                           │            └─▶ late.v1 (too late)
                     Prometheus ◀── metrics from producer, Kafka, Flink, PostgreSQL ──▶ Grafana
```

## Guarantees (as designed, to be proven in Phase 5)
StreamPulse does **not** claim end-to-end exactly-once. It claims *convergent results*: after a crash, the result tables match a failure-free run, through checkpointed state plus idempotent upserts. The per-boundary table is in [ARCHITECTURE §15](docs/ARCHITECTURE.md#15-delivery-semantics-boundary-by-boundary).

## Reproduce the research
```bash
python3 scripts/research/sample_eventstreams.py recentchange 120 rc.ndjson
python3 scripts/research/analyze_sample.py rc.ndjson
```

## License
Apache-2.0
