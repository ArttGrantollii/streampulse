# StreamPulse

Real-time analytics over Wikimedia's worldwide live change stream, built with Kafka, Flink (Java), PostgreSQL, Prometheus and Grafana.

**Headline question:** *can Wikipedia editing activity detect breaking news, and how quickly?*

The project is designed to show *how* streaming correctness is reasoned about:
- event time with measured lateness bounds;
- stateful deduplication;
- per-boundary delivery guarantees;
- crash recovery verified by an automated test.

> **Status: Phase 2 implemented (ingestion).** Wikimedia's live stream flows into Kafka with resume, backpressure and graceful shutdown, tested against a real broker and in CI. A 24-hour source measurement is running and will set the event-time parameters for Phase 3. See the [roadmap](docs/ROADMAP.md).

## Quick start

**Prerequisites:** Docker Desktop (with WSL integration on Windows), about 6 GB of free memory for Docker, and `make` (`sudo apt install make` in WSL/Ubuntu). No local JDK or Maven is needed; builds run in containers.

```bash
make up            # start Kafka, Flink, PostgreSQL; create topics; run migrations; wait until healthy
make build-jobs    # build + unit-test the Flink job jar (Maven in Docker)
make smoke-test    # prove Kafka -> Flink -> RocksDB state -> durable checkpoints; prints PASS
make ingest        # start live ingestion from Wikimedia into Kafka (opt-in; ~0.8 GB/day)
make ingest-logs   # watch the producer's structured logs; `make ingest-stop` stops it gracefully
make test-producer # producer lint + unit + integration tests (needs Python + uv)
make help          # all commands
```

- Flink UI: http://localhost:8081
- Kafka from the host: `localhost:9094`
- Producer metrics (while ingesting): http://localhost:8000/metrics
- PostgreSQL: `localhost:5432` (credentials in `.env`, created from `.env.example` on first `make up`)

<details>
<summary>Without make (e.g. PowerShell)</summary>

| make | Equivalent |
|---|---|
| `make up` | copy `.env.example` to `.env`, then `docker compose up -d` and wait until `docker compose ps` shows the long-running services healthy (`scripts/wait_for_stack.sh` does this in bash) |
| `make build-jobs` | `docker compose --profile build run --rm flink-jobs-build` |
| `make smoke-test` | `bash scripts/smoke_test.sh` (needs bash: WSL or Git Bash) |
| `make ingest` / `ingest-stop` | `docker compose --profile live up -d --build producer` / `docker compose --profile live stop producer` |
| `make down` | `docker compose down` (data is kept) |
| `make clean CONFIRM=yes` | `docker compose down -v` (**deletes** all data volumes) |

</details>

## What exists today
- **Local platform** ([ARCHITECTURE §3.1](docs/ARCHITECTURE.md#31-local-platform)):
  - single-node KRaft Kafka 4.2.2 with the three topics created and their configs enforced;
  - PostgreSQL 18.6 with Flyway migrations;
  - a Flink 2.2.1 session cluster with RocksDB and checkpoints on a persistent volume.

  Health-gated startup; everything pinned.
- **Producer** ([ARCHITECTURE §5](docs/ARCHITECTURE.md#5-producer)): live, replay and record modes through one publishing path.
  - Kafka settings: idempotent, `acks=all`, zstd, Java-compatible partitioning (verified against Kafka's own murmur2 test vectors).
  - Resumes from the position stored in Kafka, with guards added after a [real incident](docs/adr/0012-producer-resume-from-kafka.md) in which Wikimedia silently ignored a malformed `since` and streamed 8-day-old data.
  - 64 tests; CI on every push.
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
