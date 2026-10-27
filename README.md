# StreamPulse

Real-time analytics over Wikimedia's worldwide live change stream, built with Kafka, Flink (Java), PostgreSQL, Prometheus and Grafana. The project is designed to show *how* streaming correctness is reasoned about:
- event time with measured lateness bounds;
- stateful deduplication;
- per-boundary delivery guarantees;
- crash recovery verified by an automated test.

> **Status: Phase 0 complete (research & architecture).** No runnable platform yet. See the [roadmap](docs/ROADMAP.md).

## What exists today
- **Source validation from live data:** [Research 0001](docs/research/0001-eventstreams-source-validation.md). A 2-minute capture overturned four assumptions: the stream covers all Wikimedia projects (not just Wikipedia), `recentchange` has no page IDs, `meta.dt` is emission time rather than edit time, and most events are not edits.
- **A verified version baseline:** [COMPATIBILITY.md](docs/COMPATIBILITY.md). Flink 2.2.1 rather than 2.3.0, because no connectors support 2.3 yet; Kafka 4.2.2 to match the connector's client line.
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
