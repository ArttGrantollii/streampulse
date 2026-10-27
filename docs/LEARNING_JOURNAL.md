# StreamPulse — Learning journal

One entry per completed milestone, written so that I can reread it months later and understand both *what* was built and *why*.

---

## Phase 0 — Research & architecture

**Date:** 2026-10-26

### Milestone
Phase 0: research, assumption validation, target architecture, ADRs and a phased plan. No application code.

### What we built
- A reproducible **source investigation**: two small scripts capture the live Wikimedia stream and analyse it (`scripts/research/`). The findings are written up in [Research 0001](research/0001-eventstreams-source-validation.md).
- A **verified version baseline** ([COMPATIBILITY.md](COMPATIBILITY.md)), checked against the Flink downloads page, Maven Central POMs, Docker Hub tags and vendor release policies.
- **Thirteen ADRs** covering source, language, versions, topics, contracts, event time, dedup, sink semantics, state and checkpoints, spike detection, database choice, producer resume and the public showcase.
- The **architecture document**, a **roadmap** with acceptance criteria, an interview question bank, and this journal.

### Concepts learned
- **Server-Sent Events.** A long-lived HTTP response in which each event has `id:` and `data:` lines and ends with a blank line. The `id` is how the client resumes (`Last-Event-ID`). For Wikimedia it is a JSON array of Kafka positions (timestamps) for each datacenter topic.
- **A schema is a contract, not a description of the data.** Wikimedia's schema makes almost every field optional, and the data contains fields that the schema doesn't. A robust consumer depends only on what it validates, ignores extra fields, and has a defined path (the DLQ) for violations.
- **Event time vs emission time vs processing time.** We found three clocks:
  - when the edit was saved (`timestamp`);
  - when Wikimedia emitted the event (`meta.dt`, 0.03–50 s later);
  - when we received it.

  Choosing the right one is a semantic decision, not a technical one.
- **Out-of-orderness is measurable.** "How late can data be?" has an empirical answer: the distribution of how far each event is behind the newest one seen so far. That distribution drives the watermark bound and the allowed lateness.
- **Clock skew is real, even on one laptop.** Negative delays exposed a WSL2 clock running about 3–4 s slow. Any latency that compares two machines' clocks carries that error.
- **Compatibility is a graph.** The connector suffix (`5.0.0-2.2`) ties a connector to a Flink minor version, and the connector bundles a Kafka client version. The newest Flink was unusable because its connectors didn't exist yet.
- **Guarantees are per boundary.** "Exactly-once" in Flink means state consistency. External effects need transactions or idempotence. Saying exactly what holds at each hop is more credible than one big claim.

### Why we did it this way
- **Data before design.** Two minutes of real data overturned four assumptions in the specification:
  1. it is Wikipedia-only (false);
  2. pages have IDs (false in this stream);
  3. "event datetime" is the edit time (it isn't);
  4. all events are edits (more than half aren't).

  Designing first would have built all four mistakes in.
- **Fewer moving parts.** We removed the clean topic, analytics topics, Schema Registry and MinIO, each with a written trigger for when to add it. Complexity has to earn its place.
- **Parameters are labelled as assumptions.** The watermark bound, lateness, dedup horizon and spike thresholds all wait for a 24-hour measurement instead of being picked because they look reasonable.

### Problems encountered
1. **Fetching the schema through the web tool failed** with no output. *Solution:* downloaded the YAML directly with `curl` from schema.wikimedia.org, which also listed the available versions (1.0.0 and 1.0.1).
2. **Negative "receive − meta.dt" latencies** in the first analysis. That looked like a bug in the script. *Diagnosis:* compared the local clock with HTTP `Date` headers from two servers and found the local clock ~3–4 s behind. *Result:* a documented clock-domain design for latency, and a Phase 2 task to measure the offset properly (R-4).
3. **The JDBC connector documentation disagreed with the code.** The 4.1.0 docs show the legacy `addSink(JdbcSink.exactlyOnceSink(...))` API, which Flink 2.x removed. *Solution:* downloaded the 4.1.0 jar from Maven Central and inspected it with `javap`. It exposes `JdbcSink.builder()…buildAtLeastOnce()/buildExactlyOnce()` (Sink V2). Lesson: when docs and artifacts disagree, the artifact wins.
4. **Strange `page_change` timestamps from 2010.** At first they looked like corrupt data. *Diagnosis:* in every case `dt == revision.rev_dt`, so they are legitimate historical revision times. This informed both the source choice and the rule that validation bounds must follow field semantics.
5. **Environment gaps found for Phase 1.** Docker Desktop's WSL integration is not enabled for this distro (`docker` is not found inside WSL), and the GitHub CLI isn't installed. Neither is solved yet; both are listed as Phase 1 prerequisites.

### How we verified the work
- `python3 scripts/research/sample_eventstreams.py recentchange 120 rc.ndjson`, then `python3 scripts/research/analyze_sample.py rc.ndjson`. The committed script reproduced every number in Research 0001 from the recording.
- Version facts were checked at the source: the Flink downloads page, connector POMs on Maven Central (e.g. `kafka.version=4.2.0` in the Kafka connector parent POM), Docker Hub tag listings, PostgreSQL release notes (18.6: 2026-08-13), the Prometheus LTS table (3.13 until 2027-07-31), and the Flink 2.2 configuration reference for every `execution.checkpointing.*` key used.

### Production perspective
- A real Wikimedia-internal team would consume WMF Kafka directly, not public SSE.
- Watermark parameters would come from weeks of data, and per-wiki behaviour might justify per-source bounds.
- Version choices would go through an internal platform matrix and staged upgrades (savepoint, then upgrade, then restore) on a schedule.
- Clocks would be NTP-disciplined everywhere, with clock offset monitored as a metric.

### Key takeaways
1. Look at the data before designing. Documentation describes intent; data describes reality.
2. Every guarantee belongs to a specific boundary. Write them down one hop at a time.
3. Don't pick the newest version; pick the newest *compatible* one.
4. Unmeasured parameters must be labelled as assumptions, with a plan to measure them.
5. Every component and topic needs a reason to exist, and a written trigger for anything deferred.

---

## Phase 0 review — Product and scope revision

**Date:** 2026-10-26

### Milestone
A critical review of the Phase 0 plan before any code was written.

### What we changed
- **Product framing.** "Wikipedia stream → Kafka → dashboard" is one of the most common streaming tutorials, and its basic metrics (edits/min, bot vs human) are not interesting on their own. The project now has a headline question: *can Wikipedia editing detect breaking news, and how fast?* The answer gets validated against an independent signal.
- **Scope.** Cut from 12 phases to 6. Observability, CI and benchmarks are folded into the phases that need them rather than standing alone. The deeper experiments (full backpressure study, savepoint upgrade, rescaling, cloud mapping) became stretch goals.
- **Audience.** Portfolio visitors will never run Docker, so the public deliverable is a static site ([ADR-0013](adr/0013-public-showcase.md)). It has a live in-browser view (Wikimedia allows cross-origin browser connections: `access-control-allow-origin: *`), results recorded from real runs, and the crash-test proof. The full stack stays runnable locally for engineers.

### Why
- A large unfinished project is worth less than a smaller finished one.
- The differentiator is depth: measured parameters, proven recovery and a validated detector. The choice of data source matters much less.
- A recruiter spends minutes, not hours. The experience must work instantly on any device.

### Problems encountered
- Docker Desktop's WSL integration was enabled, but the shell still couldn't reach the socket (`permission denied … /var/run/docker.sock`). The user is listed in the `docker` group, but the running WSL session predates that membership. Fix: restart WSL (`wsl --shutdown` from Windows) and verify with `docker version`.

### Key takeaways
- Review the plan honestly before building: ask what a reviewer will actually see and what makes it different.
- Separate the system you engineer from the experience you publish.
