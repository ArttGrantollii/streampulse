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

---

## Phase 1 — Infrastructure foundation

**Date:** 2026-10-27

### Milestone
Phase 1: the local platform. Kafka, Flink and PostgreSQL start with one command, provision themselves, and are verified end to end by an automated smoke test.

### What we built
- **`docker-compose.yml`** with four long-running services, each health-checked:
  - Kafka 4.2.2 in KRaft combined mode;
  - PostgreSQL 18.6;
  - a Flink 2.2.1 JobManager;
  - a Flink 2.2.1 TaskManager.
- **Three one-shot init services:**
  - `kafka-init` creates topics and enforces configs;
  - `db-migrate` runs Flyway migrations;
  - `flink-data-init` prepares the checkpoint volume.
- **A containerised Maven build** (`flink-jobs-build`), so no JDK or Maven is needed on the host.
- **Flink cluster configuration** kept in the repository (`infra/flink/flink-properties.yaml`) and loaded through the image's own `FLINK_PROPERTIES` mechanism.
- **A Java smoke job**, `SmokeJob` plus `CountRecords`: it reads a Kafka topic, counts records in RocksDB keyed state, and checkpoints. Two operator-harness unit tests cover it, including snapshot and restore.
- **Scripts:**
  - `scripts/smoke_test.sh` produces 500 records, checks the job counts exactly 500, waits for checkpoints, checks the checkpoint files and committed offsets, and checks that the checkpoint is retained after cancel. It prints PASS or FAIL.
  - `scripts/wait_for_stack.sh` decides when the stack is ready.
- **A `Makefile`**, with each target a thin wrapper around a documented Compose command.

### Concepts learned
- **KRaft:** Kafka now stores its own metadata in a Raft-replicated log on controller nodes, replacing ZooKeeper. Combined mode (one node as both broker and controller) is fine locally and fragile in production.
- **Advertised listeners:** clients bootstrap from one address, then use whatever addresses the broker *advertises* in metadata. Containers and the host need different ones (`kafka:9092` vs `localhost:9094`).
- **Health checks vs readiness:** "the process started" isn't "ready". The TaskManager is healthy only once it has registered with the JobManager.
- **Idempotent provisioning:** init steps that can be re-run safely. Running them twice changed nothing, which I verified.
- **Flink offsets live in checkpoints.** Committed consumer-group offsets only reached the end of the topic *after* a checkpoint completed. They exist for monitoring, not for recovery.
- **Checkpoint anatomy:** `chk-N/_metadata` (written by the JobManager), `shared/` (incremental SST files referenced by several checkpoints) and `taskowned/`. The JobManager and TaskManagers must see the same path.
- **Keyed-state recovery in miniature:** the harness test snapshots state, restores it into a brand-new operator, and counting continues from 2 to 3.
- **Provided vs shaded dependencies:** the cluster supplies Flink and SLF4J; the job jar must carry connectors.
- **Transactional DDL:** PostgreSQL rolled back a broken migration completely.

### Why we did it this way
- **Health and completion conditions instead of sleeps**, so the startup order expresses real requirements rather than timing guesses.
- **Configuration file plus the image's own mechanism**, instead of mounting `config.yaml`. The entrypoint writes to the config file at startup, so a mounted repository file would be modified or fail as read-only.
- **The smoke test verifies the riskiest compatibility assumption first** (connector 5.0.0-2.2 + broker 4.2.2 + Flink 2.2.1), before any business logic depends on it.
- **The test stack is aligned with Flink's own** (JUnit 5.11.4, AssertJ 3.27.3) rather than the newest versions, to avoid classpath conflicts with `flink-test-utils`.

### Problems encountered
1. **First build failed: `package org.slf4j does not exist`.** Flink 2.2's POMs don't pass `slf4j-api` through to job projects.
2. **`docker compose up --wait` was non-deterministic.** It returned exit code 0 on one run and 1 on the next ("container db-migrate exited (0)"), and once returned before the TaskManager was healthy. It treats successfully exited init containers as failures.
3. **The smoke test reported the wrong checkpoint.** It printed `chk-1` while `chk-2` existed. My `grep … | tail -1` took the last `external_path` in the REST response, which is the *oldest* history entry. The check passed, but it would have broken once retention deleted old checkpoints. This is a test that passed for the wrong reason.
4. **Noise: "WARNING: Unknown module: jdk.compiler specified to --add-exports"** on every `flink` CLI call. The image's default JVM flags export a module that its own JRE doesn't contain.
5. **`make` is not installed** in the WSL distro.
6. **A clean-room run (fresh volumes, no build output) failed to build.**
   - The JobManager bind-mounted `flink-jobs/target`. Because it didn't exist yet, Docker created it **owned by root**, and the Maven container (running as my uid) then failed with "could not create parent directories".
   - Earlier runs had hidden this because `target/` already existed.
   - `make up` would also have hidden it (the Makefile created the directory first), but a plain `docker compose up`, which is the PowerShell path, would not.
7. **Gotchas caught by reading the images before they caused damage:**
   - The PostgreSQL 18 image moved its data directory (`/var/lib/postgresql/18/docker`); mounting the familiar `/var/lib/postgresql/data` would have left the real data in an anonymous volume.
   - A new named volume is owned by root, but Flink runs as uid 9999.
   - The image's `FLINK_PROPERTIES` parser strips all whitespace and doesn't understand comments.
   - Setting `MAVEN_CONFIG` would inject its value into Maven's command line. Confirmed in the launcher source: `MAVEN_CMD_LINE_ARGS="$MAVEN_CONFIG $@"`.

### How we solved them
1. Declared `slf4j-api` 1.7.36 as `provided`, matching the version in the flink-parent POM. The runtime supplies it from `lib/`.
2. Wrote `scripts/wait_for_stack.sh`, which waits for long-running services to be `healthy` and one-shot services to have exited 0. If an init service fails, it prints that service's logs. I tested the failure path with a deliberately broken migration: it exited 1 and printed Flyway's syntax error.
3. Inspected the REST JSON (key order: `counts`, `summary`, `latest`, `history`) and took the *first* `external_path`, which is `latest.completed`. A re-run reported `chk-2` correctly.
4. Filtered only that exact warning from the CLI's stderr; all other errors still show.
5. Documented `sudo apt install make` as a prerequisite, plus a no-make table in the README. I ran every target's underlying command directly to verify it.
6. The shade plugin now writes the jar to `flink-jobs/dist/`, a directory committed with a `.gitkeep`. It always exists and is owned by whoever cloned the repo, and `mvn clean` never removes it. The clean-room run then passed.
7. For the gotchas:
   - mounted the PostgreSQL volume at `/var/lib/postgresql`;
   - added the `flink-data-init` container, which chowns the volume;
   - added a wrapper entrypoint that strips comments, and kept values free of spaces;
   - left `MAVEN_CONFIG` unset and passed `-Dmaven.repo.local` instead.

### How we verified the work
- `docker compose --profile build run --rm flink-jobs-build`: **2 tests, 0 failures**, BUILD SUCCESS. The 24 MB shaded jar contains kafka-clients 4.2.0.
- `docker compose up -d && ./scripts/wait_for_stack.sh` from fresh containers: **ready in about 32 s** (one measurement).
- `REST /jobmanager/config` shows every repository setting live (RocksDB, the checkpoint dir, `EXACTLY_ONCE`, `RETAIN_ON_CANCELLATION`, `exponential-delay`, 2 slots).
- `./scripts/smoke_test.sh`: **PASS**. 500/500 records counted, checkpoints `chk-1` and `chk-2` completed (5 s interval override), committed offsets = 500, checkpoint retained after cancel. Run time about 44 s.
- **Idempotency:** a second `up` re-ran all init services; no topics were created, and Flyway reported "Schema is up to date".
- **Durability:** after `docker compose down` and `up` (containers destroyed), Kafka still held 500 smoke records, `flyway_schema_history` showed v001, the database timezone was UTC, and all retained checkpoints still had `_metadata`.
- **Clean room:** `docker compose down -v`, delete all build output, then start, build and smoke-test with no manual steps: **PASS in about 1 min 24 s**.
- **Topic configs as stored by the broker:** raw has 3 partitions, RF 1, `retention.ms=604800000`, `retention.bytes=2147483648` and `compression.type=producer`.

### Production perspective
- Kafka would be three or more brokers with RF 3 and `min.insync.replicas=2`, plus a dedicated controller quorum. TLS and SASL with ACLs would replace PLAINTEXT.
- Flink would run on Kubernetes with the Flink Operator, JobManager HA and checkpoints in object storage.
- Secrets would come from a secret manager, not `.env`.
- Startup ordering would be handled by readiness probes and operators, and provisioning (topics, migrations) would run in CI/CD with review, not on every `up`.

### Key takeaways
1. Verify the riskiest assumption, here version compatibility, with the smallest possible end-to-end test before building on it.
2. Read the images you depend on (entrypoints, users, paths). Most of this phase's traps were visible there before they caused damage.
3. A test that passes can still be wrong. Check *what* it verified, not just that it said PASS. Run from a clean room, because state left over from earlier runs hides bugs.
4. Prefer explicit readiness rules over tools whose semantics don't fit (`--wait` with init containers).
5. Idempotency and durability are claims, and both were tested: re-run everything, destroy the containers, check again.

---

## Phase 2 — Ingestion

**Date:** 2026-10-28

### Milestone
Phase 2: the Python producer that makes Wikimedia's live stream durable in Kafka, plus record and replay modes, CI, and the start of the 24-hour source measurement. The 24-hour results are added to Research 0002 when the run completes.

### What we built
- **`producer/`**, a Python 3.14 package (uv-locked) with one module per responsibility:
  - the SSE parser;
  - the live source (reconnect, backoff, resume, `since` guard);
  - the replay source (speed, rate or unpaced);
  - the Kafka publisher (key, headers, delivery tracking, backpressure);
  - resume from Kafka;
  - an SNTP clock-offset probe;
  - Prometheus metrics and structured JSON logs.
- **A Docker image and an opt-in Compose service** (`make ingest`), with health check and graceful stop.
- **74 tests:**
  - 65 unit tests, including a scripted fake upstream for reconnects, mid-stream drops, HTTP errors and resume;
  - 9 integration tests against the real broker: byte-for-byte replay, Java-compatible partitioning, and resume from real records.
- **GitHub Actions CI:**
  - producer lint and unit tests;
  - producer integration tests against the Compose Kafka;
  - Flink build and tests;
  - Compose validation and ShellCheck.
- **`scripts/research/analyze_raw_topic.py`**, which measures R-1 to R-6 straight from the raw topic.
- **A committed 120-event fixture** with attribution.

### Concepts learned
- **At-least-once by construction:**
  - Resume positions are timestamps, so a resume re-delivers events that share the boundary millisecond.
  - The restart margin deliberately re-reads 30 s.
  - Duplicates are the price of never having a gap, and dedup downstream is the plan.
- **Idempotent producer scope:** it removes retry duplicates *within one producer session*, not duplicates that come from re-reading the source.
- **Backpressure in a producer:** a full local queue blocks `produce()`, which stops reading upstream, instead of dropping data.
- **Graceful shutdown:** SIGTERM sets a flag, the loop stops, in-flight messages are flushed, and the exit code says whether delivery succeeded.
- **Partitioner compatibility across clients:** librdkafka uses CRC32 by default and Java uses murmur2. Same key, different partition, unless configured.
- **Disorder must be measured where watermarks are generated** (per Kafka partition) and for the events that matter (edits).
- **Steady state vs catch-up:** after a restart, event age is backlog, not latency. The analysis separates them.

### Why we did it this way
- **Kafka as the producer's checkpoint:** the position can't disagree with what was actually written.
- **Our own SSE parser:** small, spec-conformant, and fully testable. We need exact `id` semantics for resume.
- **Integration tests against the real Compose broker instead of Testcontainers:** one fewer framework, and the real pinned configuration.
- **Live ingestion is opt-in:** `make up` shouldn't contact a third party or fill the disk without being asked.

### Problems encountered
1. **Restart resumed 8 days in the past.** The first restart test ingested about 85,000 events from 8 days earlier in under a minute. `since` was sent as epoch milliseconds, and Wikimedia silently ignored it and started from its oldest retained event. My unit test had asserted the epoch format, so it encoded the bug rather than catching it.
2. **A shell pipeline swallowed the evidence.** The first restart attempt, piped through a log formatter, printed nothing and reported exit 143. The run had actually happened and written 66,731 records.
3. **The integration test compared the wrong records.** It matched records by receive millisecond, but 8 fixture events share a millisecond, and 3 even share an SSE id.
4. **An arg-parsing hack:** I first detected `--rate` by searching `sys.argv`, because `--rate max` and no rate both became `None`.
5. **ShellCheck SC2145:** `"/opt/kafka/bin/"$@"` in the smoke test only worked by accident.
6. **A diagnostic that could only ever say 0:** "duplicates within a run" was computed from a dict, whose keys are unique by construction. I discarded that number rather than reporting it.
7. **Analysis skewed by a catch-up.** Receive − `meta.dt` came out at a median of 238 s because a restart's backlog was included.
8. **The scratchpad recording from Phase 0 was gone after a reboot,** so the fixture had to be re-captured.
9. **The integration tests failed in CI but passed locally,** even against a fresh broker. The job logs need authentication I didn't have.
10. **CI then showed a real startup race:** `_DESTROY_BROKER` ("broker handle destroyed without termination") while reading the resume position.
11. **Reconnect metric mislabelled.** Wikimedia's disconnects were counted as `network_error`. The logs also showed that **no connection reached the documented 15 minutes, and none closed cleanly**: streams broke mid-body after 144–802 s.
12. **An internet outage of about 8 minutes** during the run, which was unplanned.

### How we solved them
1. Probed Wikimedia directly. ISO was honoured to within 0.0 s; epoch ms was off by −712,330 s, the start of retention. Fixed the format, added a check that the first event matches the requested `since` (else exit 1) and a 24 h maximum resume age (else exit 2), replaced the wrong test, and truncated the polluted records with `kafka-delete-records` before the 24-hour run. Documented as an incident in ADR-0012.
2. Re-ran with output written to a file, then grouped the raw topic by `sp-connection` to reconstruct what each run had done. That's the header earning its keep.
3. Matched on payloads, which are unique. The finding (SSE ids aren't unique) went into Research 0002 §1.3.
4. Kept the raw `--rate` string and interpreted it explicitly (`_replay_pacing`).
5. Rewrote it as `kafka <tool> [args]`, and ShellCheck now runs in CI.
6. Recorded the defect; within-connection duplicates are measured by the analysis tool instead.
7. The tool now treats each connection as catching up until its events are fresh (< 10 s), and measures steady-state delay after that: p50 256 ms, p99 5.4 s.
8. Captured 20 s of fresh data with the committed research script. The fixture is now committed, so it can't be lost again.
9. Made failures readable without logs: CI now turns pytest's failure summary into `::error` annotations, which are public through the checks API. The test consumer also switched from `subscribe()` to `assign()`, removing a dependence on consumer-group coordination.
10. The annotation showed the cause. librdkafka retires its bootstrap connection once it learns the broker's advertised address, and a request in flight on it fails. That is production code, not just the test: the producer could crash at startup on a cold broker. Added `with_retries` for transient Kafka errors (5 attempts, exponential backoff, anything else re-raised), with unit tests. CI went green.
11. Replaced the single label with causes that say what happened (`stream_interrupted`, `connect_error`, `timeout`, `server_closed`, `network_error`), checking httpx's exception hierarchy first, since `ConnectTimeout` is a `TimeoutException`. Added a `connection_lifetime_seconds` histogram, so the 24-hour run measures lifetimes instead of us assuming 15 minutes. Added a test that reproduces the mid-stream drop.
12. No fix was needed. The producer backed off with jittered waits (0.3–57 s, capped at 60 s), then resumed with `Last-Event-ID` and caught up 20,423 events in 84 s, with no gap and no delivery errors. That's a free real-world test of the reconnect design.

### How we verified the work
- `uv run pytest`: **74 passed** (65 unit, 9 integration against Kafka 4.2.2). **CI green** on all four jobs. `ruff check` and `ruff format --check` are clean. ShellCheck is clean.
- **Live, against Wikimedia:**
  - SIGTERM after 40 s: exit 0, all 1,598 events in Kafka.
  - Restart: resumed from an ISO position, overlap exactly 30.0 s, 949 duplicates, no gap.
  - Container `docker stop`: exit 0 in 0.5 s.
  - Container start after 8.5 min of downtime: caught up about 14,000 events within seconds.
- **Partitioning:** every replayed record sits on the partition Java's murmur2 would choose, and our murmur2 matches Kafka 4.2.2's six reference values.
- **Dependency cutoff:** `exclude-newer` resolved ruff 0.17.0, not the newer 0.17.1, and confluent-kafka 2.16.0, not the newer 2.16.1.
- The smoke test still passes with live ingestion running alongside.

### Production perspective
- WMF-internal consumers read Kafka directly, so the SSE layer and its 15-minute reconnects wouldn't exist.
- A single active producer would need leader election, or the redundancy of two producers plus dedup.
- Alerts would fire on `reconnects{cause!="server_closed"}`, on `produce_errors`, on a growing `last_event_emitted` age, and on clock offset.
- Secrets aren't needed for a public SSE source, but a production client would identify itself through a registered User-Agent, and possibly an API key.

### Key takeaways
1. A request that "succeeds" isn't proof of the effect. Verify outcomes at boundaries you don't control, and cap the blast radius.
2. A test that asserts what your code does, rather than what the other side requires, can encode the bug.
3. Make the system of record cleanable, and clean it before measuring.
4. Every header you add for observability (here `sp-connection`) pays off the first time something goes wrong.
5. Measure where the mechanism operates: watermarks are per partition, and analytics are about edits.
6. "Works on my machine" includes timing. A cold CI runner found a race a warm laptop never hit, and making failures visible (annotations) was the fastest way to find it.
7. Documented limits aren't typical behaviour. "At most 15 minutes" turned out to be "every few minutes" in practice, so measure the distribution.
