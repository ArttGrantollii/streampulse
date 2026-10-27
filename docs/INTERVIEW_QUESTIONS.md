# StreamPulse — Interview question bank

A growing, project-specific bank of questions with model answers, organised by phase.

**Honesty rule:** answers describe what StreamPulse *actually* does at the stage stated. Phase 0 answers describe **design decisions and research evidence**, because nothing is implemented yet. When the implementation changes, answers are revised, never left stale.

Difficulty: **Fundamental** (do you understand the concept?) · **Intermediate** (can you explain how and why you did it?) · **Advanced/Senior** (trade-offs, failure modes, production implications).

## Contents
- [Phase 0 — Research & architecture](#phase-0--research--architecture)
  - [Source and data understanding](#source-and-data-understanding)
  - [Technology choices](#technology-choices)
  - [Kafka design](#kafka-design)
  - [Event time and watermarks](#event-time-and-watermarks)
  - [Deduplication and state](#deduplication-and-state)
  - [Delivery semantics](#delivery-semantics)
  - [Observability](#observability)
  - [Product and presentation](#product-and-presentation)
- [Senior-level system design scenarios](#senior-level-system-design-scenarios)

---

## Phase 0 — Research & architecture

### Source and data understanding

**Question:** What does the Wikimedia EventStreams `recentchange` stream actually contain, and what did you learn from looking at real data rather than the docs?

**Difficulty:** Fundamental / Intermediate

**Interview-ready answer:**
It's an SSE stream of every change across all Wikimedia projects, not just Wikipedia. I captured two minutes of it before designing anything:
- about 47 events per second;
- more than half were `categorize` events, which aren't edits at all;
- only about a third of the edits were on Wikipedia, with Commons and Wikidata dominating;
- around half the edits were bot-flagged.

So I had to define "edit" explicitly as `type` in `edit` or `new`, and add a project-family dimension. Otherwise the headline metric would have been more than double what users expect.

**Deep technical explanation:**
The published schema requires only `$schema`, `meta.dt` and `meta.stream`; everything else is optional. Real events drift from the schema: they declare version 1.0.0 while the registry is at 1.0.1, and they carry five undocumented fields (`title_url`, `notify_url`, `meta.topic/partition/offset`). The schema's descriptions of `revision.old`/`new` also appear swapped, so I verified the meaning against data (`revision.new > revision.old` in 2,086 of 2,086 edits). The lesson is to treat documentation as a hypothesis and data as the test.

**How StreamPulse demonstrates this:**
[Research 0001](research/0001-eventstreams-source-validation.md) records the method and results. The capture and analysis scripts are in [`scripts/research/`](../scripts/research/), so the numbers are reproducible. The definition of "edit" is in [ADR-0001](adr/0001-source-stream-selection.md).

**Potential follow-up question:**
Two minutes is a tiny sample. How do you know your conclusions hold?

**Follow-up answer:**
I don't, yet, and the docs say so. Anything that depends on tails, like watermark bounds or the dedup horizon, is marked as an ASSUMPTION. Phase 2 runs a 24-hour capture covering peak and quiet hours before those parameters are fixed. The structural findings (no `page_id`, the type mix, schema drift) don't need a large sample; they only need one counter-example.

---

**Question:** The spec said "page identifiers". Why doesn't StreamPulse use `page_id`?

**Difficulty:** Intermediate

**Interview-ready answer:**
Because `recentchange` doesn't have one. I checked the published schema and the live data. The newer `mediawiki.page_change.v1` stream has `page_id`, but it changes the bot semantics: the performer's group membership instead of the edit's bot flag. It also contained about 4% of events with `dt` values from years ago, and its events are 2.4 times larger. For V1, I identify pages by `(wiki, title)` and document that a page move splits its history.

**Deep technical explanation:**
This is a classic identity trade-off: natural keys (title) are human-readable but mutable, while surrogate keys (page_id) are stable but need a different source. The ancient `dt` values in `page_change` are interesting. They equal the revision's `rev_dt`, so they're legitimate historical timestamps, not corruption. A naive "reject timestamps older than X" rule would have sent valid events to the DLQ. Validation rules must be grounded in what the field *means*.

**How StreamPulse demonstrates this:**
[ADR-0001](adr/0001-source-stream-selection.md) and Research 0001 §6 contain the side-by-side comparison. The `page_change` join is a stretch goal with an explicit trigger in the [roadmap](ROADMAP.md).

**Potential follow-up question:**
How would you add stable page IDs later without rewriting the pipeline?

**Follow-up answer:**
Ingest `page_change` into its own raw topic and enrich `recentchange` events by `(wiki, rev_id)` with a keyed join in Flink. Note that the key must include the wiki, because revision IDs are per wiki. Because analytics only see the internal `EditEvent` contract, the change is confined to the parser and an enrichment operator. The hard part is the join's state and timing: how long to wait for the matching event, and what to do when one side never arrives.

---

### Technology choices

**Question:** Why did you pick Flink 2.2.1 when 2.3.0 is the latest release?

**Difficulty:** Intermediate

**Interview-ready answer:**
Because no released Kafka or JDBC connector supports 2.3 yet. Flink connectors are versioned per Flink minor. The Kafka connector 5.0.0 artifacts are `5.0.0-2.1` and `5.0.0-2.2`, and the JDBC connector 4.1.0 is the same. The newest Flink that both connectors officially support is 2.2.x, so 2.2.1 it is. "Latest" isn't a compatibility strategy.

**Deep technical explanation:**
In a streaming stack, the compatibility graph matters more than any single version:
- connector ↔ Flink minor;
- Kafka client ↔ broker (the Flink connector bundles kafka-clients 4.2.0, so I matched the broker to the 4.2 line);
- runtime JVM ↔ Flink (17 is the recommended version, and 21 is experimental).

I also applied a "no release younger than about two weeks" rule. Fresh patch releases sometimes get re-released, and pinning exact tags makes the stack reproducible.

**How StreamPulse demonstrates this:**
[COMPATIBILITY.md](COMPATIBILITY.md) and [ADR-0003](adr/0003-version-baseline.md). I verified versions from the Flink downloads page, Maven Central POMs and Docker Hub tags.

**Potential follow-up question:**
How will you upgrade to 2.3 when connectors arrive, without losing state?

**Follow-up answer:**
I'd take a savepoint with `stop-with-savepoint`, bump Flink and the connector versions together, and restore. That only works if every stateful operator has a stable `uid()` and the state serializers are compatible. I'd also rehearse it in Phase 5, because a savepoint you've never restored is a hope, not a backup.

---

**Question:** Why write the Flink jobs in Java rather than PyFlink, given you're stronger in Python?

**Difficulty:** Intermediate / Advanced

**Interview-ready answer:**
Three concrete reasons from the docs:
1. The JDBC exactly-once sink is "still not supported in Python API".
2. Several operators, such as async I/O and window or interval joins, aren't supported in PyFlink at all.
3. In PyFlink's default PROCESS mode, every record crosses a JVM-to-Python boundary over gRPC.

I want to demonstrate stateful streaming internals: serializers, state backends and operator testing. Those live on the JVM side. I kept Python where it fits best, the I/O-bound producer.

**Deep technical explanation:**
PyFlink's THREAD mode embeds Python in the JVM (PEMJA) to avoid serialization, but it's still bound by the GIL and silently falls back to PROCESS mode where unsupported. PyFlink is good for Table API and ML-oriented pipelines. For a job whose value lies in custom keyed state, timers and precise sink semantics, Java gives first-class APIs, operator test harnesses and one runtime to debug.

**How StreamPulse demonstrates this:**
[ADR-0002](adr/0002-flink-java-over-pyflink.md) includes a capability table.

**Potential follow-up question:**
When would PyFlink be the right choice?

**Follow-up answer:**
When the team is Python-only and the logic is mostly SQL or Table API (windows, joins, aggregations), or when the job calls Python ML libraries for inference. In those cases productivity beats raw control, and the JVM boundary cost is small next to the model's cost.

---

**Question:** Why PostgreSQL and not ClickHouse for the serving layer?

**Difficulty:** Intermediate

**Interview-ready answer:**
Because I serve pre-aggregated windowed results, not raw events. That's thousands of rows per hour, queried by recent time range. PostgreSQL gives me real primary keys and ACID upserts, which is exactly what my idempotency story depends on. ClickHouse's `ReplacingMergeTree` dedups only eventually, at merge time or with `FINAL`, which is a weaker contract. I'd switch if we needed ad-hoc queries over billions of raw events.

**Deep technical explanation:**
The choice follows from the access pattern and the correctness mechanism. Columnar engines shine at large scans with high compression. Row stores with B-tree primary keys shine at point and range upserts with strong constraints. Since the Flink job writes absolute values per deterministic key, upsert semantics are the core requirement.

**How StreamPulse demonstrates this:**
[ADR-0011](adr/0011-postgresql-over-clickhouse.md) and [ADR-0008](adr/0008-postgres-sink-semantics.md). The PostgreSQL write limits will be measured in Phase 5 rather than assumed.

**Potential follow-up question:**
What PostgreSQL-specific problems might you hit with constant upserts?

**Follow-up answer:**
Every update creates a new row version (MVCC), so constant upserts mean dead tuples, vacuum pressure and index bloat. Mitigations:
- write fewer, batched upserts;
- emit late corrections only when a value actually changes;
- use a lower fillfactor for HOT updates;
- partition by time so old data is dropped rather than deleted.

I'll measure this rather than guess.

---

### Kafka design

**Question:** Why only three topics, when the original design had seven?

**Difficulty:** Intermediate / Advanced

**Interview-ready answer:**
A topic needs a consumer or a replay requirement to justify itself:
- **Raw** is the immutable system of record, used for replay.
- **DLQ** holds invalid records for debugging.
- **Late** keeps valid-but-too-late events instead of silently dropping them.

A "clean" topic would have no consumer, because validation and analytics run in the same Flink job. Clean data can always be re-derived from raw. Analytics output topics would have no consumer either, because results go to PostgreSQL. I wrote down the trigger for adding each one, such as a second team consuming validated events.

**Deep technical explanation:**
Every extra topic adds a serialization hop, storage, a contract to version, and possibly a transactional boundary to reason about. Adding a clean topic between two jobs would make exactly-once *Kafka* output relevant: transactional sink plus `read_committed` consumers, at a latency cost of one checkpoint interval. That's a real design option ([ADR-0008](adr/0008-postgres-sink-semantics.md), Option C), just not justified yet.

**How StreamPulse demonstrates this:**
[ADR-0004](adr/0004-kafka-topic-topology.md) has the per-topic justification table and the triggers.

**Potential follow-up question:**
Isn't a clean topic good practice in a medallion-style architecture?

**Follow-up answer:**
In a multi-consumer platform, yes: it gives a validated, shared contract. But architecture patterns are answers to problems. With one consumer, it doubles storage and adds latency for no reader. The raw-plus-code combination is still a reproducible "silver" layer, just computed rather than stored.

---

**Question:** How did you choose the partition key and partition count for the raw topic?

**Difficulty:** Intermediate

**Interview-ready answer:**
I key by `wiki|title`. That gives per-page ordering and an even spread, whereas keying by wiki would put about 36% of events (Commons) on one partition. I chose 3 partitions for parallelism and learning, not throughput: one partition could carry 64 KB/s easily. Three lets me run the Flink source at parallelism 1 to 3 and demonstrate per-partition watermarks and idle partitions. One subtle thing: the Python client's default partitioner is CRC32, not Java's murmur2, so I set `murmur2_random` to keep partitioning consistent if a Java client ever writes the same keys.

**Deep technical explanation:**
- Partitions are the unit of ordering and of consumer parallelism.
- You can add partitions later but never remove them, and adding them remaps keys (the hash modulo changes), which breaks per-key ordering across the change.
- Skewed keys create hot partitions, which then become hot subtasks downstream.

**How StreamPulse demonstrates this:**
[ADR-0004](adr/0004-kafka-topic-topology.md). The skew figures come from Research 0001, and the librdkafka default is from its CONFIGURATION.md.

**Potential follow-up question:**
Does Flink actually rely on your Kafka partitioning?

**Follow-up answer:**
Only lightly. Flink `keyBy` re-partitions by its own key groups, so Kafka partitioning doesn't determine Flink's state distribution. Kafka partitioning matters for source parallelism, per-partition ordering (which affects watermark disorder), and balanced source load.

---

### Event time and watermarks

**Question:** Which timestamp did you use as event time, and why not the one the schema calls "event datetime"?

**Difficulty:** Advanced

**Interview-ready answer:**
The schema describes `meta.dt` as the "UTC event datetime", but the data shows it is always 30 ms to 50 seconds *later* than `timestamp`, which is MediaWiki's `rc_timestamp`. So `meta.dt` is when Wikimedia's event pipeline emitted the event, and `timestamp` is when the edit was saved. I use `timestamp`, because "when did people edit" is what the analytics mean. The cost is 1-second resolution and more disorder, up to about 50 seconds versus about 4 seconds for `meta.dt`. That means a larger watermark bound.

**Deep technical explanation:**
Choosing `meta.dt` would give lower latency (a smaller out-of-orderness bound), but it is really "upstream processing time". During a Wikimedia pipeline delay, a burst of edits would be attributed to the minute the delay cleared. That is the same failure mode as processing-time windows, shifted one hop upstream. Because `timestamp` is optional in the schema, I fall back to `meta.dt` and record which source was used.

**How StreamPulse demonstrates this:**
[ADR-0006](adr/0006-event-time-and-watermarks.md) and Research 0001 §5, which has the percentile table.

**Potential follow-up question:**
How will you choose the watermark bound and the allowed lateness?

**Follow-up answer:**
From measured disorder: the bound B is about p99 and the allowed lateness L is about p99.99, from a 24-hour capture. Early results come out after B, the tail corrects them via upserts, and anything beyond L goes to the late topic and a metric. The initial guesses, 10 seconds and 2 minutes, are explicitly labelled as assumptions until then.

---

**Question:** What can make a "deterministic" event-time pipeline non-deterministic?

**Difficulty:** Advanced / Senior

**Interview-ready answer:**
Watermarks are generated periodically on wall-clock time (every 200 ms by default) and combined across partitions by taking the minimum. So whether a borderline event is "late" can depend on how fast each partition was read. Replay at 10× might classify an event differently from replay at 1×. Restoring from a checkpoint also restarts watermark generation, so an event that was late before the crash can be accepted after it. My tests use fixtures where lateness is unambiguous, and Phase 5 explicitly tests the restore case.

**Deep technical explanation:**
The restore case is dangerous with overwrite-upserts. If a previously dropped late event re-enters a window that has already been purged, Flink creates a *new* window with a partial count. When that fires, it could overwrite the complete row. Possible mitigations:
- a per-key "maximum closed window" guard kept in state;
- monotonic merge semantics in the sink;
- watermark alignment during catch-up.

**How StreamPulse demonstrates this:**
The hazards are documented as consequences in [ADR-0006](adr/0006-event-time-and-watermarks.md) and scheduled as Phase 5 experiments. They are not yet tested.

**Potential follow-up question:**
Why not use punctuated watermarks to make it deterministic?

**Follow-up answer:**
A punctuated, per-event watermark in a single-partition test makes outcomes deterministic, and it is a good *test* technique. In production it just moves the problem: the multi-partition minimum still depends on read interleaving. The robust approach is to make the *results* insensitive, by designing the sink and guards so that late re-entries can't regress values.

---

### Deduplication and state

**Question:** Where do duplicates come from in StreamPulse, and why is a 1-hour processing-time TTL the right dedup horizon?

**Difficulty:** Advanced

**Interview-ready answer:**
Duplicates come from:
- the SSE resume overlap on every reconnect, which happens at least every 15 minutes;
- the safety margin when the producer restarts;
- Kafka retries across producer sessions, since idempotence only covers one session.

All of these produce copies close together *in arrival time*. Flink's state TTL is processing-time based, which is normally a limitation, but here it matches the cause. One hour of UUIDs is about 170,000 keys, roughly 17 MB. Deliberate operator replays are a different problem, handled by idempotent sinks rather than dedup.

**Deep technical explanation:**
Dedup state must be bounded, or it grows with total stream history. Options:
- TTL: simple, and processing-time based;
- event-time timers: deterministic under replay, but one timer per key;
- Bloom filters: compact, but false positives drop real events, and they are hard to rescale.

The horizon is a business statement: "we guarantee no duplicates within H". I'll record the age of detected duplicates to check H against reality.

**How StreamPulse demonstrates this:**
[ADR-0007](adr/0007-deduplication.md) and [ADR-0012](adr/0012-producer-resume-from-kafka.md). This is the design; it is implemented in Phase 3.

**Potential follow-up question:**
What if Wikimedia itself emits the same change twice with different `meta.id` values?

**Follow-up answer:**
UUID dedup wouldn't catch it. That's experiment R-3: count repeats of `(wiki, rcid)` under different `meta.id` values. If they exist, I'd add a secondary dedup key for types where rcid is non-null. Note that rcid is unique per wiki, not globally, so the key must include the wiki.

---

### Delivery semantics

**Question:** Your Flink job uses checkpointing. Is StreamPulse exactly-once?

**Difficulty:** Advanced / Senior

**Interview-ready answer:**
Not end-to-end, and I don't claim that. I state guarantees per boundary:
- Wikimedia to producer is at-least-once.
- Producer to Kafka is at-least-once, with no duplicates from retries within a session.
- Kafka to Flink plus checkpoints gives exactly-once *state* consistency.
- Flink to PostgreSQL uses an at-least-once sink with idempotent upserts of absolute values on deterministic keys. Replays rewrite identical rows, so the *result tables converge* to the failure-free values.

The claim I'll make is "results converge after a crash, verified by an automated test", not "exactly-once".

**Deep technical explanation:**
Checkpoints guarantee that state reflects each input once after recovery. But records emitted between the last checkpoint and the crash are emitted again, so external effects need either transactions (two-phase commit: Kafka transactions, or JDBC XA) or idempotence. The upsert approach depends on determinism: no processing time in keys or values, and absolute values rather than increments.

**How StreamPulse demonstrates this:**
[ARCHITECTURE §15](ARCHITECTURE.md#15-delivery-semantics-boundary-by-boundary) and [ADR-0008](adr/0008-postgres-sink-semantics.md). Verified by the Phase 5 crash test, not yet built.

**Potential follow-up question:**
Why not just use the JDBC XA exactly-once sink?

**Follow-up answer:**
I compared it. The connector docs say:
- with PostgreSQL, it needs `max_prepared_transactions > 0`;
- it needs one connection per transaction (`withTransactionPerConnection`);
- it needs `maxRetries = 0`, or duplicates can occur;
- results become visible only at checkpoint completion.

If the job dies and never recovers, orphaned prepared transactions hold locks and block VACUUM. For idempotent aggregate tables it adds cost without adding correctness. I'd use it for non-idempotent effects, such as appends without a natural key.

---

**Question:** Compare the three ways to get correct results into PostgreSQL from Flink.

**Difficulty:** Senior

**Interview-ready answer:**
- **XA two-phase commit:** true exactly-once for the restored lineage, but operationally fragile, with commit latency.
- **At-least-once plus idempotent upserts:** simple, low-latency, and correct if writes are deterministic.
- **Exactly-once Kafka output to a separate materializer:** the materializer writes rows and consumed offsets in one PostgreSQL transaction. It's the platform-grade option when several consumers need the results, but it's the most moving parts.

I chose the idempotent option and documented the trigger for the Kafka option.

**Deep technical explanation:**
Option C's correctness relies on:
- `read_committed` consumers;
- a Kafka `transaction.timeout.ms` greater than the maximum checkpoint duration plus restart time, or transactions abort and data is lost;
- a unique transactional ID prefix per application;
- the materializer committing its offset atomically with its writes (the transactional outbox/inbox pattern).

Option B relies on deterministic computation. Option A relies on the database's XA implementation and on recovery always happening.

**How StreamPulse demonstrates this:**
The comparison table is in [ADR-0008](adr/0008-postgres-sink-semantics.md).

**Potential follow-up question:**
Does any of these protect you if someone restores an old savepoint?

**Follow-up answer:**
Idempotent upserts do: re-processing rewrites the same values. XA doesn't, because restoring an older savepoint re-executes writes that were already committed. With non-idempotent statements, that duplicates effects. This is a strong argument for designing idempotency in, even when transactions are available.

---

### Observability

**Question:** How will you measure end-to-end latency, and what's tricky about it?

**Difficulty:** Advanced

**Interview-ready answer:**
"Latency" hides several definitions, so I separate them:
- **source delay**, from Wikimedia emission to our receipt;
- **pipeline latency**, from our ingest to the sink write, all on one host clock;
- **result freshness**, from row write to window end, which includes the intentional watermark wait.

The tricky part is clocks. My first sample showed *negative* delays, because the local WSL clock was about 3 to 4 seconds behind. Anything comparing our clock with Wikimedia's carries that skew. Another catch: Flink exports histograms as Prometheus summaries, whose quantiles can't be aggregated across subtasks, so for a fleet-wide p99 I'll export bucketed counters instead.

**Deep technical explanation:**
A p99 can't be averaged. Correct aggregation needs histogram buckets, which can be summed and then turned into a quantile. Clock domains matter: containers on Docker Desktop share the VM clock, but that clock differs from WSL's and from Windows'. Measure within a single clock domain where you can, and report the offset where you can't.

**How StreamPulse demonstrates this:**
[ARCHITECTURE §17](ARCHITECTURE.md#17-observability) and the Research 0001 clock-skew finding. Implemented in Phase 5.

**Potential follow-up question:**
Why might Kafka consumer lag look wrong for a Flink job?

**Follow-up answer:**
Flink commits offsets to Kafka only when a checkpoint completes, and doesn't use those commits for recovery. Group lag from an exporter therefore jumps up and down with the checkpoint interval and can be up to one interval stale. Flink's `pendingRecords` metric reflects what the source has actually read, so I show both and explain the difference.

---

### Product and presentation

**Question:** Why isn't the StreamPulse pipeline hosted live? Doesn't a static showcase undermine the "real-time" claim?

**Difficulty:** Intermediate

**Interview-ready answer:**
Hosting Kafka, Flink and PostgreSQL 24/7 just so visitors can look at it would cost money and need operations, and visitors can't interact with Flink anyway. So I separated the two concerns:
- The engineering system runs locally, and anyone can reproduce it with `make up` and `make demo`.
- The public page is static. Its live tab connects the visitor's browser directly to Wikimedia's stream, and its results tab shows real outputs exported from recorded multi-day runs, with the run dates stated.

The real-time claim is backed by the code, the tests and the crash-test evidence, not by a server I keep running.

**Deep technical explanation:**
This is a cost and reliability trade-off. A dashboard frozen during an outage looks worse than a static page. Wikimedia sends `access-control-allow-origin: *`, so the browser can consume SSE without a proxy. The in-browser concept visualisation (watermarks, late events) is labelled as an illustration, not the Flink pipeline. Honesty about what runs where matters more than looking impressive.

**How StreamPulse demonstrates this:**
[ADR-0013](adr/0013-public-showcase.md). Built in Phase 6. Not implemented yet.

**Potential follow-up question:**
If a company asked you to run this in production, what would change?

**Follow-up answer:**
Managed Kafka and Flink (or Kubernetes with the Flink Operator and JobManager HA), checkpoints in object storage, managed PostgreSQL, and alerting on the SLOs. The showcase's static export would become a proper serving API. The pipeline's design doesn't change, only where it runs and who operates it.

---

## Senior-level system design scenarios

*These answers reflect the Phase 0 design. They will deepen as each mechanism is implemented and measured.*

**Question:** What happens if event volume increases 100×?

**Difficulty:** Senior

**Interview-ready answer:**
About 5,000 events per second is still modest for Kafka and Flink. I would:
- add partitions (for example 24) and increase parallelism;
- add TaskManagers;
- tune RocksDB, or consider disaggregated state;
- rely on top-N already being two-phase, to avoid one hot operator.

PostgreSQL load scales with *keys per window*, not raw events, because only aggregates are written. The per-page state for baselines and trending is what grows, so TTL and timers become critical. Then I'd split ingestion and quality from analytics into separate jobs with a `clean` topic.

**Deep technical explanation:**
The likely bottlenecks, in order, are: JSON parsing (CPU), per-page keyed state (RocksDB I/O), checkpoint size and duration, and sink upsert rate. Backpressure would show which one comes first, via busy and backpressured time per operator. Changing Kafka partitions remaps keys, so plan the count upfront. Flink's `maxParallelism` caps rescaling forever, so set it with headroom.

**How StreamPulse demonstrates this:**
Reasoning only in Phase 0. Phase 5 measures the real local ceiling and first bottleneck.

**Potential follow-up question:**
Would you still use Python for the producer?

**Follow-up answer:**
At 5,000 events per second, librdkafka-backed Python is fine, since the heavy lifting is in C. The upstream SSE endpoint is the real limit: WMF says production services should consume their Kafka directly. At that scale I'd replace SSE ingestion with Kafka MirrorMaker or a direct Kafka consumer, rather than optimise the Python process.

---

**Question:** How would you deploy a new Flink job version without losing state?

**Difficulty:** Senior

**Interview-ready answer:**
1. Run `stop-with-savepoint`, which drains and takes a consistent savepoint.
2. Deploy the new jar.
3. Restore from the savepoint.

That only works if every stateful operator has a stable `uid()`, so state maps to operators by ID rather than by graph position, and if state serializers are compatible. Renamed or removed operators need `--allowNonRestoredState`, used deliberately. Results are idempotent, so the overlap during the switch is harmless.

**Deep technical explanation:**
Rescaling uses the same mechanism: restore the savepoint at a new parallelism. Key groups are redistributed, up to `maxParallelism`. Changing a state type (e.g. a POJO field) needs a serializer that supports evolution. Kryo-serialized state is a classic trap that blocks upgrades.

**How StreamPulse demonstrates this:**
Planned Phase 5 demo (see [ROADMAP](ROADMAP.md)). Not implemented yet.

**Potential follow-up question:**
What if the new version changes the window size?

**Follow-up answer:**
That's a semantic change, not just a code change. The old window state can't become new window state. I'd deploy the new logic as a new operator UID, starting fresh and backfilling from raw Kafka (up to 7 days of retention) into new result tables or rows, then switch the dashboards. That's the payoff of keeping an immutable, replayable raw log.

---

**Question:** How would you recover after losing the PostgreSQL database entirely?

**Difficulty:** Senior

**Interview-ready answer:**
PostgreSQL is a derived serving store, not the system of record. Recovery:
1. Recreate it; the migrations rebuild the schema automatically.
2. Restart the Flink job from scratch (no state), with the Kafka source starting at the earliest offset of `raw`.
3. Recompute everything still within Kafka retention, which is 7 days.

Idempotent upserts make this safe even if the database is restored from a partial backup. Anything older than the retention window needs database backups or an archive of raw events in object storage.

**Deep technical explanation:**
This is the event-sourcing property in practice: derived views can be rebuilt from the log. The constraints are retention, rebuild time (measured by the Phase 5 throughput ceiling) and watermark behaviour during catch-up, when partitions are read at different speeds. Long-term rebuilds would need raw events archived to object storage, for example with a Kafka-to-S3 sink.

**How StreamPulse demonstrates this:**
The design is in [ADR-0004](adr/0004-kafka-topic-topology.md) and [ADR-0008](adr/0008-postgres-sink-semantics.md). A rebuild runbook is planned for Phase 5.

**Potential follow-up question:**
How long would a 7-day rebuild take?

**Follow-up answer:**
I don't know yet, and I won't guess. At 47 events per second, 7 days is about 28 million events. Rebuild time is that divided by the measured maximum replay throughput from Phase 5.
