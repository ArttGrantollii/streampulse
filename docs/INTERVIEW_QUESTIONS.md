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
- [Phase 1 — Infrastructure foundation](#phase-1--infrastructure-foundation)
  - [Kafka in KRaft mode](#kafka-in-kraft-mode)
  - [Flink, Kafka offsets and checkpoints](#flink-kafka-offsets-and-checkpoints)
  - [Platform engineering](#platform-engineering)
- [Phase 2 — Ingestion](#phase-2--ingestion)
  - [Producer design](#producer-design)
  - [Source behaviour](#source-behaviour)
  - [Engineering practice](#engineering-practice)
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

## Phase 1 — Infrastructure foundation

### Kafka in KRaft mode

**Question:** What is KRaft, and what does running Kafka in "combined mode" mean in StreamPulse?

**Difficulty:** Fundamental / Intermediate

**Interview-ready answer:**
KRaft is Kafka's built-in Raft consensus for cluster metadata. Topics, partitions, leaders and configs are stored in an internal metadata log, replicated across a quorum of *controller* nodes. Before KRaft, Kafka needed a separate ZooKeeper ensemble for this. Kafka 4.x is KRaft-only. StreamPulse runs one node with `process.roles=broker,controller`, so the same JVM serves client traffic and is the entire controller quorum. That's fine for local development, but in production you'd run three or five dedicated controllers, so a broker under heavy load can't destabilise metadata management.

**Deep technical explanation:**
The controller quorum elects a leader with Raft. Brokers fetch the metadata log from it and apply changes. A `CLUSTER_ID` identifies the cluster, and the storage directory is formatted with it on first start. That's why the image refuses to start in KRaft mode without one, and why it's fixed in `.env` rather than random per start: a new ID wouldn't match the already-formatted volume. With a single controller there's no fault tolerance for metadata at all. A quorum of three tolerates one failure, and five tolerates two.

**How StreamPulse demonstrates this:**
The `kafka` service in [`docker-compose.yml`](../docker-compose.yml) sets `KAFKA_PROCESS_ROLES`, `KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093` and `CLUSTER_ID`. The data in the `kafka-data` volume survived `docker compose down` and `up` with the same ID.

**Potential follow-up question:**
Why set the internal topics' replication factor to 1 explicitly?

**Follow-up answer:**
Internal topics such as `__consumer_offsets` and the transaction log default to RF 3. On a single broker they can't be created, so consumer groups or transactions would fail at first use, not at startup, which is a confusing delayed failure. Setting them to 1 is correct for one broker. In production they should be 3, with `min.insync.replicas=2`.

---

**Question:** Why does the Kafka container define three listeners, and what is an "advertised" listener?

**Difficulty:** Intermediate

**Interview-ready answer:**
A Kafka client first connects to a bootstrap address, but then uses the broker addresses returned in *metadata* for all real traffic. Those returned addresses are the advertised listeners. Containers must be told `kafka:9092`, a Docker DNS name, while tools on my laptop must be told `localhost:9094`, because `kafka` doesn't resolve on the host. So there is an INTERNAL listener for containers, an EXTERNAL one for the host, and a CONTROLLER listener for KRaft traffic. The classic symptom of getting this wrong is a client that connects to the bootstrap address, then hangs or times out connecting to an address it can't reach.

**Deep technical explanation:**
`listeners` is where the broker binds (`0.0.0.0:port`). `advertised.listeners` is what it tells clients. `listener.security.protocol.map` assigns a protocol to each named listener, and `inter.broker.listener.name` chooses which one brokers use to talk to each other. The host port is bound to `127.0.0.1` only, so nothing outside the laptop can reach the broker, which matters because it's PLAINTEXT.

**How StreamPulse demonstrates this:**
The `KAFKA_LISTENERS` and `KAFKA_ADVERTISED_LISTENERS` settings in [`docker-compose.yml`](../docker-compose.yml). The Flink job uses `kafka:9092`. Host tooling (the Phase 2 producer during development) will use `localhost:9094`.

**Potential follow-up question:**
How would this look in production?

**Follow-up answer:**
One listener per network zone (internal VPC, cross-VPC or peering, maybe a public endpoint), each with TLS and SASL authentication, and ACLs per principal. Advertised hostnames must resolve from wherever each client runs, which is a common source of trouble with Kubernetes and cloud load balancers.

---

### Flink, Kafka offsets and checkpoints

**Question:** After a crash, where does Flink's Kafka source resume from: the consumer group's committed offsets, or something else?

**Difficulty:** Advanced

**Interview-ready answer:**
From the offsets stored *in the Flink checkpoint* it restores. The Kafka connector docs are explicit that the source doesn't rely on committed offsets for fault tolerance. It commits offsets back to Kafka only when a checkpoint completes, purely so external tools can see progress. My smoke test shows this: after the job had counted all 500 records, the group's committed offsets reached 500 (lag 0) only once a checkpoint had completed.

**Deep technical explanation:**
The source's split state, meaning partition plus offset, is part of the checkpoint, taken consistently with every other operator's state at the barrier. On recovery, all operators restore checkpoint *n* and the source rewinds to the offsets recorded in *n*. That is what makes state exactly-once: state and input position always come from the same consistent cut. If recovery used committed offsets instead, they could be ahead of or behind the restored state. This is also why consumer-group lag for a Flink job looks like a sawtooth that moves with the checkpoint interval.

**How StreamPulse demonstrates this:**
[`scripts/smoke_test.sh`](../scripts/smoke_test.sh) waits until offsets committed by group `streampulse-smoke` sum to N, after checkpoints complete. The job is [`SmokeJob`](../flink-jobs/src/main/java/io/github/arttgrantollii/streampulse/smoke/SmokeJob.java).

**Potential follow-up question:**
When *are* committed offsets used by Flink?

**Follow-up answer:**
They're used when a job starts *without* restoring state, if it's configured with `OffsetsInitializer.committedOffsets(...)`. For example, after a deliberate fresh deploy you might want to continue roughly where the previous job stopped. The smoke job uses `earliest()` because its topic is recreated each run. Without checkpointing at all, the source falls back to the Kafka client's auto-commit.

---

**Question:** What is actually inside a Flink checkpoint directory, and why must the JobManager and TaskManagers share it?

**Difficulty:** Intermediate / Advanced

**Interview-ready answer:**
Each job gets a directory with:
- `chk-N` folders, one per retained checkpoint, each with a `_metadata` file written by the JobManager that describes the whole checkpoint;
- `shared/`, where incremental RocksDB checkpoints keep SST files that several checkpoints reference;
- `taskowned/`, for state owned by the tasks themselves.

TaskManagers write the state files and the JobManager writes the metadata pointing to them, so both need the same path. In StreamPulse that's a named Docker volume mounted into both containers.

**Deep technical explanation:**
With incremental checkpoints, checkpoint *n* uploads only the SST files that are new since *n−1*. `_metadata` references files in `shared/`, which Flink reference-counts, so an SST file is deleted only when no retained checkpoint uses it. This is why you can't delete "old" files from `shared/` by hand. In production the shared path is object storage (S3, ADLS, GCS), because a local disk is neither shared nor durable across hosts.

**How StreamPulse demonstrates this:**
The smoke test lists `chk-1`, `chk-2`, `shared` and `taskowned` on the `flink-data` volume, and checks that `_metadata` still exists after the job is cancelled (`RETAIN_ON_CANCELLATION`). Rationale is in [ADR-0009](adr/0009-state-backend-and-checkpoint-storage.md).

**Potential follow-up question:**
What's the difference between a retained checkpoint and a savepoint, if both survive cancellation?

**Follow-up answer:**
A retained checkpoint is Flink-owned, possibly incremental, in the backend's native format, and managed by retention settings. It exists for recovery. A savepoint is user-triggered and user-owned, never deleted automatically, and meant for planned operations: upgrades, rescaling, migrations. You can restore from a retained checkpoint in an emergency, but you plan operations around savepoints.

---

### Platform engineering

**Question:** How does StreamPulse make sure topics, schemas and directories exist before services need them, without manual steps?

**Difficulty:** Intermediate

**Interview-ready answer:**
With one-shot init containers gated by health checks:
- `kafka-init` waits for Kafka to be healthy, then creates the topics with `--if-not-exists` and re-applies their configs;
- `db-migrate` waits for PostgreSQL, then runs Flyway;
- `flink-data-init` fixes volume ownership before Flink starts.

Each is idempotent, so running it again changes nothing, which I verified by re-running all three. I deliberately didn't use `docker compose up --wait`. It treats a successfully exited init container as a failure, and I saw it return exit code 0 on one run and 1 on the next with the same stack. So `make up` uses a small script that checks long-running services for `healthy` and init services for exit code 0.

**Deep technical explanation:**
Health checks should test *readiness for the dependant's purpose*: for example, the TaskManager is healthy once it has registered with the JobManager, not merely once its process has started. Sleeps encode a guess about timing, and dependency conditions encode the actual requirement. Idempotent init lets the same command converge any state: a fresh machine, a partial failure, or config drift.

**How StreamPulse demonstrates this:**
[`docker-compose.yml`](../docker-compose.yml) (`depends_on` conditions), [`create-topics.sh`](../infra/kafka/create-topics.sh), [`wait_for_stack.sh`](../scripts/wait_for_stack.sh). I tested the failure path with a deliberately broken migration, and the script printed Flyway's error and exited 1.

**Potential follow-up question:**
Why does `create-topics.sh` fail on a partition-count mismatch instead of fixing it?

**Follow-up answer:**
Partitions can be added but never removed, and adding them changes `hash(key) % partitions`. That remaps keys and breaks per-key ordering across the change. That decision needs a human, so the script stops loudly. Retention and compression are safe to re-apply, so it does.

---

**Question:** Why use Flyway migrations instead of the PostgreSQL image's init scripts?

**Difficulty:** Intermediate

**Interview-ready answer:**
The image's `docker-entrypoint-initdb.d` scripts run only when the data directory is empty, so they can't evolve an existing database. Flyway applies numbered migrations in order and records each one in `flyway_schema_history`, so every environment converges to the same version and schema changes are reviewable code. I also disabled `clean` so the "drop everything" command can't run by accident.

**Deep technical explanation:**
PostgreSQL has transactional DDL. When I tested a deliberately broken migration, Flyway reported "Changes successfully rolled back", and the history still showed only v001. Many databases (e.g. MySQL) auto-commit DDL, so a failed migration can leave a half-applied schema. The baseline migration also sets the database timezone to UTC, so event-time windows read the same from every client.

**How StreamPulse demonstrates this:**
[`database/migrations/V001__baseline.sql`](../database/migrations/V001__baseline.sql) and the `db-migrate` service.

**Potential follow-up question:**
How do you handle a migration that must change a large table without downtime?

**Follow-up answer:**
Use expand and contract. Add the new column or table in a backwards-compatible migration, deploy code that writes both versions, backfill in batches, switch reads, then drop the old structure in a later migration. Build indexes with `CREATE INDEX CONCURRENTLY`, which can't run inside a transaction, so that migration has to be marked non-transactional.

---

**Question:** In the Flink job's build, what goes into the jar and what doesn't, and why?

**Difficulty:** Intermediate / Advanced

**Interview-ready answer:**
Flink's own APIs (`flink-streaming-java`, `flink-clients`) and SLF4J are `provided`: the cluster already has them in `lib/`, and packaging a second copy risks class conflicts. Connectors are *not* part of the Flink distribution, so the Kafka connector and its `kafka-clients` 4.2.0 are shaded into the uber-jar. The shade plugin also merges `META-INF/services` files, because connectors register themselves through Java's ServiceLoader. Without merging, one connector's registration silently overwrites another's.

**Deep technical explanation:**
I hit this for real. Flink 2.2's POMs don't pass `slf4j-api` through to job projects, so the first build failed with "package org.slf4j does not exist". The fix is to declare it `provided`, matching the runtime's 1.7.36, rather than bundling a logging implementation. For tests, I use the same binding the image ships (`log4j-slf4j-impl`) and the JUnit and AssertJ versions Flink's test utilities are built on, rather than the newest JUnit 6, which could conflict with them.

**How StreamPulse demonstrates this:**
[`flink-jobs/pom.xml`](../flink-jobs/pom.xml). The shaded jar is about 24 MB.

**Potential follow-up question:**
What would break if you shaded Flink itself into the jar?

**Follow-up answer:**
You'd have two copies of Flink classes in different classloaders. Flink loads user code child-first, so types like `StreamExecutionEnvironment` or state descriptors from your jar wouldn't match the runtime's. That causes `ClassCastException` or `LinkageError` problems that are hard to diagnose. Provided scope keeps one copy, owned by the runtime.

---

## Phase 2 — Ingestion

### Producer design

**Question:** How does the StreamPulse producer make sure it never silently loses events between Wikimedia and Kafka?

**Difficulty:** Intermediate / Advanced

**Interview-ready answer:**
Three mechanisms, one per failure point:
- **Acknowledged writes:** `acks=all` with an idempotent producer. A record counts only once Kafka confirms it, and any delivery failure makes the producer stop instead of moving on.
- **Reconnects:** before resuming with `Last-Event-ID`, I flush, so the id I resume from refers to an event Kafka has acknowledged.
- **Restarts:** the producer reads the last record of every partition back from Kafka, takes the *oldest* position, and resumes 30 seconds before it.

The trade-off is deliberate: I accept duplicates, which Flink dedup removes, in exchange for never having a gap. A restart test measured an overlap of exactly the 30-second margin, with no gap.

**Deep technical explanation:**
- **Why the oldest partition:** the idempotent producer only orders writes *per partition*. After a crash, one partition can hold acknowledged records newer than records still in flight to another, so resuming from the newest position could skip those.
- **Why Kafka itself is the checkpoint:** every record carries its upstream SSE id in a header, so the log can't disagree with a separate state file about what was actually written.
- **Backpressure:** if librdkafka's local queue is full, `produce()` raises `BufferError`. I block and poll instead of dropping, which stops reading upstream: backpressure all the way to the source.

**How StreamPulse demonstrates this:**
[`publisher.py`](../producer/src/streampulse_producer/publisher.py), [`live.py`](../producer/src/streampulse_producer/live.py), [`resume.py`](../producer/src/streampulse_producer/resume.py), [ADR-0012](adr/0012-producer-resume-from-kafka.md). Measurements are in [Research 0002 §1.4](research/0002-ingestion-behaviour.md).

**Potential follow-up question:**
So is Wikimedia → Kafka exactly-once?

**Follow-up answer:**
No, it's at-least-once, and only while downtime is shorter than Wikimedia's retention of 7 to 31 days. Idempotence removes duplicates from *retries within one producer session*. It can't remove duplicates from re-reading the source after a reconnect or restart, and neither can Kafka transactions, because the duplicate comes from upstream, not from the write path. That's why dedup by `meta.id` belongs downstream, in Flink.

---

**Question:** Tell me about a bug you found in StreamPulse that tests didn't catch.

**Difficulty:** Senior

**Interview-ready answer:**
Restart resume. My unit tests passed: the producer computed the right position and sent it as `since=<epoch milliseconds>`. When I restarted it for real, it ingested about 85,000 events in under a minute, all from **8 days earlier**. Wikimedia's `since` parameter is parsed with JavaScript's `Date.parse`. An epoch-ms string isn't valid input, and instead of returning an error, the server silently starts from its oldest retained event. I confirmed it with a direct probe: ISO-8601 was honoured to within 0.0 seconds, and epoch ms was off by −712,330 seconds. I fixed the format, but I also added two guards, because the real problem was trusting the request rather than verifying its effect:
- the first event must be near the requested time, or the producer stops;
- a resume more than 24 hours back is refused unless explicitly allowed.

**Deep technical explanation:**
My test was wrong too: it asserted the epoch value I sent, which encoded my assumption instead of the server's contract. I replaced it with one that asserts the ISO format, and added tests for both guards. The cleanup mattered as well. The raw topic is the system of record, so I truncated the polluted records with `kafka-delete-records` before starting the 24-hour measurement. Otherwise the event-time statistics, and later every Flink test, would have been contaminated.

**How StreamPulse demonstrates this:**
The incident section of [ADR-0012](adr/0012-producer-resume-from-kafka.md), [Research 0002 §1.1](research/0002-ingestion-behaviour.md), and `since_param` / `_check_resumed_at` in [`live.py`](../producer/src/streampulse_producer/live.py).

**Potential follow-up question:**
How do you protect against this class of problem in general?

**Follow-up answer:**
Verify outcomes at boundaries you don't control: check the response matches the request. Put hard limits on blast radius, like a maximum catch-up age or rate limits. Make the system of record cleanable. And test against the real dependency where it's cheap: a 25-second restart against the live service found what dozens of mocked tests couldn't.

---

**Question:** Why does your Python producer set `partitioner=murmur2_random`, and how did you verify it?

**Difficulty:** Intermediate / Advanced

**Interview-ready answer:**
librdkafka's default partitioner hashes keys with CRC32, while the Java client uses murmur2. If both a Python and a Java producer wrote the same key to the same topic, they'd put it on different partitions, which silently breaks per-key ordering. `murmur2_random` makes librdkafka hash like Java. I didn't trust the config name: my integration test implements Kafka's Java murmur2, checks it against all six reference values in Kafka 4.2.2's own unit tests, then asserts that every record the producer wrote landed on the partition Java would have chosen.

**Deep technical explanation:**
Java's default partition is `toPositive(murmur2(keyBytes)) % numPartitions`, where `toPositive` masks the sign bit. The `_random` suffix only affects null keys, which are spread randomly; Java's sticky partitioner behaves differently there, but null keys have no ordering to preserve. The bigger lesson is client heterogeneity: defaults differ across Kafka clients (`enable.idempotence` is false in librdkafka but true in Java since 3.0), so settings must be explicit.

**How StreamPulse demonstrates this:**
`test_replay_lands_every_record_byte_for_byte_on_the_java_partition` and `test_java_murmur2_matches_kafkas_reference_values` in [`test_integration_kafka.py`](../producer/tests/test_integration_kafka.py).

**Potential follow-up question:**
What happens to key placement if you later add partitions?

**Follow-up answer:**
`hash % n` changes for most keys, so new records for a key can go to a different partition than its old ones, and per-key ordering across the change is lost. That's why `create-topics.sh` refuses to "fix" a partition-count mismatch automatically, and why I picked the count upfront.

---

### Source behaviour

**Question:** Wikimedia's docs say connections are closed after 15 minutes. How do you handle reconnects, and what did you actually observe?

**Difficulty:** Intermediate / Advanced

**Interview-ready answer:**
Reconnects are treated as routine. If a connection lived at least 60 seconds and delivered events, the producer resets its backoff and reconnects immediately with `Last-Event-ID`. Only quick, repeated failures back off exponentially with full jitter, up to 60 seconds, and `Retry-After` is honoured. What I observed didn't match the docs: in the first 90 minutes, **no connection reached 15 minutes and none closed cleanly**. Streams broke mid-body ("incomplete chunked read") after 2 to 13 minutes. I relabelled the reconnect metric so routine drops are distinguishable from real outages, and added a connection-lifetime histogram so the 24-hour run measures the distribution instead of assuming it.

**Deep technical explanation:**
- **"At most 15 minutes" is a limit, not a schedule.** With a single client I can't tell whether the drops come from Wikimedia or from the network path (home router, WSL/Docker NAT). The lifetime histogram plus a run from a second network would separate them.
- **Labelling depends on exception order.** httpx's `ConnectTimeout` is a subclass of `TimeoutException`, so connect errors have to be classified first.
- **The cost of each reconnect is a small duplicate overlap.** The SSE id holds a millisecond *timestamp*, not an offset; I found 3 cases in 120 events where several events share one id, so a resume re-delivers that millisecond. Records carry `sp-connection`, so the analysis attributes every duplicate to a reconnect or a restart.
- **Full jitter** (a uniform random wait between 0 and the exponential ceiling) prevents synchronised retry storms.

**How StreamPulse demonstrates this:**
`classify_transport_error` in [`live.py`](../producer/src/streampulse_producer/live.py), [`backoff.py`](../producer/src/streampulse_producer/backoff.py), `test_mid_stream_drop_keeps_delivered_events_and_resumes_after_the_last_one`, and [Research 0002 §1.8–1.9](research/0002-ingestion-behaviour.md). That includes an unplanned 8-minute internet outage: DNS failures, jittered backoff, then 20,423 events caught up in 84 s with no gap.

**Potential follow-up question:**
Why not run two producers in parallel for redundancy?

**Follow-up answer:**
Each would write the full stream, doubling every record. It would work, since dedup by `meta.id` removes the copies, but it doubles Kafka volume and dedup state to protect against something resume already handles. Production systems use active-passive with leader election instead. I'd only consider active-active if the requirement was zero ingestion gap even during a producer restart.

---

**Question:** A test passed on your machine but failed in CI. How did you debug it without access to the logs?

**Difficulty:** Intermediate / Advanced

**Interview-ready answer:**
The producer's integration tests passed locally, even against a freshly started local broker, but failed in GitHub Actions, and I couldn't read the job logs. Instead of guessing, I made failures visible: CI now turns pytest's failure summary into `::error` annotations, which appear on the commit and are readable through the public checks API. The next run showed `_DESTROY_BROKER` while reading the resume position. librdkafka retires its bootstrap connection once it learns the broker's advertised address, and a request in flight on it fails. It was a race only a slow, cold runner hit. It was in *production* code, so the producer could have crashed at startup. I added bounded retries for transient Kafka errors, with unit tests, and CI went green.

**Deep technical explanation:**
- **Classify, then retry:** retry only errors that mean "try again" (`_DESTROY_BROKER`, `_TRANSPORT`, `_TIMED_OUT`, leader changes, or anything librdkafka marks `retriable()`). Retrying everything hides real errors.
- **Bound the retries** (5 attempts, exponential backoff) and re-raise afterwards, so a broken setup still fails loudly.
- **Remove unnecessary dependencies on timing:** the test consumer switched from `subscribe()` (consumer-group coordination) to `assign()` (explicit partitions).
- **Treat environment differences as data:** CPU speed, cold caches and network timing are part of "the environment". A race that never shows locally is still a bug.

**How StreamPulse demonstrates this:**
`with_retries` in [`kafka_position.py`](../producer/src/streampulse_producer/kafka_position.py) and its tests, the "Annotate failures" step in [`ci.yml`](../.github/workflows/ci.yml), and [Research 0002 §1.10](research/0002-ingestion-behaviour.md).

**Potential follow-up question:**
How would you make such races reproducible?

**Follow-up answer:**
Inject the fault deterministically: a fake that raises the transient error first (which my unit test does). Add network shaping in integration tests (latency, dropped connections, e.g. with Toxiproxy), run integration tests many times in CI to surface flakiness, and record which tests are flaky rather than silently re-running them.

---

**Question:** What did measuring the real stream change about your event-time design?

**Difficulty:** Advanced

**Interview-ready answer:**
Three things, even before the 24-hour run finished:
- **Edits are more out of order than events overall.** In a 10-minute sample, edits were p99 25 s behind the newest edit in their partition, against p99 13 s for all events. My initial 10-second watermark bound was too small.
- **Some legitimate events have ancient timestamps.** A Commons upload log event emitted today carried a 2005 timestamp, so "older than 31 days means invalid" would have sent valid data to the DLQ. Time sanity bounds belong on the events that analytics use.
- **The dangerous case is the opposite: future timestamps.** An old event can't move a max-based watermark, but one future timestamp would push it ahead and make every normal event late. The analysis now counts both cases by event type.

**Deep technical explanation:**
Disorder must be measured *per Kafka partition*, because Flink generates watermarks per partition and takes the minimum. Measuring disorder over the whole stream would describe the wrong thing. I also separate steady-state delay from catch-up: after a restart the producer replays its backlog, and those old events say nothing about pipeline latency.

**How StreamPulse demonstrates this:**
[`analyze_raw_topic.py`](../scripts/research/analyze_raw_topic.py), [Research 0002 §1.5–1.7](research/0002-ingestion-behaviour.md), and the early-evidence note in [ADR-0006](adr/0006-event-time-and-watermarks.md).

**Potential follow-up question:**
Why not just choose a very large watermark bound to be safe?

**Follow-up answer:**
Every result would wait that long: a 1-minute window with a 3-minute bound closes about 4 minutes late, for every window, all the time. The better design is a bound near p99, which keeps early results fast, plus allowed lateness near p99.99, which corrects them through idempotent upserts. Whatever is later still goes to a late topic. That's a latency versus completeness trade-off, set from data.

---

### Engineering practice

**Question:** How do you keep a Python project's dependencies reproducible and safe from brand-new releases?

**Difficulty:** Intermediate

**Interview-ready answer:**
`uv.lock` pins every package, transitive ones included, and CI runs `uv sync --locked`, which fails if the lock is stale. On top of that, `exclude-newer` in `pyproject.toml` makes the resolver ignore anything published after a cutoff. That enforces my "nothing younger than two weeks" rule across the whole tree, not only the packages I name. You can see it working: it resolved ruff 0.17.0 even though 0.17.1 existed, because 0.17.1 was published after the cutoff.

**Deep technical explanation:**
New releases are where regressions and occasional supply-chain incidents surface first. A short quarantine gives the ecosystem time to notice. The trade-off is slower uptake of security fixes, so the cutoff is a dated, reviewable line that gets moved forward deliberately, not a permanent freeze. The Docker image installs from the same lock (`uv sync --locked --no-dev`), so what runs is exactly what was tested.

**How StreamPulse demonstrates this:**
[`producer/pyproject.toml`](../producer/pyproject.toml) (`[tool.uv] exclude-newer`), `producer/uv.lock`, [`producer/Dockerfile`](../producer/Dockerfile), and the CI workflow.

**Potential follow-up question:**
What would you add for a production service?

**Follow-up answer:**
Automated dependency update PRs (Renovate or Dependabot) that respect the quarantine, vulnerability scanning of the lock and image (e.g. `pip-audit`, Trivy) in CI, hash-pinned base images, and an SBOM per release.

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

---

**Question:** Your local platform runs with one command. What would you change to run the same stack in production?

**Difficulty:** Senior

**Interview-ready answer:**
Almost every "one" becomes "several", and every shortcut gets replaced:
- **Kafka:** three or more brokers with RF 3 and `min.insync.replicas=2`, plus a separate three- or five-node controller quorum.
- **Flink:** JobManager HA (Kubernetes HA via the Flink Operator), several TaskManagers, and checkpoints in object storage instead of a Docker volume.
- **PostgreSQL:** managed, with backups and point-in-time recovery.
- **Security:** TLS and SASL with ACLs instead of PLAINTEXT, and secrets from a secret manager instead of `.env`.
- **Readiness checks** become Kubernetes readiness probes, and init containers become operator-managed resources or CI/CD steps.

The *design* stays the same; the *operations* change.

**Deep technical explanation:**
The things that don't change are what make the design portable: idempotent topic and schema provisioning, explicit health-based dependencies, pinned versions, configuration outside code, and state outside processes. Those are habits, not tools. What changes is the failure domain. Locally, one Docker host is a single point of failure for everything. In production, each component's own replication and HA defines it, and you then need monitoring to tell you when that redundancy is degraded.

**How StreamPulse demonstrates this:**
The local shortcuts are documented as such: ARCHITECTURE §3.1, §19 and §22, and the cloud mapping in §21. The production equivalents are reasoning, not implementation.

**Potential follow-up question:**
Which of these would you prioritise if you could only do one?

**Follow-up answer:**
Durable, replicated storage for the system of record: Kafka with RF 3, and checkpoints in object storage. Compute can be recreated, but lost data can't. JobManager HA is next, because without it a JobManager failure needs manual resubmission.
