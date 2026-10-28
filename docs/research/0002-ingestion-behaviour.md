# Research 0002 — Ingestion behaviour: resume, duplicates, disorder, clocks

- **Date:** 2026-10-28 (Phase 2). The 24-hour run started 2026-10-28 13:09:37 UTC.
- **Questions:** R-1 to R-6 from [Research 0001 §7](0001-eventstreams-source-validation.md#7-open-items-carried-into-later-phases). How does the real source behave under reconnects and restarts, and what event-time disorder must the watermark policy absorb?
- **Method:**
  - The producer writes every live event to `wikimedia.recentchange.raw.v1`, with its upstream SSE id, ingest time and upstream connection in `sp-*` headers. **The raw topic is the recording.**
  - [`scripts/research/analyze_raw_topic.py`](../../scripts/research/analyze_raw_topic.py) reads it back and computes each measurement.
  - Wikimedia's `since` behaviour was probed directly with ISO and epoch values.

Labels: **FACT** = measured or observed here; **PRELIMINARY** = measured on too small a sample to set parameters.

---

## 1. Findings from building and testing the producer (2026-10-28)

### 1.1 `since` must be ISO-8601. An unparseable value is silently ignored (FACT)
- **Probe:** request `?since=` two minutes ago, then read the first event's upstream timestamp.

  | `since` value | First event relative to the requested time |
  |---|---|
  | `2026-10-28T12:32:32.415Z` (ISO-8601, ms) | **0.0 s** |
  | `1793190752415` (epoch ms) | **−712,330 s (−8.2 days)**: the oldest retained event |

- The docs say `since` accepts anything JavaScript's `Date.parse()` understands. A bare epoch-ms number isn't, and **the server returns no error**: it starts from the oldest retained offset.
- **How we found it:** the producer's first restart test resumed "from" a correct position but ingested **~85,000 events from 8 days earlier** in under a minute. The polluted raw-topic data was truncated (`kafka-delete-records`) before the 24-hour run.
- **Consequences, implemented:**
  1. `since` is sent as ISO-8601 UTC with milliseconds.
  2. The first event of a `since` connection must be no more than 5 min earlier than requested; otherwise the producer stops (`ResumeMismatchError`, exit 1).
  3. The producer refuses to resume from more than `--max-resume-age` ago (default 24 h, exit 2), so a long catch-up is a deliberate decision.

### 1.2 Wikimedia's active datacenter switched from eqiad to codfw (FACT)
- On 2026-10-26 the SSE ids carried timestamps for `eqiad.*` and `offset: -1` for `codfw.*`. On 2026-10-28 it's the reverse.
- This is the switchover case [ADR-0012](../adr/0012-producer-resume-from-kafka.md) anticipated. Because the producer stores the full id array and reads whichever entry carries a timestamp, it needed no change. A real-world test, passed.

### 1.3 SSE ids aren't unique per event (FACT)
- In the 120-event fixture, **3 SSE ids were shared by more than one event**. The id is a millisecond timestamp, not an offset.
- **Consequences:**
  - Resuming from such an id re-delivers events that share its millisecond, so resume is at-least-once by construction.
  - The SSE id must never be used as an event's identity. Dedup uses `meta.id` ([ADR-0007](../adr/0007-deduplication.md)).

### 1.4 Restart resume overlaps by exactly the safety margin (FACT, one test)
- Run A ran 40 s and was stopped with SIGTERM. Run B was started afterwards.
- B resumed from `2026-10-28T12:46:36.750Z`. Its first event was **30.0 s** before A's last event, which equals the configured margin.
- The windows overlap, so there's no gap. The overlap produced **949 duplicate `meta.id`s** (about 30 s of traffic).
- After 8.5 minutes of downtime, the container caught up about 14,000 events within seconds.

### 1.5 Legitimate events with very old `timestamp` exist (FACT)
- A Commons `log` event (type `upload`) emitted 2026-10-28 carried `timestamp` = **2005-01-23**. That's 21.8 years before ingest, and legitimate.
- **Consequence for Phase 3:** a "more than 31 days old → DLQ as impossible" rule would mislabel valid non-edit events. Sanity bounds should apply to the events analytics use (`edit`/`new`). The 24-hour run counts far-past and future timestamps by type to decide.
- Old timestamps can't move a max-based watermark. A **future** timestamp would: it would make every normal event look late. That's the case to guard against.

### 1.6 Clock offset varies (FACT)
- The producer's SNTP probe measured the WSL2 clock at **−0.09 to −0.18 s** (slightly ahead) on 2026-10-28.
- On 2026-10-26 an HTTP `Date` comparison suggested it was about 3–4 s behind.
- WSL clock drift is not constant, so the producer exports `streampulse_producer_clock_offset_seconds` rather than assuming a fixed correction.

### 1.7 Small-sample numbers (PRELIMINARY: ~10 minutes, including a catch-up)

| Measurement | Value |
|---|---|
| Event rate | ~31–40 events/s (vs 47/s on Monday afternoon) |
| Receive − `meta.dt`, after catch-up | p50 256 ms, p95 2.7 s, p99 5.4 s |
| `timestamp` disorder per Kafka partition, **all events** | p99 13 s, p99.9 60 s |
| `timestamp` disorder per Kafka partition, **edits only** | p99 25 s, p99.9 108 s, max 195 s |
| `(wiki, rcid)` under different `meta.id` (R-3) | 0 |
| Canary events (R-5) | 0 |

The edit-only disorder is already larger than the initial watermark assumption (B = 10 s, ADR-0006). The parameters will be set from the 24-hour data, not from this sample.

### 1.8 Connections drop mid-stream long before 15 minutes (FACT, first ~90 minutes)
- In the first 90 minutes of the run, **no connection reached Wikimedia's documented 15-minute limit, and none ended cleanly**.
- Streams broke mid-body with `RemoteProtocolError: incomplete chunked read` after **144, 238, 287, 399, 419, 712 and 802 s** (healthy connections only).
- There were also 2 read timeouts (no data for 30 s), one of which was the start of the outage below.
- **Cause unknown.** From one client we can't separate Wikimedia's side from the network path (home router, WSL/Docker NAT). The `connection_lifetime_seconds` histogram, added the same day, measures the 24-hour distribution.
- **Impact:** none on correctness. Every drop resumed with `Last-Event-ID`. It does mean more boundary duplicates than a 15-minute cadence would produce, and R-1 quantifies that.
- Reconnect causes were relabelled so routine drops (`stream_interrupted`) can be told apart from real connectivity problems (`connect_error`).

### 1.9 Internet outage recovery (FACT, unplanned)
- From about 14:12 to 14:20 UTC, the dev machine lost internet access.
- The producer logged 17 `ConnectError` (DNS: "Name or service not known") reconnects, with full-jitter backoff waits growing from 0.3 s to a maximum of 57 s (cap 60 s).
- On recovery it resumed with `Last-Event-ID` and caught up **20,423 events in 84 s**, with no gap and **0 delivery errors**: 178,746 of 178,769 received events were acknowledged, and the remaining 23 were in flight at the moment of the reading.

### 1.10 Startup race against a cold broker (FACT, CI)
- In GitHub Actions, reading the resume position failed once with `_DESTROY_BROKER`.
- librdkafka retires its bootstrap connection after learning the broker's advertised address, and a request still in flight on it fails.
- It never happened against the warm local broker, nor against a freshly started local one.
- **Fix:** transient errors are retried with backoff (`kafka_position.with_retries`). CI has been green since.

### 1.11 Planned interruptions during the 24-hour run
| Time (UTC) | Event | Why |
|---|---|---|
| 2026-10-28 14:43:32 | Producer container replaced (graceful SIGTERM, resume from Kafka) | Deploy the reconnect-label and startup-retry fixes |

## 2. 24-hour results

⏳ **Pending.** The run started 2026-10-28 13:09:37 UTC. This section will contain the output of `analyze_raw_topic.py` over the full window:
- R-1 duplicates by cause;
- R-2 disorder by hour and type;
- R-3, R-5 and R-6;
- connection lifetimes (from `sp-connection` spans, plus the producer's `connection_lifetime_seconds` histogram);
- receive delay;
- any interruptions, such as laptop sleep, and how resume handled them.

ADR-0006 and ADR-0007 parameters will then be set from these numbers.
