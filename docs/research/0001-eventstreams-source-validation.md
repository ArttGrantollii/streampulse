# Research 0001 — Wikimedia EventStreams source validation

- **Date:** 2026-10-26 (Phase 0)
- **Question:** What exactly does Wikimedia's live stream give us, and which assumptions in the original StreamPulse specification are wrong?
- **Method:** official documentation and published JSON Schemas, plus a 2-minute live capture of two streams, analysed with [`scripts/research/`](../../scripts/research/).

Labels: **FACT** = confirmed from docs or directly observed; **ASSUMPTION** = believed but not yet verified; **EXPERIMENT** = something a later phase will measure.

---

## 1. Endpoint and protocol

| Item | Finding | Label |
|---|---|---|
| Endpoint | `https://stream.wikimedia.org/v2/stream/{streams}`; `recentchange` is an alias of `mediawiki.recentchange` | FACT ([EventStreams docs][es], `?spec` path list) |
| Protocol | HTTP chunked transfer with **Server-Sent Events** (SSE). `Accept: application/json` returns NDJSON, but drops the SSE `id` that we need for resuming | FACT |
| Multiple streams | Comma-separated: `/v2/stream/a,b` | FACT |
| Server-side filtering | **Not supported.** Clients must filter themselves | FACT |
| Connection lifetime | WMF terminates connections after **15 minutes**. Clients must reconnect and resume with `Last-Event-ID` | FACT |
| Intended audience | "small scale external tool developers". WMF production services should consume Kafka directly | FACT |
| Rate limits | No numeric limits are published. The docs ask clients to "only consume the minimum of data you need" and to be kind with historical timestamps | FACT |
| User-Agent | Not strictly required by the EventStreams page. The Python example sets one and links to the Wikimedia API etiquette / User-Agent policy | FACT |

**Design consequences.** A 15-minute reconnect is *normal operation*, not a failure. The producer must treat reconnects as routine and resume correctly every time, and should count them as a metric rather than log them as errors. We send a descriptive User-Agent with contact information.

## 2. Resume semantics

- Each SSE event's `id:` field is a JSON array of `{topic, partition, timestamp | offset}` objects, one per upstream Kafka topic. Observed (FACT):
  ```json
  [{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1793029347980},
   {"topic":"codfw.mediawiki.recentchange","partition":0,"offset":-1}]
  ```
  Upstream is **multi-datacenter** (`eqiad`, `codfw`), and each DC topic has **one partition** (FACT, observed). Wikimedia uses *timestamps* rather than offsets in IDs because "Kafka offsets are unique per cluster" (FACT).
- Resume works by sending that array back as the `Last-Event-ID` header. Alternatively, `?since=<ISO-8601 or epoch ms>` starts from a point in time (FACT).
- Retention: "between 7 and 31 days of history" depending on the stream (FACT). An outage longer than retention means **unrecoverable upstream data loss**.
- **ASSUMPTION → EXPERIMENT (Phase 2):** A timestamp-based resume uses Kafka's offsets-for-times lookup, which returns the *first* offset with timestamp ≥ T. Records sharing the boundary millisecond are therefore re-delivered, so a resume yields **at-least-once with a small overlap**. We will measure the overlap per reconnect.

## 3. Schema of `mediawiki.recentchange`

Published schema: `/mediawiki/recentchange/1.0.1` (latest), JSON Schema draft-07, `additionalProperties: true` ([schema][rcschema]).

**Required fields are only `$schema`, `meta`, `meta.dt` and `meta.stream`.** Everything else is optional per the contract (FACT). The application must not assume the presence of fields that the contract doesn't guarantee, even though they appeared in 100% of our sample.

| Field | Meaning (per schema) | Observed presence | Notes |
|---|---|---|---|
| `meta.id` | "Unique ID of this event" (UUID string) | 100% | **Our dedup key.** 0 duplicates in 5,598 events |
| `meta.dt` | "UTC event datetime, ISO-8601" | 100% (required) | In practice the time EventBus *emitted* the event, 30 ms–50 s after `timestamp` (see §5) |
| `meta.domain` | Domain, e.g. `de.wikipedia.org`; `canary` for synthetic events | 100% | Canary events must be discarded |
| `meta.request_id`, `meta.uri`, `meta.stream` | request ID, canonical URI, stream name | 100% | |
| `meta.topic/partition/offset` | Upstream Kafka coordinates | 100% | **Not in the schema.** Observed only |
| `timestamp` | "Unix timestamp (derived from rc_timestamp)", **seconds** | 100% | The MediaWiki change time. **Our event time** ([ADR-0006](../adr/0006-event-time-and-watermarks.md)) |
| `type` | `edit`, `new`, `log`, `categorize`, `external` (or a number) | 100% | See §4: most events are not edits |
| `id` | rcid, integer **or null** | 98.4% | null on 92 of 265 `log` events. Unique per wiki, not globally |
| `wiki` | DB name, e.g. `enwiki`, `commonswiki` | 100% | Wiki identity |
| `server_name` | e.g. `en.wikipedia.org` | 100% | Project family and language subdomain |
| `title`, `namespace` | Full prefixed title, namespace ID | 100% | **There is no `page_id` field.** See §6 |
| `bot` | `rc_bot`: the change was flagged as a bot edit | 100% | Human/bot dimension |
| `user` | user name or IP/temporary account | 100% | |
| `revision.{old,new}`, `length.{old,new}` | revision IDs, byte lengths | 42.7% (edit/new only) | Schema descriptions look **swapped** (`new` says "rc_last_oldid"). Data shows `revision.new > revision.old` in 2086/2086 edits, so `new` is the new revision. For `type=new`, `old` is null |
| `minor`, `patrolled` | flags | 42.7%, 30% | `patrolled` exists only where patrolling is enabled |
| `log_*` | log metadata | 4.7% (log only) | `log_params` is polymorphic: array, object or string |
| `title_url`, `notify_url` | URLs | 100%, 95.3% | **Not in the schema.** Undocumented additions |

**Schema evolution evidence (FACT).** Every observed event declares `$schema: /mediawiki/recentchange/1.0.0` while the repository's latest is `1.0.1`. Events carry five fields absent from the schema (`title_url`, `notify_url`, `meta.topic`, `meta.partition`, `meta.offset`). Upstream drift is therefore already happening, and our parser must **ignore unknown fields** and **tolerate missing optional ones**.

## 4. What the stream contains (2-minute sample, Mon 2026-10-26 ~15:42 UTC)

| Metric | Value | Label |
|---|---|---|
| Event rate (all types) | 46.7 events/s | FACT (one sample; varies by time of day → EXPERIMENT) |
| `categorize` | 52.5% | FACT |
| `edit` / `new` / `log` | 37.3% / 5.5% / 4.7% | FACT |
| Edit rate (`edit`+`new`) | ~19.9 edits/s | FACT |
| Bot share of edits (`bot=true`) | 51.9% | FACT |
| Share of edits on `*.wikipedia.org` | 34.2% | FACT. Commons, Wikidata, Wiktionary and Wikisource dominate |
| Distinct wikis | 93 | FACT |
| Distinct pages among 2,392 edits | 2,162 | FACT. Extremely high cardinality |
| Avg raw event size | 1,356 bytes | FACT |
| zstd compression ratio (batches of 50 / 500) | 6.5× / 8.7× | FACT. Kafka batches are closer to the 50 case |
| Canary events | 0 in 2 minutes | FACT. Documented to occur "several times an hour" |

**Corrections to the original specification:**
1. **"Wikipedia's live edit stream"** actually covers *all Wikimedia projects*. Only about a third of edits are Wikipedia. We keep all projects and expose a `project_family` dimension.
2. **"Edits"** must be defined: `type ∈ {edit, new}`. `categorize` events are side effects (a page was added to a category) and would more than double "edit" counts if included naively.
3. **"Top edited pages"** is dominated by Wikidata bot activity (the top 5 pages in the sample were all on Wikidata). The product needs explicit filters (content namespaces, human-only, project family).

## 5. Timestamps and out-of-orderness

Observed (FACT, raw values, local clock *not* corrected):

| Measurement | p50 | p95 | p99 | p99.9 | max |
|---|---|---|---|---|---|
| `meta.dt − timestamp` | 1.6 s | 3.6 s | 6.8 s | 23.1 s | 50.8 s |
| local receive − `meta.dt` | −0.55 s | 1.4 s | 3.0 s | 3.8 s | 3.8 s |
| behind running max of `timestamp` (62% of events) | 1 s | 3 s | 8 s | 26 s | 50 s |

Interpretation:
- `meta.dt` is always later than `timestamp` (min +30 ms). `timestamp` is when the change was saved, and `meta.dt` is when the event was emitted. **`timestamp` is the semantically correct event time for "when did people edit"**, but it only has **1-second resolution**.
- In stream order, `timestamp` is out of order by up to ~50 s. `meta.dt` is nearly in arrival order.
- **Clock skew (FACT):** a *negative* median receive − `meta.dt` is physically impossible, so the local WSL2 clock was behind. An HTTP `Date` comparison suggested it was ~3–4 s behind real time (1 s resolution). Any latency computed as "our clock − Wikimedia's clock" therefore mixes pipeline delay with clock offset. This drives the latency definitions in ARCHITECTURE §17.
- **EXPERIMENT (Phase 2):** repeat over ≥24 h (covering peak and quiet hours and reconnects) before fixing watermark parameters. Two minutes are not enough to estimate a p99.99 tail.

## 6. Page identity: `recentchange` vs `mediawiki.page_change.v1`

The specification assumes "page identifiers". `recentchange` has **no `page_id`** (FACT). The newer public stream `mediawiki.page_change.v1` (schema `/mediawiki/page/change/1.12.0`) does have one:

| Aspect | `mediawiki.recentchange` | `mediawiki.page_change.v1` |
|---|---|---|
| Page identity | `(wiki, title)`. The title changes on page moves | `page.page_id` (stable), plus Wikidata item ID |
| Bot signal | `bot` = edit was bot-flagged (rc_bot) | `performer.is_bot` = the user is in the bot group (different semantics) |
| Event time | `timestamp` (s) / `meta.dt` (ms) | `dt` documented as event time, `meta.dt` as receive time |
| Event-time hazard | max observed skew ~50 s | **3.9% of events had `dt` from years ago** (`dt == revision.rev_dt`, back to 2010). Naive bounds checks would DLQ legitimate events |
| Contents | edits, creations, categorize, log | create/edit/move/delete/undelete/visibility |
| Rate / size | ~47 ev/s, ~1.4 KB | ~22 ev/s, ~3.3 KB |
| Schema strictness | `additionalProperties: true` | `additionalProperties: false`, more required fields |

Decision in [ADR-0001](../adr/0001-source-stream-selection.md): use `recentchange` for V1. A `page_change` enrichment join (by `rev_id`) is a stretch goal.

## 7. Open items carried into later phases

| ID | Item | Phase |
|---|---|---|
| R-1 | Measure duplicate overlap per reconnect/resume | 2 |
| R-2 | 24 h measurement of `timestamp` disorder (p99, p99.9, p99.99, max) and rate by hour | 2 |
| R-3 | Check how often `(wiki, id)` repeats under a different `meta.id` (duplicates that UUID dedup would miss) | 2 / 3 |
| R-4 | Measure the local clock offset properly (NTP or `chronyc`) and decide how to report it | 2 |
| R-5 | Observe canary events and confirm the filter | 2 |
| R-6 | Daily distinct page count, for state sizing | 3 |

[es]: https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams_HTTP_Service
[rcschema]: https://schema.wikimedia.org/repositories/primary/jsonschema/mediawiki/recentchange/latest.yaml
