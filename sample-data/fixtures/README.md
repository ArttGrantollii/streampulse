# Test fixtures

Small recordings committed for deterministic tests. Large recordings stay out of Git (`*.ndjson` is gitignored except in this directory).

| File | Contents | Captured |
|---|---|---|
| `recentchange-120.ndjson` | 120 consecutive live events from `https://stream.wikimedia.org/v2/stream/recentchange` (51 edit, 7 new, 52 categorize, 10 log; 113 distinct pages; 3.5 s span) | 2026-10-28, with `scripts/research/sample_eventstreams.py` |

**Format:** the recording format of [`producer/src/streampulse_producer/recording.py`](../../producer/src/streampulse_producer/recording.py). One JSON object per line: `{"recv_ms", "sse_id", "data"}`, where `data` is the raw SSE payload string.

**Attribution:** the event data comes from Wikimedia projects via [Wikimedia EventStreams](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams_HTTP_Service). Text content of Wikimedia projects is available under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/). It is used here unmodified, as test data.
