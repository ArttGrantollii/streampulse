"""Analyse a recording produced by sample_eventstreams.py.

Prints the evidence used in docs/research/0001-eventstreams-source-validation.md:
field presence, duplicate IDs, canary events, change types, bot share,
timestamp skew between source fields and local receive time, and how far
events arrive behind the running maximum event time (out-of-orderness).

Usage:
    python scripts/research/analyze_sample.py rc.ndjson          # mediawiki.recentchange
    python scripts/research/analyze_sample.py pc.ndjson --page-change

Note: "recv - source" skews include the offset between the local clock and
Wikimedia's clocks. Check the local clock before trusting absolute values.
"""

import json
import sys
from collections import Counter
from datetime import datetime


def iso_to_ms(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def percentiles(values: list[int], points=(50, 95, 99, 99.9, 100)) -> dict:
    values = sorted(values)
    if not values:
        return {}
    return {p: values[min(len(values) - 1, int(len(values) * p / 100))] for p in points}


def behind_running_max(event_times_ms: list[int]) -> list[int]:
    """For each event older than the newest event seen so far, how far behind it is."""
    newest, behind = 0, []
    for t in event_times_ms:
        if t < newest:
            behind.append(newest - t)
        newest = max(newest, t)
    return behind


def main() -> None:
    path, page_change = sys.argv[1], "--page-change" in sys.argv
    rows = [json.loads(line) for line in open(path)]
    events, unparseable = [], 0
    for row in rows:
        try:
            events.append((row, json.loads(row["data"])))
        except json.JSONDecodeError:
            unparseable += 1
    n = len(events)
    print(f"events={n} unparseable={unparseable} first_sse_id={rows[0]['sse_id']}")

    keys = Counter()
    for _, e in events:
        keys.update(e.keys())
    print("top-level field presence:", {k: round(v / n, 3) for k, v in keys.most_common()})
    ids = Counter(e["meta"].get("id") for _, e in events)
    print("duplicate meta.id:", sum(c - 1 for c in ids.values() if c > 1))
    print("canary events:", sum(1 for _, e in events if e["meta"].get("domain") == "canary"))
    print("declared $schema:", Counter(e.get("$schema") for _, e in events))

    if page_change:
        print("page_change_kind:", Counter(e.get("page_change_kind") for _, e in events))
        print("performer.is_bot:", Counter((e.get("performer") or {}).get("is_bot") for _, e in events))
        print("recv - dt (ms):", percentiles([r["recv_ms"] - iso_to_ms(e["dt"]) for r, e in events]))
        print("recv - meta.dt (ms):", percentiles([r["recv_ms"] - iso_to_ms(e["meta"]["dt"]) for r, e in events]))
        event_times = [iso_to_ms(e["dt"]) for _, e in events]
    else:
        print("type:", Counter(e.get("type") for _, e in events))
        edits = [e for _, e in events if e.get("type") in ("edit", "new")]
        print(f"edit+new={len(edits)} ({len(edits) / n:.1%}); bot share of edits={sum(e['bot'] for e in edits) / len(edits):.1%}")
        print("distinct (wiki,title) among edits:", len({(e["wiki"], e["title"]) for e in edits}))
        print("wikipedia share of edits:", round(sum(e["server_name"].endswith(".wikipedia.org") for e in edits) / len(edits), 3))
        print("null rcid by type:", Counter(e["type"] for _, e in events if e.get("id") is None))
        timed = [(r, e) for r, e in events if isinstance(e.get("timestamp"), int)]
        print("recv - meta.dt (ms):", percentiles([r["recv_ms"] - iso_to_ms(e["meta"]["dt"]) for r, e in events]))
        print("recv - timestamp (ms):", percentiles([r["recv_ms"] - e["timestamp"] * 1000 for r, e in timed]))
        print("meta.dt - timestamp (ms):", percentiles([iso_to_ms(e["meta"]["dt"]) - e["timestamp"] * 1000 for _, e in timed]))
        event_times = [e["timestamp"] * 1000 for _, e in timed]

    behind = behind_running_max(event_times)
    print(f"behind running max event time: {len(behind)} events ({len(behind) / len(event_times):.1%}), ms:", percentiles(behind))


if __name__ == "__main__":
    main()
