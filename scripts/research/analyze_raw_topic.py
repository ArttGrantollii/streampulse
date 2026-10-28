"""Measure the source's real behaviour from the raw Kafka topic (Phase 2 experiments R-1..R-6).

The raw topic is the recording: each record carries the upstream SSE id, the ingest
time and the upstream connection that delivered it (sp-* headers). This reads the
topic from the start (or --since) up to its end at launch and reports:

  R-1  duplicates by cause: same connection, reconnect (same producer session),
       producer restart (different session); and how far apart they arrive
  R-2  event-time disorder per Kafka partition (what Flink's per-partition
       watermarks see): how far each event is behind the newest `timestamp` seen,
       for all events and for edits (edit/new) only; plus timestamps far in the
       past or in the future relative to ingest time, by event type
  R-3  (wiki, rcid) seen under more than one meta.id (duplicates UUID dedup misses)
  R-5  canary events
  R-6  distinct pages per UTC day (state sizing)
  plus rates per hour, bot share, connection lifetimes, and receive-minus-meta.dt
  measured only once a connection has caught up (after a restart the producer
  first replays its backlog, whose age is backlog, not latency).

Run with the producer's environment (it has confluent-kafka):
    cd producer && uv run python ../scripts/research/analyze_raw_topic.py [--since ISO] [--json out.json]
"""

import argparse
import json
import sys
from array import array
from collections import Counter, defaultdict
from datetime import UTC, datetime

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition


def pct(values, points=(50, 90, 95, 99, 99.9, 99.99, 100)):
    values = sorted(values)
    if not values:
        return {}
    return {p: values[min(len(values) - 1, int(len(values) * p / 100))] for p in points}


def iso_ms(value):
    try:
        return int(datetime.fromisoformat(value).timestamp() * 1000)
    except TypeError, ValueError:
        return None


def utc(ms):
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%d %H:%M:%S")


def id_key(meta_id: str) -> int:
    """64-bit key for a UUID (collision chance negligible at millions of events; saves memory)."""
    return int(meta_id.replace("-", "")[:16], 16)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bootstrap-servers", default="localhost:9094")
    parser.add_argument("--topic", default="wikimedia.recentchange.raw.v1")
    parser.add_argument("--since", help="only records ingested at/after this ISO time")
    parser.add_argument("--json", help="also write the results as JSON to this file")
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": "streampulse-research",
            "enable.auto.commit": False,
        }
    )
    partitions = sorted(consumer.list_topics(args.topic, timeout=10).topics[args.topic].partitions)
    ends = {
        p: consumer.get_watermark_offsets(TopicPartition(args.topic, p), timeout=10)[1] for p in partitions
    }
    start = [TopicPartition(args.topic, p, OFFSET_BEGINNING) for p in partitions]
    if args.since:
        since_ms = iso_ms(args.since)
        start = consumer.offsets_for_times(
            [TopicPartition(args.topic, p, since_ms) for p in partitions], timeout=10
        )
    consumer.assign(start)
    remaining = {tp.partition for tp in start if tp.offset >= 0 and tp.offset < ends[tp.partition]}
    if args.since is None:
        remaining = {p for p in partitions if ends[p] > 0}

    total = unparseable = canary = 0
    first_seen: dict[int, tuple[int, int]] = {}  # id -> (connection index, ingest ms)
    connections: dict[str, int] = {}
    connection_span: dict[int, list[int]] = {}
    dup_by_cause: Counter = Counter()
    dup_gap_ms = array("q")
    behind = array("q")
    behind_edits = array("q")
    newest_ts = defaultdict(int)  # per Kafka partition, all events
    newest_edit_ts = defaultdict(int)  # per Kafka partition, edit/new only
    far_past_by_type: Counter = Counter()  # timestamp > 31 days before ingest
    future_by_type: Counter = Counter()  # timestamp > 5 minutes after ingest
    caught_up: set[int] = set()  # connections whose events have become fresh
    rcid_ids: dict[tuple[str, int], int] = {}
    rcid_conflicts = 0
    per_hour: Counter = Counter()
    edits_per_hour: Counter = Counter()
    bot_edits = edits = 0
    types: Counter = Counter()
    pages_per_day: dict[str, set] = defaultdict(set)
    recv_minus_dt = array("q")
    canary_examples = []

    while remaining:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            print("kafka error:", message.error(), file=sys.stderr)
            continue
        partition = message.partition()
        if message.offset() >= ends[partition] - 1:
            remaining.discard(partition)
        if message.offset() >= ends[partition]:
            continue
        headers = dict(message.headers() or [])
        ingest_ms = int(headers.get("sp-ingest-ts", b"0"))
        connection = headers.get("sp-connection", b"?").decode()
        total += 1
        per_hour[utc(ingest_ms)[:13]] += 1

        conn_index = connections.setdefault(connection, len(connections))
        span = connection_span.setdefault(conn_index, [ingest_ms, ingest_ms])
        span[0], span[1] = min(span[0], ingest_ms), max(span[1], ingest_ms)

        try:
            event = json.loads(message.value())
            meta = event["meta"]
        except ValueError, KeyError, TypeError:
            unparseable += 1
            continue

        if meta.get("domain") == "canary":
            canary += 1
            if len(canary_examples) < 3:
                canary_examples.append({"meta": meta, "type": event.get("type"), "wiki": event.get("wiki")})

        key = id_key(meta["id"])
        if key in first_seen:
            first_conn, first_ingest = first_seen[key]
            first_name = next(n for n, i in connections.items() if i == first_conn)
            same_session = first_name.rsplit("-", 1)[0] == connection.rsplit("-", 1)[0]
            cause = (
                "same_connection" if first_conn == conn_index else "reconnect" if same_session else "restart"
            )
            dup_by_cause[cause] += 1
            dup_gap_ms.append(ingest_ms - first_ingest)
            continue  # analyse each event once
        first_seen[key] = (conn_index, ingest_ms)

        types[event.get("type")] += 1
        if isinstance(event.get("id"), int) and isinstance(event.get("wiki"), str):
            rcid = (event["wiki"], event["id"])
            if rcid in rcid_ids and rcid_ids[rcid] != key:
                rcid_conflicts += 1
            rcid_ids.setdefault(rcid, key)

        timestamp = event.get("timestamp")
        is_edit = event.get("type") in ("edit", "new")
        if isinstance(timestamp, int):
            ts_ms = timestamp * 1000
            if ts_ms < newest_ts[partition]:
                behind.append(newest_ts[partition] - ts_ms)
            newest_ts[partition] = max(newest_ts[partition], ts_ms)
            if is_edit:
                if ts_ms < newest_edit_ts[partition]:
                    behind_edits.append(newest_edit_ts[partition] - ts_ms)
                newest_edit_ts[partition] = max(newest_edit_ts[partition], ts_ms)
            if ts_ms < ingest_ms - 31 * 86_400_000:
                far_past_by_type[event.get("type")] += 1
            elif ts_ms > ingest_ms + 300_000:
                future_by_type[event.get("type")] += 1
        dt_ms = iso_ms(meta.get("dt"))
        if dt_ms is not None and headers.get("sp-mode") == b"live":
            delay_ms = ingest_ms - dt_ms
            if conn_index not in caught_up and delay_ms < 10_000:
                caught_up.add(conn_index)
            if conn_index in caught_up:
                recv_minus_dt.append(delay_ms)

        if is_edit:
            edits += 1
            bot_edits += bool(event.get("bot"))
            edits_per_hour[utc(ingest_ms)[:13]] += 1
            if isinstance(event.get("wiki"), str) and isinstance(event.get("title"), str):
                pages_per_day[utc(ingest_ms)[:10]].add(hash((event["wiki"], event["title"])))
    consumer.close()

    unique = len(first_seen)
    lifetimes_s = [round((s[1] - s[0]) / 1000) for s in connection_span.values()]
    sessions = {name.rsplit("-", 1)[0] for name in connections}
    analysed = unique + sum(dup_by_cause.values())
    results = {
        "window": {
            "first_ingest": utc(min(s[0] for s in connection_span.values())) if connection_span else None,
            "last_ingest": utc(max(s[1] for s in connection_span.values())) if connection_span else None,
            "records": total,
            "unique_events": unique,
            "unparseable": unparseable,
        },
        "R1_duplicates": {
            "total": sum(dup_by_cause.values()),
            "share_of_records": round(sum(dup_by_cause.values()) / max(1, analysed), 5),
            "by_cause": dict(dup_by_cause),
            "arrival_gap_ms_percentiles": pct(dup_gap_ms),
        },
        "R2_event_time_disorder_per_partition_ms": {
            "events_behind_newest": len(behind),
            "share_behind": round(len(behind) / max(1, unique), 4),
            "percentiles_of_behind": pct(behind),
            "edits_only": {
                "events_behind_newest": len(behind_edits),
                "share_behind": round(len(behind_edits) / max(1, edits), 4),
                "percentiles_of_behind": pct(behind_edits),
            },
            "timestamp_more_than_31d_before_ingest_by_type": dict(far_past_by_type),
            "timestamp_more_than_5min_after_ingest_by_type": dict(future_by_type),
            "note": "1 s resolution: `timestamp` is whole seconds",
        },
        "R3_rcid_under_different_meta_id": rcid_conflicts,
        "R5_canary_events": {"count": canary, "examples": canary_examples},
        "R6_distinct_pages_per_utc_day": {day: len(pages) for day, pages in sorted(pages_per_day.items())},
        "receive_minus_meta_dt_ms_percentiles_after_catch_up": pct(recv_minus_dt),
        "connections": {
            "count": len(connections),
            "producer_sessions": len(sessions),
            "lifetime_s_percentiles": pct(lifetimes_s),
        },
        "rates": {
            "events_per_hour": dict(sorted(per_hour.items())),
            "edits_per_hour": dict(sorted(edits_per_hour.items())),
            "bot_share_of_edits": round(bot_edits / max(1, edits), 4),
            "types": dict(types.most_common()),
        },
    }
    print(json.dumps(results, indent=2, default=str))
    if args.json:
        with open(args.json, "w", encoding="utf-8") as out:
            json.dump(results, out, indent=2, default=str)
    return 0


if __name__ == "__main__":
    sys.exit(main())
