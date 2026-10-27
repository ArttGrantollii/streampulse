"""Capture raw Server-Sent Events from Wikimedia EventStreams for a fixed duration.

Phase 0 research tool. It records every event exactly as received (the SSE `id`
and the raw `data` payload) together with the local receive time, so that the
analysis script can measure field presence, timestamp skew and out-of-orderness
without trusting any parsing done at capture time.

Usage:
    python scripts/research/sample_eventstreams.py recentchange 120 rc.ndjson
    python scripts/research/sample_eventstreams.py mediawiki.page_change.v1 120 pc.ndjson

Output lines: {"recv_ms": <local epoch ms>, "sse_id": "<raw id>", "data": "<raw json>"}

Recordings are large (~5 MB/minute) and are intentionally not committed.
Only the standard library is used so the script runs anywhere.
"""

import json
import sys
import time
import urllib.request

USER_AGENT = (
    "StreamPulse-research/0.1 "
    "(https://github.com/ArttGrantollii/streampulse; "
    "175612905+ArttGrantollii@users.noreply.github.com)"
)


def main() -> None:
    stream, seconds, out_path = sys.argv[1], float(sys.argv[2]), sys.argv[3]
    request = urllib.request.Request(
        f"https://stream.wikimedia.org/v2/stream/{stream}",
        headers={"User-Agent": USER_AGENT, "Accept": "text/event-stream"},
    )
    deadline = time.time() + seconds
    count = 0
    event_id = None
    data_lines: list[str] = []
    with urllib.request.urlopen(request, timeout=30) as response, open(out_path, "w") as out:
        for raw_line in response:
            line = raw_line.decode("utf-8").rstrip("\n")
            if line.startswith("id:"):
                event_id = line[3:].strip()
            elif line.startswith("data:"):
                data_lines.append(line[5:].lstrip())
            elif line == "" and data_lines:  # a blank line terminates one SSE event
                record = {"recv_ms": int(time.time() * 1000), "sse_id": event_id, "data": "\n".join(data_lines)}
                out.write(json.dumps(record) + "\n")
                count += 1
                data_lines = []
                if time.time() > deadline:
                    break
    print(f"{stream}: captured {count} events -> {out_path}")


if __name__ == "__main__":
    main()
