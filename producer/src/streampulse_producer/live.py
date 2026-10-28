"""Live source: Wikimedia EventStreams over SSE, with reconnect and resume.

Wikimedia closes every connection after about 15 minutes (Research 0001), so a
reconnect is normal operation, not an error. Resume rules:
- first connection: `?since=<ISO-8601>` if the raw topic gave us a position, else "now".
  The value must be ISO-8601: Wikimedia silently ignores a `since` it cannot parse
  (e.g. epoch milliseconds) and streams from the *oldest* retained event instead,
  days in the past (measured 2026-10-28). So we also check that the first event
  really is near the requested time and stop if it is not;
- every later connection: `Last-Event-ID` = the id of the last event we yielded.
  Before reconnecting we call `before_reconnect` (the publisher's flush), so that
  id refers to an event Kafka has acknowledged; if delivery failed, that raises
  and the producer stops instead of silently skipping events.
"""

import threading
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime

import httpx
import structlog

from streampulse_producer.backoff import Backoff
from streampulse_producer.events import RawEvent
from streampulse_producer.metrics import Metrics
from streampulse_producer.resume import upstream_timestamp_ms
from streampulse_producer.sse import SseParser

DEFAULT_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
# How much earlier than the requested `since` the first event may be before we assume
# the server ignored the parameter.
SINCE_TOLERANCE_MS = 5 * 60 * 1000


class ResumeMismatchError(RuntimeError):
    """Upstream did not start where we asked; continuing would ingest the wrong data."""


def since_param(since_ms: int) -> str:
    """Epoch ms as ISO-8601 UTC with milliseconds, the format Wikimedia's `since` understands."""
    return (
        datetime.fromtimestamp(since_ms / 1000, UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    )


class UpstreamStatusError(Exception):
    def __init__(self, status: int, retry_after_s: float | None) -> None:
        super().__init__(f"HTTP {status}")
        self.status = status
        self.retry_after_s = retry_after_s


def _retry_after_seconds(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After", "")
    return float(value) if value.isdigit() else None


class LiveSource:
    def __init__(
        self,
        client: httpx.Client,
        metrics: Metrics,
        stop: threading.Event,
        *,
        url: str = DEFAULT_URL,
        user_agent: str,
        since_ms: int | None = None,
        before_reconnect: Callable[[], None] = lambda: None,
        backoff: Backoff | None = None,
        healthy_connection_s: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client
        self._metrics = metrics
        self._stop = stop
        self._url = url
        self._user_agent = user_agent
        self._since_ms = since_ms
        self._before_reconnect = before_reconnect
        self._backoff = backoff or Backoff()
        self._healthy_connection_s = healthy_connection_s
        self._clock = clock
        self._log = structlog.get_logger(component="live-source")
        self._session = f"{int(time.time())}"  # distinguishes producer restarts in sp-connection
        self.last_event_id: str | None = None

    def events(self) -> Iterator[RawEvent]:
        connection_number = 0
        while not self._stop.is_set():
            connection_number += 1
            connection = f"live-{self._session}-{connection_number}"
            started = self._clock()
            received = 0
            retry_after_s = None
            try:
                for event in self._stream(connection):
                    received += 1
                    yield event
                    if self._stop.is_set():
                        return
                cause = "server_closed"
            except UpstreamStatusError as error:
                cause, retry_after_s = f"http_{error.status}", error.retry_after_s
            except httpx.TimeoutException:
                cause = "timeout"
            except httpx.TransportError as error:  # connection reset, DNS, protocol errors
                cause = "network_error"
                self._log.warning("upstream_transport_error", error=repr(error), error_category="network")

            if self._stop.is_set():
                return
            lived_s = self._clock() - started
            self._metrics.reconnects.labels(cause=cause).inc()
            # A connection that worked for a while resets the backoff: a routine 15-minute
            # close reconnects immediately; repeated quick failures back off exponentially.
            if lived_s >= self._healthy_connection_s and received:
                self._backoff.reset()
                delay = 0.0
            else:
                delay = self._backoff.next_delay()
            if retry_after_s is not None:
                delay = max(delay, retry_after_s)
            self._log.info(
                "upstream_reconnect",
                cause=cause,
                connection=connection,
                lived_s=round(lived_s, 1),
                events=received,
                delay_s=round(delay, 2),
                last_event_id=self.last_event_id,
            )
            self._before_reconnect()  # flush: the resume position must be acknowledged by Kafka
            if self._stop.wait(delay):
                return

    def _stream(self, connection: str) -> Iterator[RawEvent]:
        headers = {"User-Agent": self._user_agent, "Accept": "text/event-stream"}
        params = {}
        check_since_ms = None
        if self.last_event_id is not None:
            headers["Last-Event-ID"] = self.last_event_id
        elif self._since_ms is not None:
            params["since"] = since_param(self._since_ms)
            check_since_ms = self._since_ms

        with self._client.stream("GET", self._url, headers=headers, params=params) as response:
            if response.status_code != 200:
                raise UpstreamStatusError(response.status_code, _retry_after_seconds(response))
            self._log.info(
                "upstream_connected",
                connection=connection,
                resume_from=headers.get("Last-Event-ID", params.get("since", "now")),
            )
            parser = SseParser(self.last_event_id)
            for line in response.iter_lines():
                event = parser.feed(line)
                if event is None:
                    continue
                if event.event != "message":
                    self._log.warning(
                        "upstream_non_message_event", sse_event=event.event, data=event.data[:500]
                    )
                    continue
                if check_since_ms is not None:
                    self._check_resumed_at(check_since_ms, event.id)
                    check_since_ms = None
                self.last_event_id = event.id
                yield RawEvent(
                    data=event.data.encode("utf-8"),
                    sse_id=event.id,
                    received_at_ms=time.time_ns() // 1_000_000,
                    mode="live",
                    connection=connection,
                )

    def _check_resumed_at(self, since_ms: int, event_id: str | None) -> None:
        first_ms = upstream_timestamp_ms(event_id) if event_id else None
        if first_ms is None:
            return  # nothing to compare against; Last-Event-ID resumes are exact anyway
        if first_ms < since_ms - SINCE_TOLERANCE_MS:
            raise ResumeMismatchError(
                f"asked upstream to start at {since_param(since_ms)} but the first event is from "
                f"{since_param(first_ms)}; refusing to ingest data from the wrong time"
            )
