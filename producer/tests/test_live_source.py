"""LiveSource against a scripted fake upstream (httpx.MockTransport): no network needed."""

import threading
from collections.abc import Callable

import httpx
import pytest

from streampulse_producer.backoff import Backoff
from streampulse_producer.live import LiveSource, ResumeMismatchError


def sse(*events: tuple[str, str]) -> bytes:
    lines = [":ok", ""]
    for event_id, data in events:
        lines += ["event: message", f"id: {event_id}", f"data: {data}", ""]
    return ("\n".join(lines) + "\n").encode()


class ScriptedUpstream:
    """Each request gets the next scripted response; the stop event is set after the last one."""

    def __init__(self, responses: list[Callable[[httpx.Request], httpx.Response]], stop: threading.Event):
        self.responses = responses
        self.requests: list[httpx.Request] = []
        self.stop = stop

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        response = self.responses[len(self.requests) - 1](request)
        if len(self.requests) == len(self.responses):
            self.stop.set()
        return response


class ZeroBackoff(Backoff):
    def __init__(self) -> None:
        super().__init__()
        self.delays_requested = 0

    def next_delay(self) -> float:
        self.delays_requested += 1
        return 0.0


def make_source(metrics, upstream: ScriptedUpstream, stop: threading.Event, **kwargs) -> LiveSource:
    client = httpx.Client(transport=httpx.MockTransport(upstream))
    return LiveSource(
        client,
        metrics,
        stop,
        url="https://stream.example/v2/stream/recentchange",
        user_agent="StreamPulse-test",
        **kwargs,
    )


def ok(body: bytes) -> Callable[[httpx.Request], httpx.Response]:
    return lambda _request: httpx.Response(200, content=body, headers={"content-type": "text/event-stream"})


def test_reconnect_resumes_with_last_event_id_after_flushing(metrics, registry):
    stop = threading.Event()
    flushes = []
    upstream = ScriptedUpstream([ok(sse(("id-1", "{}"), ("id-2", "{}"))), ok(sse(("id-3", "{}")))], stop)
    source = make_source(
        metrics,
        upstream,
        stop,
        backoff=ZeroBackoff(),
        before_reconnect=lambda: flushes.append(source.last_event_id),
    )

    events = list(source.events())

    assert [e.sse_id for e in events] == ["id-1", "id-2", "id-3"]
    assert "last-event-id" not in upstream.requests[0].headers
    assert upstream.requests[1].headers["last-event-id"] == "id-2"
    assert flushes == ["id-2"]  # flushed before resuming from id-2
    assert {e.connection.rsplit("-", 1)[1] for e in events} == {"1", "2"}
    assert registry.get_sample_value("streampulse_producer_reconnects_total", {"cause": "server_closed"}) == 1


def test_user_agent_and_accept_headers_are_sent(metrics):
    stop = threading.Event()
    upstream = ScriptedUpstream([ok(sse(("id-1", "{}")))], stop)
    list(make_source(metrics, upstream, stop).events())
    assert upstream.requests[0].headers["user-agent"] == "StreamPulse-test"
    assert upstream.requests[0].headers["accept"] == "text/event-stream"


def test_http_errors_back_off_and_are_counted_by_status(metrics, registry):
    stop = threading.Event()
    backoff = ZeroBackoff()
    upstream = ScriptedUpstream(
        [lambda _r: httpx.Response(503), lambda _r: httpx.Response(429), ok(sse(("id-1", "{}")))], stop
    )
    events = list(make_source(metrics, upstream, stop, backoff=backoff).events())
    assert [e.sse_id for e in events] == ["id-1"]
    assert backoff.delays_requested == 2
    assert registry.get_sample_value("streampulse_producer_reconnects_total", {"cause": "http_503"}) == 1
    assert registry.get_sample_value("streampulse_producer_reconnects_total", {"cause": "http_429"}) == 1


def test_network_errors_are_retried(metrics, registry):
    stop = threading.Event()

    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    upstream = ScriptedUpstream([refuse, ok(sse(("id-1", "{}")))], stop)
    events = list(make_source(metrics, upstream, stop, backoff=ZeroBackoff()).events())
    assert [e.sse_id for e in events] == ["id-1"]
    assert registry.get_sample_value("streampulse_producer_reconnects_total", {"cause": "network_error"}) == 1


def test_non_message_events_are_not_published(metrics):
    stop = threading.Event()
    body = b'event: error\ndata: {"message": "boom"}\n\nevent: message\nid: id-1\ndata: {}\n\n'
    upstream = ScriptedUpstream([ok(body)], stop)
    assert [e.sse_id for e in make_source(metrics, upstream, stop).events()] == ["id-1"]


def test_long_healthy_connection_resets_backoff(metrics):
    stop = threading.Event()
    times = iter([0.0, 120.0, 120.0, 121.0])  # first connection lives 120 s
    backoff = ZeroBackoff()
    backoff.attempt = 5
    upstream = ScriptedUpstream([ok(sse(("id-1", "{}"))), ok(sse(("id-2", "{}")))], stop)
    source = make_source(metrics, upstream, stop, backoff=backoff, clock=lambda: next(times))
    list(source.events())
    assert backoff.attempt == 0
    assert backoff.delays_requested == 0  # routine reconnect: immediate


def sse_with_upstream_ts(timestamp_ms: int) -> bytes:
    event_id = f'[{{"topic":"codfw.mediawiki.recentchange","partition":0,"timestamp":{timestamp_ms}}}]'
    return sse((event_id, "{}"))


def test_since_is_sent_as_iso8601_not_epoch_ms(metrics):
    # Wikimedia silently ignores an epoch-ms `since` and streams from the oldest retained
    # event (measured 2026-10-28); ISO-8601 is honoured exactly.
    stop = threading.Event()
    upstream = ScriptedUpstream([ok(sse_with_upstream_ts(1_793_190_752_415))], stop)
    list(make_source(metrics, upstream, stop, since_ms=1_793_190_752_415).events())
    assert upstream.requests[0].url.params["since"] == "2026-10-28T12:32:32.415Z"


def test_first_event_far_before_since_stops_the_source(metrics):
    stop = threading.Event()
    eight_days_ms = 8 * 24 * 3600 * 1000
    upstream = ScriptedUpstream([ok(sse_with_upstream_ts(1_793_190_752_415 - eight_days_ms))], stop)
    source = make_source(metrics, upstream, stop, since_ms=1_793_190_752_415)
    with pytest.raises(ResumeMismatchError):
        list(source.events())


def test_first_event_slightly_before_since_is_accepted(metrics):
    stop = threading.Event()
    upstream = ScriptedUpstream([ok(sse_with_upstream_ts(1_793_190_752_415 - 1_000))], stop)
    assert len(list(make_source(metrics, upstream, stop, since_ms=1_793_190_752_415).events())) == 1
