from streampulse_producer.sse import SseEvent, SseParser


def feed_all(parser: SseParser, lines: list[str]) -> list[SseEvent]:
    return [event for line in lines if (event := parser.feed(line)) is not None]


def test_single_event_with_id():
    events = feed_all(SseParser(), ["event: message", 'id: [{"t":1}]', 'data: {"a": 1}', ""])
    assert events == [SseEvent(data='{"a": 1}', id='[{"t":1}]', event="message")]


def test_multiline_data_is_joined_with_newlines():
    events = feed_all(SseParser(), ["data: first", "data: second", ""])
    assert events[0].data == "first\nsecond"


def test_comments_and_keepalives_produce_nothing():
    assert feed_all(SseParser(), [":ok", ": keep-alive", ""]) == []


def test_id_persists_for_following_events_until_changed():
    events = feed_all(SseParser(), ["id: 1", "data: a", "", "data: b", "", "id: 2", "data: c", ""])
    assert [(e.data, e.id) for e in events] == [("a", "1"), ("b", "1"), ("c", "2")]


def test_last_event_id_seeds_the_parser():
    events = feed_all(SseParser(last_event_id="resume-here"), ["data: x", ""])
    assert events[0].id == "resume-here"


def test_only_one_leading_space_is_stripped():
    events = feed_all(SseParser(), ["data:  two spaces", "data:none", ""])
    assert events[0].data == " two spaces\nnone"


def test_field_without_colon_is_a_field_with_empty_value():
    events = feed_all(SseParser(), ["data", ""])
    assert events[0].data == ""


def test_blank_line_without_data_does_not_dispatch_and_resets_event_type():
    parser = SseParser()
    assert feed_all(parser, ["event: error", ""]) == []
    assert feed_all(parser, ["data: x", ""])[0].event == "message"


def test_custom_event_type_is_reported():
    assert feed_all(SseParser(), ["event: error", "data: boom", ""])[0].event == "error"


def test_retry_field_sets_reconnect_hint_and_ignores_garbage():
    parser = SseParser()
    feed_all(parser, ["retry: 3000", "retry: soon"])
    assert parser.retry_ms == 3000


def test_id_containing_null_is_ignored():
    events = feed_all(SseParser(last_event_id="keep"), ["id: bad\0id", "data: x", ""])
    assert events[0].id == "keep"


def test_data_containing_colons_is_preserved():
    events = feed_all(SseParser(), ['data: {"uri": "https://x.org/a:b"}', ""])
    assert events[0].data == '{"uri": "https://x.org/a:b"}'
