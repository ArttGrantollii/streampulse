"""Incremental parser for the Server-Sent Events wire format.

Implements the event-stream interpretation rules of the WHATWG HTML standard
(section "Server-sent events"): lines are `field: value`, a blank line dispatches
the buffered event, lines starting with `:` are comments, and the `id` field
sets a last-event-id that persists for all following events until changed.
The persisted id is what a client sends back as `Last-Event-ID` to resume.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SseEvent:
    data: str
    # Last event id in effect when this event was dispatched (may come from an earlier event).
    id: str | None
    event: str = "message"


class SseParser:
    """Feed lines one at a time (line terminators already removed); get complete events back."""

    def __init__(self, last_event_id: str | None = None) -> None:
        self.last_event_id = last_event_id
        self.retry_ms: int | None = None
        self._data: list[str] = []
        self._event_type = ""

    def feed(self, line: str) -> SseEvent | None:
        if line == "":
            return self._dispatch()
        if line.startswith(":"):
            return None  # comment, used by servers as keep-alive
        field, colon, value = line.partition(":")
        if colon and value.startswith(" "):
            value = value[1:]  # exactly one leading space is part of the syntax
        match field:
            case "data":
                self._data.append(value)
            case "event":
                self._event_type = value
            case "id":
                if "\0" not in value:
                    self.last_event_id = value
            case "retry":
                if value.isascii() and value.isdigit():
                    self.retry_ms = int(value)
            case _:
                pass  # unknown fields are ignored by the standard
        return None

    def _dispatch(self) -> SseEvent | None:
        if not self._data:
            self._event_type = ""
            return None
        event = SseEvent(
            data="\n".join(self._data),
            id=self.last_event_id,
            event=self._event_type or "message",
        )
        self._data = []
        self._event_type = ""
        return event
