import pytest
from confluent_kafka import KafkaError, KafkaException

from streampulse_producer.kafka_position import with_retries


class Flaky:
    def __init__(self, errors: list[int]) -> None:
        self.errors = list(errors)
        self.calls = 0

    def __call__(self) -> str:
        self.calls += 1
        if self.errors:
            raise KafkaException(KafkaError(self.errors.pop(0)))
        return "ok"


def test_transient_destroy_broker_error_is_retried_with_backoff():
    # The exact error CI hit: bootstrap broker handle retired while a request was in flight.
    call, sleeps = Flaky([KafkaError._DESTROY_BROKER, KafkaError._TRANSPORT]), []
    assert with_retries(call, sleep=sleeps.append) == "ok"
    assert call.calls == 3
    assert sleeps == [0.5, 1.0]


def test_non_transient_error_is_raised_immediately():
    call = Flaky([KafkaError.UNKNOWN_TOPIC_OR_PART])
    with pytest.raises(KafkaException):
        with_retries(call, sleep=lambda _s: None)
    assert call.calls == 1


def test_gives_up_after_the_last_attempt():
    call = Flaky([KafkaError._DESTROY_BROKER] * 5)
    with pytest.raises(KafkaException):
        with_retries(call, attempts=3, sleep=lambda _s: None)
    assert call.calls == 3
