import time

from streampulse_producer.app import Settings, run


def test_live_refuses_to_resume_from_beyond_max_age():
    settings = Settings(
        mode="live",
        bootstrap_servers="localhost:1",  # never contacted: the guard runs first
        topic="unused",
        user_agent="StreamPulse-test",
        since_ms=int(time.time() * 1000) - 3 * 24 * 3600 * 1000,
        max_resume_age_s=24 * 3600,
        metrics_port=None,
        ntp_server=None,
    )
    assert run(settings) == 2
