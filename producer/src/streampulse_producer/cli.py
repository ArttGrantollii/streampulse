"""Command line: `streampulse-producer --mode live|replay|record [...]`.

Every option can also be set through an environment variable (shown in --help),
so the container is configured from docker-compose.yml and .env.
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

from streampulse_producer.app import Settings, run
from streampulse_producer.log import configure_logging

DEFAULT_USER_AGENT = "StreamPulse/0.1 (https://github.com/ArttGrantollii/streampulse)"


def _env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(f"STREAMPULSE_{name}", default)


def _parse_since(value: str) -> int:
    """Epoch milliseconds, or an ISO-8601 timestamp such as 2026-10-28T12:00:00Z."""
    if value.isdigit():
        return int(value)
    return int(datetime.fromisoformat(value).timestamp() * 1000)


def _replay_pacing(rate: str | None, speed: float | None) -> tuple[float | None, float | None]:
    """(rate, speed) for ReplaySource. Default: the recording's own timing (speed 1.0)."""
    if rate == "max":
        return None, None
    if rate is not None:
        return float(rate), None
    return None, speed if speed is not None else 1.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="streampulse-producer", description=__doc__.splitlines()[0])
    parser.add_argument("--mode", choices=["live", "replay", "record"], default=_env("MODE", "live"))
    parser.add_argument(
        "--bootstrap-servers",
        default=_env("KAFKA_BOOTSTRAP", "localhost:9094"),
        help="env STREAMPULSE_KAFKA_BOOTSTRAP",
    )
    parser.add_argument(
        "--topic", default=_env("TOPIC", "wikimedia.recentchange.raw.v1"), help="env STREAMPULSE_TOPIC"
    )
    parser.add_argument(
        "--url",
        default=_env("SOURCE_URL", "https://stream.wikimedia.org/v2/stream/recentchange"),
        help="env STREAMPULSE_SOURCE_URL",
    )
    parser.add_argument(
        "--user-agent",
        default=_env("USER_AGENT", DEFAULT_USER_AGENT),
        help="identify the client to Wikimedia (env STREAMPULSE_USER_AGENT)",
    )
    parser.add_argument("--file", type=Path, help="recording to replay (replay) or write (record)")
    pacing = parser.add_mutually_exclusive_group()
    pacing.add_argument("--rate", help="replay: events per second, or 'max' for no pacing")
    pacing.add_argument("--speed", type=float, help="replay: multiple of the recorded timing (default 1.0)")
    parser.add_argument(
        "--since",
        type=_parse_since,
        help="live/record: start from this time instead of the position stored in Kafka",
    )
    parser.add_argument(
        "--resume-margin-ms",
        type=int,
        default=int(_env("RESUME_MARGIN_MS", "30000")),
        help="live: re-read this much before the stored position (duplicates are removed later)",
    )
    parser.add_argument(
        "--max-resume-age",
        type=float,
        default=float(_env("MAX_RESUME_AGE_S", "86400")),
        help="live: refuse to resume from more than this many seconds ago (env STREAMPULSE_MAX_RESUME_AGE_S)",
    )
    parser.add_argument("--max-events", type=int, help="stop after this many events")
    parser.add_argument("--duration", type=float, help="stop after this many seconds")
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=int(_env("METRICS_PORT", "8000")),
        help="Prometheus /metrics port; 0 disables (env STREAMPULSE_METRICS_PORT)",
    )
    parser.add_argument(
        "--ntp-server",
        default=_env("NTP_SERVER", "pool.ntp.org"),
        help="measure local clock offset at startup; 'none' disables",
    )
    parser.add_argument("--log-level", default=_env("LOG_LEVEL", "INFO"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.mode in ("replay", "record") and args.file is None:
        build_parser().error(f"--mode {args.mode} requires --file")
    try:
        rate, speed = _replay_pacing(args.rate, args.speed)
    except ValueError:
        build_parser().error(f"--rate must be a number or 'max', got {args.rate!r}")
    configure_logging(args.log_level)

    settings = Settings(
        mode=args.mode,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        user_agent=args.user_agent,
        url=args.url,
        file=args.file,
        rate=rate,
        speed=speed,
        since_ms=args.since,
        resume_margin_ms=args.resume_margin_ms,
        max_resume_age_s=args.max_resume_age,
        max_events=args.max_events,
        duration_s=args.duration,
        metrics_port=args.metrics_port or None,
        ntp_server=None if args.ntp_server == "none" else args.ntp_server,
    )
    return run(settings)


if __name__ == "__main__":
    sys.exit(main())
