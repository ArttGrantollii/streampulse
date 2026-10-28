# StreamPulse producer

Moves Wikimedia's live change stream into Kafka, or replays a recording through the same publishing path.

```bash
streampulse-producer --mode live                                   # Wikimedia -> Kafka, resuming from Kafka's last position
streampulse-producer --mode replay --file rec.ndjson --speed 10    # recording -> Kafka, 10x recorded speed
streampulse-producer --mode replay --file rec.ndjson --rate 50     # ... at 50 events/s
streampulse-producer --mode record --file rec.ndjson --duration 60 # Wikimedia -> file (no Kafka)
```

Design and guarantees: [docs/ARCHITECTURE.md §5](../docs/ARCHITECTURE.md#5-producer), [ADR-0012](../docs/adr/0012-producer-resume-from-kafka.md).

## Development

```bash
cd producer
uv sync                         # creates .venv from uv.lock
uv run pytest -m "not integration"
uv run pytest -m integration    # needs Kafka on localhost:9094 (`make up` at the repo root)
uv run ruff check . && uv run ruff format --check .
```
