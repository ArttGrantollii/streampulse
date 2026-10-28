from pathlib import Path

import pytest
from prometheus_client import CollectorRegistry

from streampulse_producer.metrics import Metrics

FIXTURE = Path(__file__).resolve().parents[2] / "sample-data" / "fixtures" / "recentchange-120.ndjson"


@pytest.fixture
def registry() -> CollectorRegistry:
    return CollectorRegistry()


@pytest.fixture
def metrics(registry: CollectorRegistry) -> Metrics:
    return Metrics(registry)


@pytest.fixture
def fixture_path() -> Path:
    return FIXTURE
