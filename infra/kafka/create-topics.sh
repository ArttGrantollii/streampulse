#!/usr/bin/env bash
# Create StreamPulse topics and enforce their configuration. Safe to re-run:
# existing topics are not recreated, but their configs are re-applied so that
# manual drift is corrected. Topology rationale: docs/adr/0004-kafka-topic-topology.md
set -euo pipefail

BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-kafka:9092}"
KAFKA_BIN=/opt/kafka/bin

DAY_MS=86400000
GIB=1073741824

# name | partitions | retention.ms | retention.bytes (per partition)
TOPICS=(
  "wikimedia.recentchange.raw.v1|3|$((7 * DAY_MS))|$((2 * GIB))"
  "wikimedia.recentchange.dlq.v1|1|$((14 * DAY_MS))|$((GIB / 2))"
  "wikimedia.recentchange.late.v1|1|$((14 * DAY_MS))|$((GIB / 2))"
)

# Settings shared by every topic. Compression is done by producers (zstd);
# the broker keeps whatever codec the producer used.
COMMON_CONFIGS="cleanup.policy=delete,compression.type=producer,min.insync.replicas=1"

for spec in "${TOPICS[@]}"; do
  IFS='|' read -r name partitions retention_ms retention_bytes <<< "$spec"
  configs="${COMMON_CONFIGS},retention.ms=${retention_ms},retention.bytes=${retention_bytes}"

  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --create --if-not-exists --topic "$name" \
    --partitions "$partitions" --replication-factor 1

  "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --alter --entity-type topics --entity-name "$name" --add-config "$configs"

  actual=$("$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --describe --topic "$name" | grep -o 'PartitionCount: [0-9]*' | grep -o '[0-9]*')
  if [[ "$actual" != "$partitions" ]]; then
    # Partitions can be added but never removed, and adding them remaps keys,
    # so a mismatch needs a human decision rather than an automatic fix.
    echo "ERROR: topic $name has $actual partitions, expected $partitions" >&2
    exit 1
  fi
  echo "OK topic=$name partitions=$partitions retention.ms=$retention_ms retention.bytes=$retention_bytes"
done
