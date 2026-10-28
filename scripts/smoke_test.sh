#!/usr/bin/env bash
# Phase 1 end-to-end smoke test.
#
# Proves that the pinned stack works together:
#   Kafka 4.2.2 -> Flink 2.2.1 (kafka connector 5.0.0-2.2) -> RocksDB keyed state
#   -> checkpoints on the flink-data volume -> offsets committed back to Kafka.
#
# Steps: recreate a smoke topic, produce N records, run SmokeJob, wait until it has
# counted exactly N and completed >= 2 checkpoints, check checkpoint files exist and
# that offsets were committed on checkpoint, cancel the job, confirm the checkpoint
# is retained. Prints PASS or FAIL (exit 1).
#
# Requires: the stack running (`make up`) and the job jar built (`make build-jobs`).
set -euo pipefail

N="${SMOKE_RECORDS:-500}"
TOPIC=streampulse.smoke.v1
GROUP=streampulse-smoke
JOB_CLASS=io.github.arttgrantollii.streampulse.smoke.SmokeJob
JAR=/opt/streampulse/jobs/streampulse-flink-jobs.jar
TIMEOUT_S="${SMOKE_TIMEOUT_S:-120}"

cd "$(dirname "$0")/.."
compose() { docker compose "$@"; }
kafka() { compose exec -T kafka /opt/kafka/bin/"$@" --bootstrap-server kafka:9092; }
jm() { compose exec -T flink-jobmanager "$@"; }
# The flink CLI inherits JVM flags from the image that export jdk.compiler, a module
# the image's JRE does not ship; drop only that harmless warning, keep all other stderr.
flink_cli() { jm flink "$@" 2> >(grep -v 'Unknown module: jdk.compiler' >&2); }
rest() { jm curl -sf "http://localhost:8081$1"; }
step() { printf '\n==> %s\n' "$*"; }
fail() { printf '\nFAIL: %s\n' "$*" >&2; exit 1; }

# Poll a command until it prints a value equal to $1, or time out.
wait_for_value() {
  local expected="$1" description="$2"; shift 2
  local deadline=$((SECONDS + TIMEOUT_S)) value=""
  while ((SECONDS < deadline)); do
    value="$("$@" 2>/dev/null || true)"
    [[ "$value" == "$expected" ]] && return 0
    sleep 2
  done
  fail "$description: expected '$expected', last saw '${value:-nothing}' after ${TIMEOUT_S}s"
}

step "Checking prerequisites"
jm test -f "$JAR" || fail "job jar not found in the JobManager; run 'make build-jobs' first"
rest /taskmanagers | grep -q '"id"' || fail "no TaskManager registered; is the stack up? ('make up')"

step "Recreating topic $TOPIC (3 partitions) and resetting group $GROUP"
kafka kafka-topics.sh --delete --if-exists --topic "$TOPIC" >/dev/null
kafka kafka-consumer-groups.sh --delete --group "$GROUP" >/dev/null 2>&1 || true
# Topic deletion is asynchronous; retry creation until the old topic is gone.
deadline=$((SECONDS + 30))
until kafka kafka-topics.sh --create --topic "$TOPIC" --partitions 3 --replication-factor 1 >/dev/null 2>&1; do
  ((SECONDS < deadline)) || fail "could not recreate $TOPIC"
  sleep 1
done

step "Producing $N records"
seq 1 "$N" | kafka kafka-console-producer.sh --topic "$TOPIC" >/dev/null
end_offsets_sum() {
  kafka kafka-get-offsets.sh --topic "$TOPIC" --time latest | awk -F: '{s += $3} END {print s}'
}
wait_for_value "$N" "records in topic" end_offsets_sum
echo "topic holds $N records across 3 partitions"

step "Submitting SmokeJob (checkpoint interval overridden to 5s for this job)"
since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
submit_output="$(flink_cli run -d -Dexecution.checkpointing.interval=5s -c "$JOB_CLASS" "$JAR" \
  --topic "$TOPIC" --group-id "$GROUP")"
job_id="$(grep -oE 'JobID [0-9a-f]{32}' <<< "$submit_output" | awk '{print $2}')"
[[ -n "$job_id" ]] || fail "could not parse JobID from: $submit_output"
echo "job id: $job_id"
trap 'flink_cli cancel "$job_id" >/dev/null 2>&1 || true' EXIT

latest_count() {
  compose logs flink-taskmanager --since "$since" --no-log-prefix \
    | grep -oE 'SMOKE_COUNT=[0-9]+' | tail -1 | cut -d= -f2
}
wait_for_value "$N" "records counted by the job" latest_count
echo "job counted exactly $N records"

completed_checkpoints_at_least_2() {
  local n
  n="$(rest "/jobs/$job_id/checkpoints" | grep -oE '"completed":[0-9]+' | head -1 | cut -d: -f2)"
  ((${n:-0} >= 2)) && echo yes
}
wait_for_value yes "completed checkpoints >= 2" completed_checkpoints_at_least_2
# The response is ordered counts, summary, latest, history, so the first external_path
# is latest.completed (history lists older checkpoints after it).
checkpoint_path="$(rest "/jobs/$job_id/checkpoints" | grep -oE '"external_path":"[^"]+"' | head -1 | cut -d'"' -f4)"
echo "latest completed checkpoint: $checkpoint_path"

step "Checking checkpoint files on the flink-data volume"
checkpoint_dir="${checkpoint_path#file:}"
jm test -f "$checkpoint_dir/_metadata" || fail "checkpoint metadata missing at $checkpoint_dir"
jm ls "/flink-data/checkpoints/$job_id"

step "Checking offsets were committed to Kafka on checkpoint"
committed_offsets_sum() {
  kafka kafka-consumer-groups.sh --describe --group "$GROUP" 2>/dev/null \
    | awk -v t="$TOPIC" '$2 == t {s += $4} END {print s + 0}'
}
wait_for_value "$N" "offsets committed by group $GROUP" committed_offsets_sum
echo "group $GROUP committed offsets summing to $N (lag 0)"

step "Cancelling the job; the checkpoint must be retained (RETAIN_ON_CANCELLATION)"
flink_cli cancel "$job_id" >/dev/null
trap - EXIT
job_state() { rest "/jobs/$job_id" | grep -oE '"state":"[A-Z]+"' | head -1 | cut -d'"' -f4; }
wait_for_value CANCELED "job state" job_state
jm test -f "$checkpoint_dir/_metadata" || fail "checkpoint was deleted on cancel"
echo "retained checkpoint: $checkpoint_path"

printf '\nPASS: Kafka -> Flink -> RocksDB state -> durable checkpoints verified (%s records)\n' "$N"
