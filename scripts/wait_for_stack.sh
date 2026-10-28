#!/usr/bin/env bash
# Wait until the StreamPulse stack is ready after `docker compose up -d`.
#
# Ready means: every long-running service reports `healthy`, and every one-shot
# init service has exited with code 0. `docker compose up --wait` is not used
# because it treats any exited container as a failure, including init containers
# that exited successfully, so its result depends on timing.
set -euo pipefail

TIMEOUT_S="${STACK_TIMEOUT_S:-180}"
LONG_RUNNING=(kafka postgres flink-jobmanager flink-taskmanager)
ONE_SHOT=(kafka-init db-migrate flink-data-init)

cd "$(dirname "$0")/.."

# Prints "<state>|<health>|<exit code>" for one service.
status_of() {
  docker compose ps -a --format '{{.Service}}|{{.State}}|{{.Health}}|{{.ExitCode}}' \
    | awk -F'|' -v s="$1" '$1 == s {print $2 "|" $3 "|" $4}'
}

deadline=$((SECONDS + TIMEOUT_S))
while true; do
  pending=()
  for service in "${ONE_SHOT[@]}"; do
    IFS='|' read -r state _ exit_code <<< "$(status_of "$service")"
    if [[ "$state" == exited && "$exit_code" != 0 ]]; then
      echo "FAIL: init service '$service' exited with code $exit_code. Its logs:" >&2
      docker compose logs --no-log-prefix "$service" | tail -30 >&2
      exit 1
    fi
    [[ "$state" == exited ]] || pending+=("$service")
  done
  for service in "${LONG_RUNNING[@]}"; do
    IFS='|' read -r _ health _ <<< "$(status_of "$service")"
    [[ "$health" == healthy ]] || pending+=("$service")
  done

  if ((${#pending[@]} == 0)); then
    echo "Stack ready: ${LONG_RUNNING[*]} healthy; ${ONE_SHOT[*]} completed."
    exit 0
  fi
  if ((SECONDS >= deadline)); then
    echo "FAIL: not ready after ${TIMEOUT_S}s; still waiting for: ${pending[*]}" >&2
    docker compose ps -a >&2
    exit 1
  fi
  sleep 3
done
