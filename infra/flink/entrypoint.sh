#!/usr/bin/env bash
# Load the repository's Flink configuration, then start the stock entrypoint.
#
# The flink image applies settings from the FLINK_PROPERTIES environment
# variable ("key: value" lines). It does not understand comments, so they are
# stripped here; everything else is passed through unchanged.
set -euo pipefail

CONFIG_FILE=/opt/streampulse/flink-config/flink-properties.yaml

FLINK_PROPERTIES="$(grep -vE '^[[:space:]]*(#|$)' "$CONFIG_FILE")${FLINK_PROPERTIES:+
${FLINK_PROPERTIES}}"
export FLINK_PROPERTIES

exec /docker-entrypoint.sh "$@"
