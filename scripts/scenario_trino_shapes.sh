#!/usr/bin/env bash
# Print the scenario workflow matrix. Single-shape runs retain the original run
# ID; all-shape runs append distinct numeric suffixes for harness isolation.
set -euo pipefail

shape="${TRINO_PERF_SHAPE:-baseline}"
case "$shape" in
  baseline|large|scaleout|large-scaleout|all) ;;
  *)
    printf '%s\n' 'Invalid TRINO_PERF_SHAPE: expected baseline, large, scaleout, large-scaleout, or all' >&2
    exit 1
    ;;
esac

if [[ "$shape" != baseline && "${SCENARIO_NAME:-}" != posthog_frozen_perf ]]; then
  printf '%s\n' 'Trino shape experiments require posthog_frozen_perf' >&2
  exit 1
fi

if [[ "$shape" == all ]]; then
  printf '%s\n' '{"include":[{"shape":"baseline","suffix":"1"},{"shape":"large","suffix":"2"},{"shape":"scaleout","suffix":"3"},{"shape":"large-scaleout","suffix":"4"}]}'
else
  printf '{"include":[{"shape":"%s","suffix":""}]}\n' "$shape"
fi
