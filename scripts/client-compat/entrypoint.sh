#!/bin/bash
# Wrapper entrypoint that tees stdout/stderr to LOG_DIR if set.
# Usage: entrypoint.sh <command> [args...]
set -eo pipefail

if [ -n "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
    exec "$@" > >(tee "$LOG_DIR/stdout.log") 2> >(tee "$LOG_DIR/stderr.log" >&2)
else
    exec "$@"
fi
