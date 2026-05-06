#!/usr/bin/env bash
# Reproducer for the posthog-mw-prod-us pgwire-write-stall incident.
#
# Goal: drive a CP into the same kernel state seen during the incident — a
# long-running SELECT whose return path is silently dropped (NLB tore down
# the idle 350s+ connection while the CP was streaming results). Without the
# TCP_USER_TIMEOUT fix the CP blocks on the unacked write for ~15 minutes
# before the kernel collapses the socket, and the only log line emitted is
# Info-level "Query finished.". With the fix the CP fails the write in ~60s
# and emits Error-level "Query execution errored.".
#
# How the simulation works:
#   - socat is started as a transparent TCP middlebox between psql and the CP.
#   - psql connects to socat, socat connects to the CP.
#   - We run a streaming SELECT that produces rows continuously.
#   - Once results start flowing we SIGSTOP socat. socat's userspace stops
#     draining its receive buffer; once that buffer fills, socat's kernel
#     stops advertising window space; the CP's writes stall waiting for
#     window to open. This is the same kernel state an NLB-induced black
#     hole produces — a half-dead socket that still appears connected to
#     the CP's TCP stack but accepts no more data.
#
# Usage:
#   CP_HOST=cp.dev.example.com CP_PORT=5432 PGUSER=postgres PGPASSWORD=… \
#     ./scripts/repro_pgwire_write_stall.sh
#
# Then watch the CP's logs and confirm:
#   - Pre-fix: ~15 min of silence, then a single "Query finished." Info line.
#   - Post-fix: ~60 s, then "Query execution errored." at Error level with
#     SQLSTATE 57P03 (or class 08 connection_exception, depending on how the
#     kernel surfaces the ETIMEDOUT).
set -euo pipefail

CP_HOST="${CP_HOST:?CP_HOST required (e.g. cp.dev.example.com)}"
CP_PORT="${CP_PORT:-5432}"
LOCAL_PORT="${LOCAL_PORT:-25432}"
STREAM_ROWS="${STREAM_ROWS:-100000000}"
STALL_AFTER="${STALL_AFTER:-5}"   # seconds of streaming before SIGSTOPing socat

command -v socat >/dev/null || { echo "socat not found — brew install socat / apt install socat" >&2; exit 1; }
command -v psql >/dev/null || { echo "psql not found" >&2; exit 1; }

cleanup() {
  if [[ -n "${SOCAT_PID:-}" ]] && kill -0 "$SOCAT_PID" 2>/dev/null; then
    kill -CONT "$SOCAT_PID" 2>/dev/null || true
    kill "$SOCAT_PID" 2>/dev/null || true
  fi
  if [[ -n "${PSQL_PID:-}" ]] && kill -0 "$PSQL_PID" 2>/dev/null; then
    kill "$PSQL_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

echo "[$(date +%H:%M:%S)] starting socat middlebox: 127.0.0.1:$LOCAL_PORT -> $CP_HOST:$CP_PORT"
socat TCP-LISTEN:"$LOCAL_PORT",reuseaddr,fork TCP:"$CP_HOST":"$CP_PORT" &
SOCAT_PID=$!
sleep 1

# Streaming SELECT: deliberately CPU-light so the CP's row-emit loop is
# bottlenecked on socket writes, not on DuckDB. Each row is ~50 bytes so
# the kernel send buffer (default ~4 MB) fills in well under a second of
# stalled writes, so the CP starts blocking on Write() immediately after
# the SIGSTOP.
QUERY="SELECT range::BIGINT AS n, md5(range::TEXT) AS h FROM range(0, $STREAM_ROWS)"

echo "[$(date +%H:%M:%S)] starting psql streaming query (rows=$STREAM_ROWS)"
PGPASSWORD="${PGPASSWORD:-}" psql \
  "host=127.0.0.1 port=$LOCAL_PORT user=${PGUSER:-postgres} sslmode=disable dbname=${PGDATABASE:-postgres}" \
  -c "$QUERY" \
  >/dev/null 2>/tmp/repro_psql.err &
PSQL_PID=$!

sleep "$STALL_AFTER"
echo "[$(date +%H:%M:%S)] SIGSTOPing socat — return path is now black-holed"
kill -STOP "$SOCAT_PID"

echo "[$(date +%H:%M:%S)] socat stopped. Watch the CP logs now."
echo
echo "  kubectl -n duckgres logs -f -l app=duckgres-cp --tail=100 \\"
echo "      | grep -E 'Query (started|finished|execution errored)'"
echo
echo "Expected timeline:"
echo "  pre-fix : ~15 min until ETIMEDOUT, then Info-level 'Query finished.'"
echo "  post-fix: ~60 s until TCP_USER_TIMEOUT fires, then Error-level"
echo "            'Query execution errored.' with the wire-write error."
echo
echo "Press Ctrl-C to clean up and exit."
wait "$PSQL_PID" || true
