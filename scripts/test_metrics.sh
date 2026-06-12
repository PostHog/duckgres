#!/bin/bash
set -e

PORT=35437
METRICS_PORT=9090

# Build
go build -o duckgres .

# Start server in background
./duckgres --port $PORT 2>&1 &
PID=$!
trap "kill $PID 2>/dev/null" EXIT

# Wait for server to be ready (check metrics endpoint)
for i in {1..10}; do
    if curl -s "http://localhost:$METRICS_PORT/metrics" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

# Run two queries
PGPASSWORD=postgres psql "host=127.0.0.1 port=$PORT user=postgres sslmode=require" -c "SELECT 1" >/dev/null
PGPASSWORD=postgres psql "host=127.0.0.1 port=$PORT user=postgres sslmode=require" -c "SELECT 2" >/dev/null

# Check metrics
METRICS=$(curl -s "http://localhost:$METRICS_PORT/metrics")
QUERY_COUNT=$(echo "$METRICS" | awk '/^duckgres_query_duration_seconds_count/ {sum += $2} END {print sum}')
QUERY_SUCCESS_TOTAL=$(echo "$METRICS" | awk '/^duckgres_query_total\{.*outcome="success"/ {sum += $2} END {print sum}')

if [ -z "$QUERY_COUNT" ]; then
    echo "FAIL: could not find 'duckgres_query_duration_seconds_count' metric in metrics output"
    exit 1
fi

if ! [[ "$QUERY_COUNT" =~ ^[0-9]+$ ]]; then
    echo "FAIL: query count '$QUERY_COUNT' is not a valid integer metric value"
    exit 1
fi
if [ "$QUERY_COUNT" -ge 2 ]; then
    echo "PASS: query count is $QUERY_COUNT (expected >= 2)"
else
    echo "FAIL: query count is $QUERY_COUNT (expected >= 2)"
    exit 1
fi

if [ -z "$QUERY_SUCCESS_TOTAL" ]; then
    echo "FAIL: could not find 'duckgres_query_total{outcome=\"success\"}' metric in metrics output"
    exit 1
fi

if ! [[ "$QUERY_SUCCESS_TOTAL" =~ ^[0-9]+$ ]]; then
    echo "FAIL: query success total '$QUERY_SUCCESS_TOTAL' is not a valid integer metric value"
    exit 1
fi
if [ "$QUERY_SUCCESS_TOTAL" -ge 2 ]; then
    echo "PASS: query success total is $QUERY_SUCCESS_TOTAL (expected >= 2)"
else
    echo "FAIL: query success total is $QUERY_SUCCESS_TOTAL (expected >= 2)"
    exit 1
fi
