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
QUERY_COUNT=$(curl -s "http://localhost:$METRICS_PORT/metrics" | grep 'duckgres_query_duration_seconds_count' | awk '{print $2}')

if [ "$QUERY_COUNT" -ge 2 ]; then
    echo "PASS: query count is $QUERY_COUNT (expected >= 2)"
else
    echo "FAIL: query count is $QUERY_COUNT (expected >= 2)"
    exit 1
fi
