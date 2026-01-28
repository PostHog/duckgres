#!/bin/bash
# Generates continuous query load for testing metrics.
# Runs until Ctrl-C.

PORT=35437

# Build and start server
go build -o duckgres .
./duckgres --port $PORT &
PID=$!
trap "kill $PID 2>/dev/null; exit" INT TERM

# Wait for server
server_ready=0
for i in {1..10}; do
    if curl -s "http://localhost:9090/metrics" >/dev/null 2>&1; then
        server_ready=1
        break
    fi
    sleep 1
done

if [ "$server_ready" -ne 1 ]; then
    echo "Error: server did not start within timeout. Exiting." >&2
    kill "$PID" 2>/dev/null
    exit 1
fi
echo "Running queries every 2 seconds. Ctrl-C to stop."
while true; do
    PGPASSWORD=postgres psql "host=127.0.0.1 port=$PORT user=postgres sslmode=require" -c "SELECT 1" >/dev/null 2>&1
    echo "$(date +%H:%M:%S) - query sent"
    sleep 2
done
