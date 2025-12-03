#!/bin/bash
# Test rate limiting functionality

set -e

echo "=== Testing Rate Limiting ==="

# Create a test config with aggressive rate limiting
cat > /tmp/test-ratelimit.yaml <<EOF
host: "127.0.0.1"
port: 35433
data_dir: "./data"
tls:
  cert: "./certs/server.crt"
  key: "./certs/server.key"
users:
  postgres: "postgres"
rate_limit:
  max_failed_attempts: 3
  failed_attempt_window: "1m"
  ban_duration: "10s"
  max_connections_per_ip: 5
EOF

# Kill any existing duckgres
pkill -f "duckgres.*35433" 2>/dev/null || true
sleep 1

# Start server
echo "Starting server with rate limiting config..."
./duckgres --config /tmp/test-ratelimit.yaml &
SERVER_PID=$!
sleep 2

cleanup() {
    kill $SERVER_PID 2>/dev/null || true
    rm -f /tmp/test-ratelimit.yaml
}
trap cleanup EXIT

PASS_COUNT=0
FAIL_COUNT=0

echo ""
echo "Test 1: Successful authentication should work"
if PGPASSWORD=postgres psql "host=127.0.0.1 port=35433 user=postgres dbname=test sslmode=require" -c "SELECT 'auth works' as result;" 2>&1 | grep -q "auth works"; then
    echo "  PASS: Successful auth works"
    ((PASS_COUNT++))
else
    echo "  FAIL: Successful auth failed"
    ((FAIL_COUNT++))
fi

echo ""
echo "Test 2: Failed auth attempts should be counted (3 attempts)"
for i in 1 2 3; do
    echo "  Attempt $i with wrong password..."
    PGPASSWORD=wrongpassword psql "host=127.0.0.1 port=35433 user=postgres dbname=test sslmode=require" -c "SELECT 1" 2>&1 | head -1 || true
done

echo ""
echo "Test 3: After 3 failures, connection should be rejected"
RESULT=$(PGPASSWORD=postgres psql "host=127.0.0.1 port=35433 user=postgres dbname=test sslmode=require" -c "SELECT 'should fail'" 2>&1 || true)
if echo "$RESULT" | grep -q "server closed the connection unexpectedly"; then
    echo "  PASS: Connection rejected after too many failures"
    ((PASS_COUNT++))
else
    echo "  FAIL: Connection was not rejected as expected"
    echo "  Result: $RESULT"
    ((FAIL_COUNT++))
fi

echo ""
echo "Test 4: Wait for ban to expire (10s) and try again"
echo "  Waiting 12 seconds for ban to expire..."
sleep 12

if PGPASSWORD=postgres psql "host=127.0.0.1 port=35433 user=postgres dbname=test sslmode=require" -c "SELECT 'unbanned' as result;" 2>&1 | grep -q "unbanned"; then
    echo "  PASS: Connection works after ban expires"
    ((PASS_COUNT++))
else
    echo "  FAIL: Connection still blocked after ban should have expired"
    ((FAIL_COUNT++))
fi

echo ""
echo "=== Rate Limiting Tests Complete ==="
echo "Results: $PASS_COUNT passed, $FAIL_COUNT failed"

if [ $FAIL_COUNT -gt 0 ]; then
    exit 1
fi
