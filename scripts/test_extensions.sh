#!/bin/bash
# Test script for extension loading

set -e

cd "$(dirname "$0")/.."

# Create a test config with json extension
cat > /tmp/ext-test-config.yaml <<EOF
host: "127.0.0.1"
port: 35433
data_dir: "./data"
tls:
  cert: "./certs/server.crt"
  key: "./certs/server.key"
users:
  testuser: "testpass"
extensions:
  - json
EOF

# Clean up any existing data for testuser
rm -rf ./data/testuser.db

# Kill any existing instances
pkill -f "duckgres.*35433" 2>/dev/null || true
sleep 1

echo "=== Starting server with json extension configured ==="
./duckgres --config /tmp/ext-test-config.yaml &
SERVER_PID=$!
sleep 3

echo ""
echo "=== Testing JSON extension functionality ==="
# The json extension adds functions like json_extract, json_valid, etc.
RESULT=$(PGPASSWORD=testpass psql "host=127.0.0.1 port=35433 user=testuser dbname=test sslmode=require" -t -c "SELECT json_extract('{\"name\": \"Alice\"}', '\$.name') as result;" 2>&1)
echo "Result: $RESULT"

if echo "$RESULT" | grep -q "Alice"; then
    echo "✓ JSON extension loaded and working!"
else
    echo "✗ JSON extension test failed"
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi

# Cleanup
kill $SERVER_PID 2>/dev/null || true
rm -f /tmp/ext-test-config.yaml

echo ""
echo "=== Extension loading test passed! ==="
