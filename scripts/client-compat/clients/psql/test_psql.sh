#!/bin/bash
# psql client compatibility test for duckgres.
#
# Runs shared queries from queries.yaml plus psql-specific features
# (backslash commands, COPY, etc.) and reports results to the gatherer.
set -eo pipefail

CLIENT="psql"
RESULTS_URL="${RESULTS_URL:-http://results-gatherer:8080}"
PGHOST="${PGHOST:-duckgres}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGPASSWORD="${PGPASSWORD:-postgres}"
export PGHOST PGPORT PGUSER PGPASSWORD PGSSLMODE=require

PASSED=0
FAILED=0

report() {
    local suite="$1" name="$2" status="$3" detail="$4"
    if [ "$status" = "pass" ]; then
        PASSED=$((PASSED + 1))
        printf "  PASS  %s" "$name"
        [ -n "$detail" ] && printf " (%s)" "$detail"
        printf "\n"
    else
        FAILED=$((FAILED + 1))
        printf "  FAIL  %s: %s\n" "$name" "$detail"
    fi
    curl -sf -X POST "$RESULTS_URL/result" \
        -H "Content-Type: application/json" \
        -d "{\"client\":\"$CLIENT\",\"suite\":\"$suite\",\"test_name\":\"$name\",\"status\":\"$status\",\"detail\":\"$detail\"}" \
        >/dev/null 2>&1 || true
}

done_report() {
    curl -sf -X POST "$RESULTS_URL/done" \
        -H "Content-Type: application/json" \
        -d "{\"client\":\"$CLIENT\"}" \
        >/dev/null 2>&1 || true
}

run_query() {
    local suite="$1" name="$2" sql="$3"
    local output
    if output=$(psql -X -A -t -c "$sql" 2>&1); then
        local rows
        rows=$(echo "$output" | grep -c '^' || true)
        report "$suite" "$name" "pass" "$rows rows"
    else
        report "$suite" "$name" "fail" "$output"
    fi
}

# Wait for duckgres
echo "Waiting for duckgres..."
for i in $(seq 1 30); do
    if psql -X -c "SELECT 1" >/dev/null 2>&1; then
        echo "Connected after $i attempt(s)."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "FAIL: Could not connect after 30 seconds"
        exit 1
    fi
    sleep 1
done

# --- Shared queries from YAML ---
echo ""
echo "=== Shared catalog queries ==="

# Parse YAML with awk (no pyyaml needed).
# Each record has suite, name, sql fields.
current_suite=""
current_name=""
current_sql=""
in_sql=0

while IFS= read -r line; do
    # New record
    if [[ "$line" =~ ^-\ suite:\ (.*) ]]; then
        # Flush previous record
        if [ -n "$current_name" ] && [ -n "$current_sql" ]; then
            run_query "$current_suite" "$current_name" "$current_sql"
        fi
        current_suite="${BASH_REMATCH[1]}"
        current_name=""
        current_sql=""
        in_sql=0
        continue
    fi

    if [[ "$line" =~ ^\ \ name:\ (.*) ]]; then
        current_name="${BASH_REMATCH[1]}"
        continue
    fi

    # sql: >-  (folded scalar, follows on next lines)
    if [[ "$line" =~ ^\ \ sql:\ \>\- ]]; then
        current_sql=""
        in_sql=1
        continue
    fi

    # sql: <inline value>
    if [[ "$line" =~ ^\ \ sql:\ (.*) ]]; then
        current_sql="${BASH_REMATCH[1]}"
        in_sql=0
        continue
    fi

    # Continuation of folded scalar
    if [ "$in_sql" -eq 1 ]; then
        if [[ "$line" =~ ^\ \ \ \  ]]; then
            local_line="${line#    }"
            if [ -z "$current_sql" ]; then
                current_sql="$local_line"
            else
                current_sql="$current_sql $local_line"
            fi
        else
            in_sql=0
        fi
    fi
done < /queries.yaml

# Flush last record
if [ -n "$current_name" ] && [ -n "$current_sql" ]; then
    run_query "$current_suite" "$current_name" "$current_sql"
fi

# --- psql-specific: backslash commands ---
echo ""
echo "=== psql backslash commands ==="

for cmd_pair in \
    "dt:\\dt" \
    "dn:\\dn" \
    "l:\\l" \
    "di:\\di" \
    "dv:\\dv" \
    "df:\\df" \
; do
    name="${cmd_pair%%:*}"
    cmd="${cmd_pair#*:}"
    if output=$(psql -X -c "$cmd" 2>&1); then
        report "psql_commands" "$name" "pass" ""
    else
        report "psql_commands" "$name" "fail" "$output"
    fi
done

# --- psql-specific: DDL/DML (single session) ---
echo ""
echo "=== DDL and DML ==="

# Run all DDL/DML in one psql session so tables persist across statements.
# Use -v ON_ERROR_STOP=1 so we can detect failures per block.
ddl_output=$(psql -X -A -t -v ON_ERROR_STOP=1 <<'SQL' 2>&1
DROP TABLE IF EXISTS psql_test;
CREATE TABLE psql_test (id INTEGER, name VARCHAR, value DOUBLE);
SELECT 'DDL_OK';
INSERT INTO psql_test VALUES (1, 'alice', 3.14), (2, 'bob', 2.72);
SELECT 'INSERT_OK';
SELECT count(*) FROM psql_test;
UPDATE psql_test SET value = 9.99 WHERE id = 1;
SELECT 'UPDATE_OK';
DELETE FROM psql_test WHERE id = 2;
SELECT 'DELETE_OK';
SELECT count(*) FROM psql_test;
DROP TABLE psql_test;
SELECT 'DROP_OK';
SQL
)

if echo "$ddl_output" | grep -q 'DDL_OK'; then
    report "ddl_dml" "CREATE TABLE" "pass" ""
else
    report "ddl_dml" "CREATE TABLE" "fail" "$(echo "$ddl_output" | head -5)"
fi

if echo "$ddl_output" | grep -q 'INSERT_OK'; then
    report "ddl_dml" "INSERT" "pass" ""
else
    report "ddl_dml" "INSERT" "fail" "$(echo "$ddl_output" | head -5)"
fi

count=$(echo "$ddl_output" | sed -n '/INSERT_OK/{n;p;}')
if [ "$count" = "2" ]; then
    report "ddl_dml" "SELECT count" "pass" "2 rows"
else
    report "ddl_dml" "SELECT count" "fail" "expected 2, got $count"
fi

if echo "$ddl_output" | grep -q 'UPDATE_OK'; then
    report "ddl_dml" "UPDATE" "pass" ""
else
    report "ddl_dml" "UPDATE" "fail" "$(echo "$ddl_output" | head -5)"
fi

if echo "$ddl_output" | grep -q 'DELETE_OK'; then
    report "ddl_dml" "DELETE" "pass" ""
else
    report "ddl_dml" "DELETE" "fail" "$(echo "$ddl_output" | head -5)"
fi

if echo "$ddl_output" | grep -q 'DROP_OK'; then
    report "ddl_dml" "DROP TABLE" "pass" ""
else
    report "ddl_dml" "DROP TABLE" "fail" "$(echo "$ddl_output" | head -5)"
fi

# --- COPY TO STDOUT (single session) ---
echo ""
echo "=== COPY ==="

copy_output=$(psql -X <<'SQL' 2>&1
DROP TABLE IF EXISTS psql_copy;
CREATE TABLE psql_copy (id INTEGER, name VARCHAR);
INSERT INTO psql_copy VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
COPY psql_copy TO STDOUT WITH CSV HEADER;
DROP TABLE psql_copy;
SQL
)

# Extract only the CSV lines (header + data rows between command tags)
csv_lines=$(echo "$copy_output" | grep -v '^DROP TABLE$' | grep -v '^CREATE TABLE$' | grep -v '^INSERT' | grep -v '^$' | grep -c '^' || true)
if [ "$csv_lines" -eq 4 ]; then
    report "copy" "COPY TO STDOUT" "pass" "$((csv_lines - 1)) data rows"
else
    report "copy" "COPY TO STDOUT" "fail" "expected 4 csv lines, got $csv_lines"
fi

# --- Summary ---
echo ""
echo "=================================================="
echo "Results: $PASSED passed, $FAILED failed"
echo "=================================================="

done_report

[ "$FAILED" -eq 0 ] || exit 1
