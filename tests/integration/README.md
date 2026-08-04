# Duckgres PostgreSQL Compatibility Integration Tests

This directory contains a comprehensive integration test suite to verify Duckgres compatibility with PostgreSQL 16 for OLAP workloads.

## Overview

The test suite runs queries against both a real PostgreSQL 16 instance and Duckgres, comparing results to ensure semantic equivalence. This approach catches compatibility issues that unit tests might miss.

### Test Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Test Runner (Go)                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────┐              ┌─────────────────────┐           │
│  │   PostgreSQL 16     │              │      Duckgres       │           │
│  │   (Docker)          │              │   (In-Process)      │           │
│  │   Port: 35432       │              │   Port: dynamic     │           │
│  └─────────────────────┘              └─────────────────────┘           │
│           │                                    │                         │
│           └────────────┬───────────────────────┘                         │
│                        │                                                 │
│                        ▼                                                 │
│             ┌──────────────────────┐                                     │
│             │  Result Comparator   │                                     │
│             │  - Column names      │                                     │
│             │  - Row data          │                                     │
│             │  - Type coercion     │                                     │
│             └──────────────────────┘                                     │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Go 1.21+
- Docker with `docker compose`
- [`just`](https://github.com/casey/just)
- GCC C++ compiler (for DuckDB native bindings - provides libstdc++ for linking)

```bash
# Fedora/RHEL
sudo dnf install gcc-c++

# Ubuntu/Debian
sudo apt install g++

# macOS (included with Xcode Command Line Tools)
xcode-select --install
```

### Running Tests

```bash
# Preferred: run the integration suite through the repo task runner
just test-integration

# DuckLake concurrency benchmark
just test-ducklake-concurrency
```

### Focused Iteration

Use direct `go test` invocations when narrowing to a specific package or test name:

```bash
# Run the full integration package directly
go test -v ./tests/integration/...

# Run without PostgreSQL (Duckgres-only mode)
# Tests automatically fall back if PostgreSQL is not running
go test -v ./tests/integration/...
```

If you want to manage the PostgreSQL reference container yourself during local iteration:

```bash
docker compose -f tests/integration/docker-compose.yml up -d
go test -v ./tests/integration/...
docker compose -f tests/integration/docker-compose.yml down -v
```

### Running Specific Test Categories

```bash
# Data Query Language tests
go test -v ./tests/integration/... -run TestDQL

# Data Definition Language tests
go test -v ./tests/integration/... -run TestDDL

# Data Manipulation Language tests
go test -v ./tests/integration/... -run TestDML

# Data type tests
go test -v ./tests/integration/... -run TestTypes

# Function tests
go test -v ./tests/integration/... -run TestFunctions

# System catalog tests
go test -v ./tests/integration/... -run TestCatalog

# Session command tests
go test -v ./tests/integration/... -run TestSession

# Wire protocol tests
go test -v ./tests/integration/... -run TestProtocol

# Client tool compatibility tests
go test -v ./tests/integration/clients/...
```

## Test Coverage

| Category | Test Cases | Description |
|----------|-----------|-------------|
| **DQL** | ~150 | SELECT, WHERE, ORDER BY, GROUP BY, JOINs, CTEs, Window functions, Set operations |
| **DDL** | ~50 | CREATE/ALTER/DROP TABLE, VIEW, INDEX, SCHEMA, constraints |
| **DML** | ~45 | INSERT, UPDATE, DELETE, RETURNING, ON CONFLICT (UPSERT) |
| **Types** | ~80 | Numeric, string, date/time, boolean, JSON, arrays, UUID, NULL handling |
| **Functions** | ~180 | String, numeric, date/time, aggregate, JSON, array, conditional |
| **Catalog** | ~50 | pg_catalog tables, information_schema, psql meta-commands |
| **Session** | ~40 | SET, SHOW, RESET, transaction commands |
| **Protocol** | ~40 | Simple query, prepared statements, transactions, error handling |
| **Clients** | ~50 | Metabase, Grafana, Superset, Tableau, DBeaver, Fivetran, Airbyte, dbt |
| **Total** | **~700** | |

## Directory Structure

```
tests/integration/
├── README.md                   # This file
├── docker-compose.yml          # PostgreSQL 16 container definition
├── harness.go                  # Test harness for side-by-side testing
├── compare.go                  # Result comparison utilities
├── setup_test.go               # Test initialization and helpers
├── fixtures/
│   ├── schema.sql              # Test table definitions (18 tables, 3 views)
│   └── data.sql                # Test data
├── dql_test.go                 # Data Query Language tests
├── ddl_test.go                 # Data Definition Language tests
├── dml_test.go                 # Data Manipulation Language tests
├── types_test.go               # Data type tests
├── functions_test.go           # Function compatibility tests
├── catalog_test.go             # pg_catalog/information_schema tests
├── session_test.go             # SET/SHOW/transaction tests
├── protocol_test.go            # Wire protocol tests
└── clients/
    └── clients_test.go         # BI/ETL tool compatibility tests
```

## Comparison Strategy

The test suite uses **semantic equivalence** rather than byte-identical comparison:

- **Column names**: Case-insensitive comparison
- **Row data**: Same values after type normalization
- **Ordering**: Respects ORDER BY; otherwise treats results as sets
- **NULL handling**: NULL == NULL for comparison purposes
- **Numeric precision**: Float tolerance of 1e-9
- **Timestamps**: Tolerance of 1 microsecond
- **JSON**: Parsed and compared structurally

## Skipped Tests

Some tests are skipped with documented reasons:

| Skip Reason | Description |
|-------------|-------------|
| `SkipUnsupportedByDuckDB` | Feature not available in DuckDB |
| `SkipDifferentBehavior` | Intentionally different (documented) |
| `SkipOLTPFeature` | OLTP feature out of scope |
| `SkipNetworkType` | Network types (INET, CIDR) not supported |
| `SkipGeometricType` | Geometric types not supported |
| `SkipRangeType` | Range types not supported |
| `SkipTextSearch` | Full-text search not supported |

## Client Tool Compatibility

The test suite includes real queries used by popular tools:

### BI Tools
- **Metabase**: Schema discovery, table introspection, column metadata
- **Grafana**: Time column detection, table autocomplete
- **Superset**: Table listing, column type discovery
- **Tableau**: Schema/table/column discovery with full metadata
- **DBeaver**: Catalog info, table and column metadata with descriptions

### ETL Tools
- **Fivetran**: Schema sync, primary key detection, commented queries
- **Airbyte**: Table discovery, column introspection
- **dbt**: Relation existence checks, schema management, transactions

## Adding New Tests

### QueryTest Structure

```go
tests := []QueryTest{
    {
        Name:         "test_name",           // Unique test name
        Query:        "SELECT 1",            // SQL to execute
        Skip:         "",                    // Skip reason (empty = don't skip)
        DuckgresOnly: false,                 // true = skip PostgreSQL comparison
        ExpectError:  false,                 // true = expect query to fail
        Options:      DefaultCompareOptions(), // Comparison options
    },
}
runQueryTests(t, tests)
```

### Custom Comparison Options

```go
opts := CompareOptions{
    IgnoreColumnOrder: true,   // Compare columns by name, not position
    IgnoreRowOrder:    true,   // Treat results as sets
    FloatTolerance:    1e-6,   // Custom float tolerance
    IgnoreCase:        true,   // Case-insensitive string comparison
}
```

## DuckLake Concurrency & Latency Benchmarks

The test suite includes benchmarks that measure DuckLake transaction conflict rates under concurrent load, and a latency sensitivity analysis that injects artificial metadata store latency to simulate remote RDS configurations.

### Prerequisites

The DuckLake benchmarks require the metadata PostgreSQL and MinIO infrastructure:

```bash
# Start DuckLake infrastructure
docker compose -f tests/integration/docker-compose.yml up -d ducklake-metadata minio minio-init
```

### Running concurrency benchmarks

```bash
# Run all concurrency tests (default: 0ms latency)
just test-ducklake-concurrency

# Or directly:
go test -v -run TestDuckLakeConcurrentTransactions -timeout 300s ./tests/integration/
```

### Running latency sensitivity analysis

The `DUCKGRES_BENCH_LATENCIES` environment variable controls which latency levels to sweep. Each value is a one-way latency injected via a TCP proxy between DuckDB/DuckLake and the metadata PostgreSQL (total RTT overhead = 2x the configured value).

```bash
# Sweep multiple latency levels
just bench-ducklake-latency 0ms,10ms,25ms,50ms

# Or directly:
DUCKGRES_BENCH_LATENCIES=0ms,10ms,50ms \
  go test -v -run TestDuckLakeConcurrentTransactions -timeout 3600s ./tests/integration/

# Write structured JSON results for comparison
DUCKGRES_BENCH_LATENCIES=0ms,10ms \
DUCKGRES_BENCH_OUT=results.json \
  go test -v -run TestDuckLakeConcurrentTransactions -timeout 3600s ./tests/integration/
```

**Important:** Higher latency levels make tests significantly slower since every metadata round-trip pays the extra RTT. Budget roughly:
- `0ms`: ~2 minutes for all 21 tests
- `10ms`: ~25 minutes
- `20ms`: ~45 minutes
- `50ms`: may exceed 1 hour

### Version matrix

Compare conflict rates across DuckDB/DuckLake versions, optionally combined with latency:

```bash
# Version matrix (current version vs others)
just bench-ducklake-matrix

# Full version × latency matrix
DUCKGRES_BENCH_LATENCIES=0ms,10ms just bench-ducklake-matrix
```

### How the latency proxy works

For non-zero latency, a TCP proxy sits between DuckDB's DuckLake extension and the metadata PostgreSQL:

```
DuckDB → DuckLake ext → [TCP Proxy (+Xms per direction)] → Metadata PostgreSQL (port 35433)
```

Each read/write through the proxy gets a `time.Sleep(latency)` before forwarding. For `0ms`, no proxy is used (zero overhead). Each latency level gets its own dedicated duckgres server instance.

### JSON output schema

When `DUCKGRES_BENCH_OUT` is set, the test writes a JSON report:

```json
{
  "duckdb_version": "v1.5.2",
  "ducklake_version": "67480b1d",
  "latencies_tested_ms": [0, 10],
  "timestamp": "2026-04-01T19:12:47Z",
  "metrics": [
    {
      "test": "concurrent_updates_same_rows",
      "metadata_latency_ms": 0,
      "successes": 103,
      "conflicts": 77,
      "conflict_rate_pct": 42.8,
      "duration_sec": 5.2,
      "throughput_ops_sec": 19.8
    }
  ]
}
```

### Environment variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DUCKGRES_BENCH_LATENCIES` | Comma-separated latency levels (e.g. `0ms,10ms,50ms`) | `0ms` |
| `DUCKGRES_BENCH_OUT` | Path to write JSON results | *(none, no file written)* |
| `DUCKGRES_STRESS` | Set to any value to increase SQLMesh model count (10→30) | *(unset)* |
| `DUCKGRES_TEST_NO_DUCKLAKE` | Set to `1` to disable DuckLake mode | *(unset)* |

## Troubleshooting

### PostgreSQL container won't start

```bash
# Check if port 35432 is in use
lsof -i :35432

# View container logs
docker-compose -f tests/integration/docker-compose.yml logs
```

### Tests timeout

```bash
# Increase test timeout
go test -v -timeout 10m ./tests/integration/...
```

### Connection refused errors

The test harness automatically retries connections. If you still see errors:

```bash
# Ensure PostgreSQL is ready
docker-compose -f tests/integration/docker-compose.yml exec postgres pg_isready

# Check Duckgres can start
go test -v ./tests/integration/... -run TestProtocolSimpleQuery
```

## Contributing

1. Add new test cases to the appropriate `*_test.go` file
2. Use `DuckgresOnly: true` for queries that can't be compared to PostgreSQL
3. Document skip reasons clearly
4. Run the full suite before submitting:
   ```bash
   go test -v ./tests/integration/...
   ```
