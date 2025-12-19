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
- Docker and Docker Compose
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
# Start PostgreSQL container
docker-compose -f tests/integration/docker-compose.yml up -d

# Wait for PostgreSQL to be ready (about 5 seconds)
sleep 5

# Run all integration tests
go test -v ./tests/integration/...

# Run without PostgreSQL (Duckgres-only mode)
# Tests will automatically fall back if PostgreSQL isn't running
go test -v ./tests/integration/...

# Stop PostgreSQL when done
docker-compose -f tests/integration/docker-compose.yml down -v
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
