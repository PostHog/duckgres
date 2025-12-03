# Duckgres

A PostgreSQL wire protocol compatible server backed by DuckDB. Connect with any PostgreSQL client (psql, pgAdmin, lib/pq, psycopg2, etc.) and get DuckDB's analytical query performance.

## Features

- **PostgreSQL Wire Protocol**: Full compatibility with PostgreSQL clients
- **Per-User Databases**: Each authenticated user gets their own isolated DuckDB database file
- **Simple Password Authentication**: Username/password authentication (cleartext)
- **Extended Query Protocol**: Support for prepared statements and parameterized queries
- **SQL Compatibility**: Run most SQL queries that DuckDB supports

## Quick Start

### Build

```bash
go build -o duckgres .
```

### Run

```bash
./duckgres
```

The server starts on port 5432 by default. Database files are stored in `./data/`.

### Connect

```bash
# Using psql
PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres

# Or with any PostgreSQL driver
```

## Configuration

Configuration is currently done in `main.go`:

```go
cfg := server.Config{
    Host:    "0.0.0.0",
    Port:    5432,
    DataDir: "./data",
    Users: map[string]string{
        "postgres": "postgres",
        "alice":    "alice123",
        "bob":      "bob123",
    },
}
```

### Users

Each user in the `Users` map gets:
- Their own DuckDB database file at `./data/{username}.db`
- Complete isolation from other users' data

## Usage Examples

```sql
-- Create a table
CREATE TABLE events (
    id INTEGER,
    name VARCHAR,
    timestamp TIMESTAMP,
    value DOUBLE
);

-- Insert data
INSERT INTO events VALUES
    (1, 'click', '2024-01-01 10:00:00', 1.5),
    (2, 'view', '2024-01-01 10:01:00', 2.0);

-- Query with DuckDB's analytical power
SELECT name, COUNT(*), AVG(value)
FROM events
GROUP BY name;

-- Use prepared statements (via client drivers)
-- Works with lib/pq, psycopg2, JDBC, etc.
```

## Architecture

```
┌─────────────────┐
│  PostgreSQL     │
│  Client (psql)  │
└────────┬────────┘
         │ PostgreSQL Wire Protocol
         ▼
┌─────────────────┐
│    Duckgres     │
│    Server       │
└────────┬────────┘
         │ database/sql
         ▼
┌─────────────────┐
│    DuckDB       │
│  (per-user db)  │
└─────────────────┘
```

## Supported SQL Commands

- `SELECT` - Full query support
- `INSERT` - Single and multi-row inserts
- `UPDATE` - With WHERE clauses
- `DELETE` - With WHERE clauses
- `CREATE TABLE/INDEX/VIEW`
- `DROP TABLE/INDEX/VIEW`
- `ALTER TABLE`
- `BEGIN/COMMIT/ROLLBACK` (DuckDB transaction support)

## Limitations

- **No SSL/TLS**: Connections are unencrypted
- **Text Format Only**: Results are returned in text format (no binary protocol)
- **Single Process**: Each user's database is opened in the same process
- **No Replication**: Single-node only
- **Limited System Catalog**: Some `pg_*` system tables are not available

## Dependencies

- [DuckDB Go Driver](https://github.com/duckdb/duckdb-go) - DuckDB database engine

## License

MIT
