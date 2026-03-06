# TODO - Future Enhancements

## High Priority

### Security
- [x] **TLS/SSL Support**: Add encrypted connections for production use
- [ ] **MD5 Password Authentication**: Support PostgreSQL's MD5-based auth for better security
- [ ] **SCRAM-SHA-256 Authentication**: Modern PostgreSQL authentication method
- [x] **Connection Rate Limiting**: Prevent brute-force attacks

### Configuration
- [x] **Config File Support**: Load configuration from YAML/TOML file instead of hardcoding
- [x] **Environment Variables**: Support `DUCKGRES_PORT`, `DUCKGRES_DATA_DIR`, etc.
- [x] **Command Line Flags**: `--port`, `--data-dir`, `--config` options
- [ ] **Dynamic User Management**: Add/remove users without restart

### Protocol Compatibility
- [ ] **Binary Format Support**: Encode results in binary format for better performance with some clients
- [x] **COPY Protocol**: Support `COPY FROM`/`COPY TO` for bulk data loading
- [x] **Cancel Request Handling**: Properly cancel long-running queries ([protocol.go](server/protocol.go), [cancel_test.go](tests/integration/cancel_test.go))

### Compatibility
- [x] **System Catalog Emulation**: Basic `pg_catalog` compatibility for psql
  - [x] `\dt` (list tables) - working
  - [x] `\l` (list databases) - working
  - [x] `\d <table>` (describe table) - working
- [x] **Information Schema**: Emulate PostgreSQL's `information_schema` ([information_schema.go](transpiler/transform/information_schema.go))
- [x] **Session Variables**: Support `SET` commands (timezone, search_path, etc.) ([setshow.go](transpiler/transform/setshow.go))
- [x] **Type OID Mapping**: Proper PostgreSQL OID mapping for all DuckDB types ([types.go](server/types.go))

### Features
- [x] **Extensions**: Load DuckDB extensions on startup

### Operations
- [ ] **Hot Reload**: Reload config without restart
- [ ] **Admin Commands**: `\duckgres status`, `\duckgres users`, etc.
- [x] **Docker Image**: Official container image ([Dockerfile](Dockerfile))
- [x] **Graceful Shutdown**: Finish in-flight queries before shutdown

## Medium Priority

### Performance
- [ ] **DuckDB Per Connection**: each connection for each user is a separate duckdb instance 
- [x] **Connection Limits**: Max connections per user and globally ([ratelimit.go](server/ratelimit.go))

### Monitoring
- [x] **Prometheus Metrics**: Export query count, latency, connection stats ([metrics.go](server/flightsqlingress/metrics.go), [flight_ingress_metrics.go](controlplane/flight_ingress_metrics.go))
- [x] **Query Logging**: Configurable query logging to file/stdout ([querylog.go](server/querylog.go))
- [ ] **Slow Query Log**: Log queries exceeding threshold
- [ ] **Health Check Endpoint**: HTTP endpoint for load balancer health checks

## Low Priority

### Features
- [ ] **Read Replicas**: Support read-only replicas using DuckDB's `ATTACH`
- [ ] **Multi-Database**: Support multiple named databases per user
- [ ] **Schema Support**: PostgreSQL schema emulation

### Testing
- [x] **Unit Tests**: Test wire protocol parsing/encoding ([protocol_test.go](server/protocol_test.go), [types_test.go](server/types_test.go), [conn_test.go](server/conn_test.go))
- [x] **Integration Tests**: Test with various PostgreSQL clients ([tests/integration/](tests/integration/README.md))
- [x] **Compatibility Tests**: Run PostgreSQL regression tests ([clients_test.go](tests/integration/clients/clients_test.go), [jdbc_test.go](tests/integration/jdbc_test.go))
- [x] **Benchmark Suite**: Performance comparison with native PostgreSQL ([tests/perf/](tests/perf/README.md))

## Ideas / Research

- **Federation**: Query multiple DuckDB files in single query
- **Caching Layer**: Redis/memcached for query results
- **Query Rewriting**: Translate PostgreSQL-specific syntax to DuckDB
- **MotherDuck Integration**: Support MotherDuck cloud backend
- **Parquet Direct Query**: `SELECT * FROM 's3://bucket/file.parquet'`

## Known Issues

- [x] Some PostgreSQL drivers may fail with unsupported OIDs — unknown types fall back to `OidText` ([types.go](server/types.go))
- [x] `\d` commands in psql don't work (need system catalog) - fixed
- [x] Transaction isolation may differ from PostgreSQL behavior — documented in [README](README.md#transaction-isolation)
- [x] Large result sets may cause memory issues (no streaming) — results are streamed row-by-row via `rows.Next()` + server-side cursor emulation ([conn.go](server/conn.go))

## Contributing

Pick an item from this list and submit a PR! Issues labeled [`good first issue`](https://github.com/PostHog/duckgres/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) are great starting points.
