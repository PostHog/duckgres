# TODO - Future Enhancements

## High Priority

### Security
- [x] **TLS/SSL Support**: Add encrypted connections for production use
- [ ] **MD5 Password Authentication**: Support PostgreSQL's MD5-based auth for better security
- [ ] **SCRAM-SHA-256 Authentication**: Modern PostgreSQL authentication method
- [ ] **Connection Rate Limiting**: Prevent brute-force attacks

### Configuration
- [x] **Config File Support**: Load configuration from YAML/TOML file instead of hardcoding
- [x] **Environment Variables**: Support `DUCKGRES_PORT`, `DUCKGRES_DATA_DIR`, etc.
- [x] **Command Line Flags**: `--port`, `--data-dir`, `--config` options
- [ ] **Dynamic User Management**: Add/remove users without restart

### Protocol Compatibility
- [ ] **Binary Format Support**: Encode results in binary format for better performance with some clients
- [ ] **COPY Protocol**: Support `COPY FROM`/`COPY TO` for bulk data loading
- [ ] **Cancel Request Handling**: Properly cancel long-running queries

### Compatibility
- [ ] **System Catalog Emulation**: Implement `pg_catalog` views for tool compatibility
  - [ ] `pg_database`
  - [ ] `pg_tables`
  - [ ] `pg_columns`
  - [ ] `pg_type`
  - [ ] `pg_class`
- [ ] **Information Schema**: Emulate PostgreSQL's `information_schema`
- [ ] **Session Variables**: Support `SET` commands (timezone, search_path, etc.)
- [ ] **Type OID Mapping**: Proper PostgreSQL OID mapping for all DuckDB types

### Features
- [ ] **Extensions**: Load DuckDB extensions on startup

### Operations
- [ ] **Hot Reload**: Reload config without restart
- [ ] **Admin Commands**: `\duckgres status`, `\duckgres users`, etc.
- [ ] **Docker Image**: Official container image
- [ ] **Graceful Shutdown**: Finish in-flight queries before shutdown

## Medium Priority

### Performance
- [ ] **DuckDB Per Connection**: each connection for each user is a separate duckdb instance 
- [ ] **Connection Limits**: Max connections per user and globally

### Monitoring
- [ ] **Prometheus Metrics**: Export query count, latency, connection stats
- [ ] **Query Logging**: Configurable query logging to file/stdout
- [ ] **Slow Query Log**: Log queries exceeding threshold
- [ ] **Health Check Endpoint**: HTTP endpoint for load balancer health checks

## Low Priority

### Features
- [ ] **Read Replicas**: Support read-only replicas using DuckDB's `ATTACH`
- [ ] **Multi-Database**: Support multiple named databases per user
- [ ] **Schema Support**: PostgreSQL schema emulation

### Testing
- [ ] **Unit Tests**: Test wire protocol parsing/encoding
- [ ] **Integration Tests**: Test with various PostgreSQL clients
- [ ] **Compatibility Tests**: Run PostgreSQL regression tests
- [ ] **Benchmark Suite**: Performance comparison with native PostgreSQL

## Ideas / Research

- **Federation**: Query multiple DuckDB files in single query
- **Caching Layer**: Redis/memcached for query results
- **Query Rewriting**: Translate PostgreSQL-specific syntax to DuckDB
- **MotherDuck Integration**: Support MotherDuck cloud backend
- **Parquet Direct Query**: `SELECT * FROM 's3://bucket/file.parquet'`

## Known Issues

- [ ] Some PostgreSQL drivers may fail with unsupported OIDs
- [ ] `\d` commands in psql don't work (need system catalog)
- [ ] Transaction isolation may differ from PostgreSQL behavior
- [ ] Large result sets may cause memory issues (no streaming)

## Contributing

Pick an item from this list and submit a PR! Issues labeled `good first issue` are great starting points.
