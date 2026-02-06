# Production Readiness Plan

This document tracks issues to bring Duckgres up to production database interface standards.

---

## Tier 1: Production Blockers

### 1. Implement Query Cancellation
- **Branch**: `feature/query-cancellation`
- **Status**: ✅ Implemented
- **File**: `server/conn.go:331-334`, `server/protocol.go:76-79`
- **Current**: Cancel request message (80877102) is received but immediately returns without action
- **Impact**: Clients cannot cancel long-running queries; Ctrl+C in psql does nothing
- **Fix**:
  - Track running queries by backend key (pid + secret)
  - Store `context.CancelFunc` for each active query
  - On cancel request, look up and call the cancel function
  - Return SQLSTATE 57014 (query_canceled)
- [x] Add query context tracking map to Server struct
- [x] Wrap all db.Query/db.Exec in context.WithCancel
- [x] Implement cancel request lookup and invocation
- [x] Add duckgres_query_cancellations_total metric
- [ ] Add tests for cancellation (future enhancement)

### 2. Add Statement Timeout Support
- **Branch**: `feature/statement-timeout`
- **Status**: ✅ Implemented
- **File**: `server/conn.go` (all db.Query/db.Exec calls)
- **Current**: No timeout mechanism; queries run indefinitely
- **Impact**: Runaway queries block connections forever
- **Fix**:
  - Add `QueryTimeout` config option (default: 0, disabled)
  - Wrap all query execution in `context.WithTimeout()`
  - Return SQLSTATE 57014 on timeout
- [x] Add QueryTimeout to Config struct
- [x] Add query_timeout to YAML config
- [x] Create queryContext() helper method
- [x] Update all db.Query/db.Exec to use context
- [x] Add duckgres_query_timeouts_total metric
- [ ] Handle SET statement_timeout in transpiler (future enhancement)

### 3. Add Connection Limits
- **Branch**: `feature/connection-limits`
- **Status**: ✅ Implemented
- **File**: `server/server.go:176-209`
- **Current**: Accepts unlimited connections; only per-IP limits exist
- **Impact**: Resource exhaustion possible (10k clients = 10k goroutines + DuckDB instances)
- **Fix**:
  - Add global `max_connections` config
  - Add per-user connection limits
  - Optionally queue connections instead of rejecting
- [x] Add MaxConnections to Config struct
- [x] Add connection limit check in handleConnection()
- [x] Add duckgres_connection_limit_rejects_total metric
- [x] Add DUCKGRES_MAX_CONNECTIONS env var and YAML config
- [ ] Add per-user connection tracking (future enhancement)

### 4. Implement Modern Authentication
- **Branch**: `feature/modern-auth`
- **Status**: ✅ Implemented (MD5)
- **File**: `server/conn.go:354-395`, `server/protocol.go:49-52`
- **Current**: Cleartext password only (relies on TLS for encryption)
- **Impact**: Weak security; many tools expect MD5 or SCRAM
- **Fix**:
  - Implement MD5 authentication (PostgreSQL standard)
  - Implement SCRAM-SHA-256 (modern standard)
  - Make auth method configurable per-user
- [x] Add AuthMethod to Config struct ("cleartext", "md5")
- [x] Add writeAuthMD5Password to protocol.go
- [x] Implement verifyMD5Password for MD5 hash verification
- [x] Add DUCKGRES_AUTH_METHOD env var and auth_method YAML config
- [ ] Add SCRAM-SHA-256 support (future enhancement)
- [ ] Store password hashes instead of plaintext in config (future enhancement)

---

## Tier 2: Major Compatibility Gaps

### 5. Improve SQLSTATE Error Code Mapping
- **File**: `server/conn.go` (sendError calls throughout)
- **Current**: Almost all errors return generic SQLSTATE 42000
- **Impact**: Clients can't distinguish constraint violations, type errors, etc.
- **Fix**:
  - Create DuckDB error → PostgreSQL SQLSTATE mapping
  - Parse DuckDB error messages for error type
  - Return appropriate codes (23xxx for constraints, 22xxx for data, etc.)
- [ ] Create error code mapping table
- [ ] Parse DuckDB error strings for classification
- [ ] Update sendError calls with proper codes
- [ ] Add tests for error code accuracy

### 6. Fix Transaction Abort Handling
- **File**: `server/conn.go:1160-1166`
- **Current**: After error, txStatus='E' but queries still execute
- **Impact**: PostgreSQL behavior is to queue commands until ROLLBACK
- **Fix**:
  - Check txStatus before executing queries
  - Return "current transaction is aborted" error for non-ROLLBACK commands
- [ ] Add txStatus check at start of query execution
- [ ] Return SQLSTATE 25P02 for commands in aborted transaction
- [ ] Allow only ROLLBACK/ABORT when txStatus='E'

### 7. Handle SET Commands Properly
- **File**: `transpiler/transform/setshow.go`
- **Current**: Many SET commands silently ignored (return success without effect)
- **Impact**: Misleading; clients think settings changed
- **Fix**:
  - Log warnings for ignored SET commands
  - Optionally return NOTICE for unsupported parameters
  - Document which SET commands are supported
- [ ] Add NOTICE responses for ignored SET commands
- [ ] Document supported vs unsupported SET parameters
- [ ] Consider implementing key SET commands (application_name, etc.)

### 8. Add LISTEN/NOTIFY Support (Optional)
- **Current**: Not implemented
- **Impact**: Real-time notification tools won't work
- **Note**: Lower priority; most analytics use cases don't need this
- [ ] Evaluate feasibility with DuckDB backend
- [ ] Implement if viable

---

## Tier 3: Operational Gaps

### 9. Add Health Check Endpoint
- **File**: `main.go:86-95`
- **Current**: Only Prometheus metrics on :9090/metrics
- **Impact**: Load balancers need simple health checks
- **Fix**:
  - Add `/health` endpoint returning 200 OK
  - Optionally check DuckDB connectivity
- [ ] Add health check HTTP handler
- [ ] Return 200 if healthy, 503 if unhealthy
- [ ] Make health check port configurable

### 10. Make Metrics Port Configurable
- **File**: `main.go:89-91`
- **Current**: Hardcoded to :9090
- **Impact**: Can't run multiple instances on same host
- **Fix**:
  - Add metrics_port to config
  - Add DUCKGRES_METRICS_PORT env var
- [ ] Add MetricsPort to config
- [ ] Add to YAML and env var parsing
- [ ] Update initMetrics() to use config

### 11. Add Query Audit Logging
- **File**: `server/conn.go` (query execution paths)
- **Current**: Queries logged at DEBUG level only
- **Impact**: Production needs audit trail (who, what, when)
- **Fix**:
  - Add separate audit log
  - Include: timestamp, user, query, status, duration, rows
  - Make configurable (enable/disable, file path)
- [ ] Add audit log config options
- [ ] Create audit log writer
- [ ] Log all queries with metadata
- [ ] Add log rotation support

### 12. Add Slow Query Logging
- **Current**: All queries logged equally at DEBUG
- **Impact**: Hard to identify performance issues
- **Fix**:
  - Add slow_query_threshold config (e.g., 1s)
  - Log queries exceeding threshold at WARN level
  - Include query duration and plan if available
- [ ] Add SlowQueryThreshold to config
- [ ] Check query duration after execution
- [ ] Log slow queries with execution details

### 13. Implement Hot Reload
- **Current**: Config changes require restart
- **Impact**: Downtime for user/setting changes
- **Fix**:
  - Watch config file for changes
  - Reload users without dropping connections
  - Apply new rate limits dynamically
- [ ] Add SIGHUP handler for reload
- [ ] Implement user list hot reload
- [ ] Implement rate limit hot reload
- [ ] Log config changes

### 14. Add Admin Interface
- **Current**: No way to inspect server state
- **Impact**: Ops can't see connections, kill queries
- **Fix**:
  - Add admin commands (via special queries or separate port)
  - Show active connections, query stats
  - Allow killing connections/queries
- [ ] Design admin command interface
- [ ] Implement SHOW CONNECTIONS
- [ ] Implement KILL CONNECTION
- [ ] Add connection/query statistics

### 15. Add Per-User Metrics
- **Current**: Metrics are global only
- **Impact**: Can't track resource usage by user
- **Fix**:
  - Add user label to query metrics
  - Track connections per user
  - Track query count/duration per user
- [ ] Add user label to Prometheus metrics
- [ ] Update metric recording to include user
- [ ] Add per-user connection gauge

---

## Tier 4: Security Improvements

### 16. Add Client Certificate Validation
- **File**: `server/server.go:142-145`
- **Current**: Server presents cert but doesn't validate clients
- **Impact**: No mutual TLS option
- **Fix**:
  - Add ClientAuth option to TLS config
  - Support verify-ca and verify-full modes
- [ ] Add TLS client auth config options
- [ ] Load CA bundle for client verification
- [ ] Implement certificate validation

### 17. Hash Passwords in Config
- **File**: `main.go` (user config parsing)
- **Current**: Plaintext passwords in YAML
- **Impact**: Config files often exposed
- **Fix**:
  - Store bcrypt/scram hashes instead of plaintext
  - Add tool to generate hashed passwords
- [ ] Change password storage to hashes
- [ ] Add password hashing utility
- [ ] Document password format

### 18. Add Per-User Access Control
- **Current**: All authenticated users have equal access
- **Impact**: No way to restrict users to read-only
- **Fix**:
  - Add role/permission config per user
  - Enforce read-only for restricted users
- [ ] Add user roles to config
- [ ] Check permissions before query execution
- [ ] Block write queries for read-only users

---

## Tier 5: Nice to Have

### 19. Connection Pooling
- **Current**: One DuckDB instance per client connection
- **Note**: May not be needed for DuckDB's use case
- [ ] Evaluate if pooling makes sense for DuckDB

### 20. Add Panic Recovery
- **File**: `server/server.go:209-212`
- **Current**: Panic in handler crashes entire server
- **Fix**: Add recover() to connection goroutines
- [ ] Add defer/recover to handleConnection
- [ ] Log panics with stack trace
- [ ] Add panic counter metric

### 21. Add Message Size Validation
- **File**: `server/protocol.go:125-132`
- **Current**: No max message size check
- **Impact**: Memory exhaustion possible
- [ ] Add MaxMessageSize constant
- [ ] Validate before allocation

---

## Progress Tracking

| Tier | Issues | Fixed | Remaining |
|------|--------|-------|-----------|
| 1 - Blockers | 4 | 4 | 0 |
| 2 - Compatibility | 4 | 0 | 4 |
| 3 - Operational | 7 | 0 | 7 |
| 4 - Security | 3 | 0 | 3 |
| 5 - Nice to Have | 3 | 0 | 3 |
| **Total** | **21** | **4** | **17** |

---

## Recommended Implementation Order

### Phase 1: Query Safety (Week 1-2)
1. Statement timeout (#2) - prevents runaway queries
2. Query cancellation (#1) - user control over queries
3. Panic recovery (#20) - server stability

### Phase 2: Security & Limits (Week 3-4)
4. Connection limits (#3) - resource protection
5. Modern auth (#4) - security baseline
6. Password hashing (#17) - config security

### Phase 3: Compatibility (Week 5-6)
7. Error code mapping (#5) - client compatibility
8. Transaction abort handling (#6) - PostgreSQL compliance
9. SET command handling (#7) - tool compatibility

### Phase 4: Operations (Week 7-8)
10. Health check endpoint (#9) - deployment ready
11. Configurable metrics port (#10) - deployment flexibility
12. Slow query logging (#12) - performance debugging

### Ongoing
- Audit logging (#11)
- Hot reload (#13)
- Admin interface (#14)
- Per-user metrics (#15)
