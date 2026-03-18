---
name: multitenant-up
description: Build, deploy, and port-forward the multi-tenant control plane on local Kubernetes. Use when you need to test duckgres in multi-tenant mode with per-team worker pools.
---

Boot up the multi-tenant K8s stack:

1. Build and deploy:
   ```bash
   just run-multitenant-local
   ```

2. Kill any stale port-forwards and start fresh ones:
   ```bash
   pkill -f 'port-forward.*duckgres' 2>/dev/null; sleep 1
   kubectl -n duckgres port-forward svc/duckgres 5432:5432 &>/dev/null &
   kubectl -n duckgres port-forward svc/duckgres 8815:8815 &>/dev/null &
   kubectl -n duckgres port-forward deployment/duckgres-control-plane 9090:9090 &>/dev/null &
   ```

3. Grab the admin API token:
   ```bash
   kubectl -n duckgres logs deployment/duckgres-control-plane | grep 'admin API token'
   ```

4. Extract the TLS cert for Flight SQL clients:
   ```bash
   kubectl -n duckgres exec deployment/duckgres-control-plane -- cat /certs/server.crt > /tmp/duckgres-server.crt
   ```

5. Report to the user:
   - Admin dashboard: http://localhost:9090 (show the token)
   - PG: `PGSSLMODE=require PGPASSWORD=postgres psql -h localhost -U postgres`
   - Flight SQL (DuckDB): `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=/tmp/duckgres-server.crt duckdb` then `INSTALL duckhog FROM community; LOAD duckhog; ATTACH 'hog:memory?user=postgres&password=postgres&flight_server=grpc+tls://localhost:8815' AS mt;`
   - Default credentials: postgres / postgres
