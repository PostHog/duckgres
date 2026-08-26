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
   kubectl -n duckgres port-forward deployment/duckgres-control-plane 8080:8080 &>/dev/null &
   ```

3. Grab the admin API token:
   ```bash
   kubectl -n duckgres logs deployment/duckgres-control-plane | grep 'admin API token'
   ```

4. Report to the user:
   - Admin dashboard: http://localhost:8080 (show the token)
   - PG: `PGSSLMODE=require PGPASSWORD=postgres psql -h localhost -U postgres`
   - Default credentials: postgres / postgres
