---
name: multitenant-down
description: Tear down the multi-tenant K8s stack, kill port-forwards, and clean up worker pods. Use when done testing multi-tenant mode.
---

Tear down the multi-tenant K8s stack:

1. Kill all port-forwards:
   ```bash
   pkill -f 'port-forward.*duckgres' 2>/dev/null
   ```

2. Delete the duckgres namespace (removes control plane, workers, RBAC, services):
   ```bash
   kubectl delete namespace duckgres --ignore-not-found
   ```

3. Stop the config store:
   ```bash
   docker compose -f k8s/local-config-store.compose.yaml down
   ```

4. Clean up the TLS cert:
   ```bash
   rm -f /tmp/duckgres-server.crt
   ```

5. Report to the user that the stack is torn down.
