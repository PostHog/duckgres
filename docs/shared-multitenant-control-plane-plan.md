# Shared Multi-Tenant Control Plane Summary

- Each team owns exactly one managed warehouse, "team" is used interchangeably with "client".
- A provisioning layer, driven by PostHog and backed by Duckling/Crossplane, creates each team's warehouse database, metadata store, S3 bucket, worker identity, and secrets.
- A shared duckgres multi-tenant control plane stores team metadata, user credentials, warehouse config, and secret refs in a separate config-store PostgreSQL database.
- The control plane also exposes an admin API and dashboard for operator visibility and config management.
- A shared K8s cluster runs team-scoped worker pods for the multi-tenant control plane.
- Each worker pod gets only the runtime configuration and access needed for that team's warehouse database, metadata store, and S3 bucket.

## Core Flow

1. A user clicks "provision managed warehouse" in PostHog.
2. The provisioning workflow creates the team's warehouse database, metadata store, S3 bucket, worker identity, and secrets, then writes the team's connection metadata into the duckgres config store.
3. Once provisioning completes, the UI shows the team's pgwire and Duckhog connection instructions for the shared control-plane endpoint.
4. The user connects to the shared multi-tenant duckgres control plane.
5. The control plane authenticates the user against the config store, resolves the user's team, and assigns the session to that team's worker pool.
6. The worker pool assign the query to an existing worker or spawn new one.
7. After session close, the worker cleans up session state and is available for reuse.

Separate from the end-user query path, operators use the admin API/dashboard to inspect state and manage multi-tenant control-plane configuration.

## Limitations / Alternative Designs
- Per-team worker identity seems like a good isolation mechanism, with the drawback that pre-warmed workers will be team-specific, we will either have 5000~20000 standing worker pods, or some sort of smart mechanism to decide when to keep standing warm worker when not to. Alternatives are injecting short-lived secrets when assigning to a session, with the drawback that it introduces isolation/security risks. 
- Control plane is single node for MVP, but will need to be made stateless and horizontally scalable for availability and expected usages. The other option is to move control plane off the query path, so it doesn't need to scale with usages.
- Supporting multiple concurrent sessions on a long-lived worker minimizes coldstart and k8 API pressure, but also brings many challenges, like cleanup failures, noisy neighbor, or hard-to-debug cross-session leakage bugs. Alternative is to enforce single session per worker, and use smaller or even dynamic pod sizes, with the downside that we must keep pre-warmed workers or suffer cold-start delays.


## Already In Place

- A multi-tenant control-plane path exists behind `--config-store` with the remote worker backend.
- The control plane can authenticate pgwire users against the config store and resolve each user to a team.
- The config store persists teams, users, password hashes, and some per-team limits, and it supports polling plus in-memory snapshots for fast lookup.
- The multi-tenant path creates separate per-team stacks, each with its own worker pool, session manager, and memory rebalancer.
- The K8s worker backend can spawn worker pods on demand and supports per-team labels plus per-team min/max worker settings.
- Within a team, the current worker pool can reuse live workers and assign more than one active session to the same worker.
- An admin API and dashboard for inspecting and managing config-store and worker/session state.

## Remaining Work

- Add full managed-warehouse contract to the config store. It needs team-scoped warehouse metadata such as warehouse-database info, metadata-store info, S3 bucket info, worker identity, secret refs, and provisioning state.
- Add end-to-end provisioning controller that turns a PostHog warehouse request into warehouse DB, metadata store, S3, IAM/Pod Identity, K8s Secret creation, and config-store updates.
- Implement team-specific DuckLake/warehouse-db/metadata-store/S3 runtime wiring.
- Change worker pod spec from global to be team-specific. 
- Implement non-disruptive customer-initiated duckgres login rotation mechanism.
- Dev Infra
  - Aurora cluster
  - Deploy control plane in k8
  - Public access exposure
- Runbooks for deployment, debugging and regular security/isolation reviews

## Out of scope for MVP / dev release
- full fledged admin console/dashboard
- comprehensive observability
