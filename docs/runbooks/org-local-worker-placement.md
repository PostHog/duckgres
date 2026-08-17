# Org-local worker placement

Duckgres can add a best-effort scheduling preference for a newly created,
org-bound worker to run on a hostname that already has another worker for the
same org. It is never a required constraint: the normal worker node selector
remains the only hard placement requirement.

## Local development

The preference is disabled by default. To exercise it in a local control-plane
deployment, set both deployment environment variables:

```text
DUCKGRES_K8S_WORKER_ORG_AFFINITY_ENABLED=true
DUCKGRES_K8S_WORKER_ORG_AFFINITY_WEIGHT=100
```

After an org-bound worker is created or activated, inspect its
`duckgres/placement-org` label and its preferred pod-affinity term. Do not use
this label for cache authorization or replace `duckgres/active-org`; the latter
continues to select the per-org network policy.

## Rollout and recovery

1. Deploy with affinity disabled and confirm org-bound worker pods gain the
   placement label.
2. Enable the setting in non-production and observe scheduling behavior.
3. Enable one production region, then the remaining regions.

To roll back immediately, set
`DUCKGRES_K8S_WORKER_ORG_AFFINITY_ENABLED=false` and roll the control-plane
deployment. Existing placement labels are harmless metadata and remain for
future scheduler matching; new pods then have no org-affinity preference.
