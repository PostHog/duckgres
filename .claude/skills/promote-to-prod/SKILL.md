---
name: promote-to-prod
description: Promote the duckgres control-plane image to prod from the CLI — resolve the built image ref from the charts state file, fire the charts Promote-to-Prod workflow, and watch it through the approval gate. Use when the user says "promote duckgres", "ship the CP to prod", "roll prod", or asks which image ref to promote.
---

# Promote duckgres to prod

Prod promotion is manual by policy: `state/duckgres.yaml` in PostHog/charts
has `require_prod_approval: true`, so the ONLY path to prod is the charts
repo's `promote-to-prod.yml` workflow, gated by required reviewers on the
`prod-promote-<team>` GitHub environment. The result posts to
#alerts-managed-warehouse.

## How images reach the state file (no action needed)

Merge to duckgres `main` → `container-image-cd.yml` builds per-arch images +
a multi-arch manifest, then repo-dispatches `commit_state_update` to
PostHog/charts → the deploy bot writes `state.duckgres.image.sha` and
auto-promotes `image.dev` (mw-dev rolls immediately). The promotable ref is
the MULTI-ARCH MANIFEST digest in the state file — never a per-arch digest,
and never the `.dockerbuild` artifacts on the CD run (those are build
provenance).

## Procedure

1. **Resolve the ref** (and sanity-check it is the merge you expect):

```bash
STATE=$(gh api -H "Accept: application/vnd.github.raw" \
  /repos/PostHog/charts/contents/state/duckgres.yaml)
DEV=$(echo "$STATE" | yq '.state.duckgres.image.dev')
PROD=$(echo "$STATE" | yq '.state.duckgres.image.prod')
echo "dev:  $DEV"
echo "prod: $PROD"
# The <git-sha> prefix of $DEV must be the duckgres main commit you intend
# to ship. If dev != sha the CD/deploy-bot hop hasn't landed yet — wait.
```

2. **Show the delta going out** (what prod is about to receive):

```bash
git log --oneline "${PROD%%@*}..${DEV%%@*}"
```

3. **Fire the promotion** (parks at the approval gate — nothing deploys yet):

```bash
gh workflow run promote-to-prod.yml -R PostHog/charts \
  -f app=duckgres -f image="$DEV"
sleep 8
RUN_URL=$(gh run list -R PostHog/charts --workflow=promote-to-prod.yml \
  --limit 1 --json url --jq '.[0].url')
echo "$RUN_URL"
# Land the approver directly on the approval page:
open "$RUN_URL"
```

4. **Approval**: a required reviewer approves the pending deployment on the
   run page just opened. Never self-approve programmatically on the
   operator's behalf — open the page and wait.

5. **Watch to completion** (after approval):

```bash
gh run watch <run-id> -R PostHog/charts --exit-status
# Verify the write landed:
gh api -H "Accept: application/vnd.github.raw" \
  /repos/PostHog/charts/contents/state/duckgres.yaml | yq '.state.duckgres.image.prod'
```

ArgoCD picks up the new `image.prod` and rolls the prod control plane;
watch pods with `kubectl -n duckgres get pods -w` if verification beyond
ArgoCD is wanted.

## Notes

- `run-name` shows as "<actor> promoted duckgres to prod (<image>)" in the
  Actions list; the commit the run page header shows is main's HEAD at
  dispatch, NOT the commit the run creates — the created commit is linked
  from the job summary.
- The cache-proxy image is a separate deploy target: same workflow with
  `-f app=duckgres-cache-proxy` (state file `state/duckgres-cache-proxy.yaml`).
- Rollback = promote the previous known-good ref (visible in
  `git log -- state/duckgres.yaml` in the charts repo) through the same
  workflow.
