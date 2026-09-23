#!/usr/bin/env bash
# Print the newest PostHog/trino master image as a digest-pinned reference:
#   ghcr.io/posthog/trino:<full revision>@sha256:<digest>
#
# The fork's publisher (PostHog/trino .github/bin/trino-release.sh) tags every
# master build with a source-ordered alias r<first-parent count, 12 digits>-<sha6>
# and only master can publish one, so the lexically greatest ordered tag is the
# newest built master commit. It can trail master HEAD while a build is still
# running; that is the newest image that exists. The ordered tag is resolved to
# its digest and the index's provenance annotations are verified before use.
set -euo pipefail

repository="${TRINO_IMAGE_REPOSITORY:-posthog/trino}"
registry=ghcr.io
source_url=https://github.com/PostHog/trino

fail() { printf 'resolve_trino_master_image: %s\n' "$*" >&2; exit 1; }

sha256_hex() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum | cut -d' ' -f1; else shasum -a 256 | cut -d' ' -f1; fi
}

scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT

token="$(curl -fsS --max-time 30 "https://$registry/token?scope=repository:$repository:pull" | jq -er .token)" \
  || fail "cannot obtain an anonymous pull token for $registry/$repository"

# Follow the registry's Link pagination so a long tag list is never truncated.
next="/v2/$repository/tags/list?n=1000"
: > "$scratch/tags"
while [ -n "$next" ]; do
  curl -fsS --max-time 30 -H "Authorization: Bearer $token" -D "$scratch/headers" \
    "https://$registry$next" -o "$scratch/page" || fail "cannot list tags for $registry/$repository"
  jq -er '.tags // [] | .[]' "$scratch/page" >> "$scratch/tags" || fail "malformed tag list"
  next="$(tr -d '\r' < "$scratch/headers" | sed -nE 's/^[Ll]ink: *<([^>]+)>; *rel="next".*/\1/p')"
done

ordered_tag="$(grep -E '^r[0-9]{12}-[0-9a-f]{6}$' "$scratch/tags" | sort | tail -n 1 || true)"
[ -n "$ordered_tag" ] || fail "no ordered master release tag found in $registry/$repository"

curl -fsS --max-time 30 -H "Authorization: Bearer $token" \
  -H 'Accept: application/vnd.oci.image.index.v1+json' \
  "https://$registry/v2/$repository/manifests/$ordered_tag" -o "$scratch/index" \
  || fail "cannot fetch manifest for $ordered_tag"

digest="sha256:$(sha256_hex < "$scratch/index")"
revision="$(jq -er --arg source "$source_url" '
  select(.mediaType == "application/vnd.oci.image.index.v1+json")
  | select(.annotations["org.opencontainers.image.source"] == $source)
  | .annotations["org.opencontainers.image.revision"]
  | select(test("^[0-9a-f]{40}$"))
' "$scratch/index")" || fail "$ordered_tag has invalid media type or source provenance"
[ "${revision:0:6}" = "${ordered_tag##*-}" ] || fail "$ordered_tag revision annotation $revision does not match its tag"

printf '%s/%s:%s@%s\n' "$registry" "$repository" "$revision" "$digest"
