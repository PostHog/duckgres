#!/usr/bin/env bash
# Resolve deployment identifiers after AWS OIDC authentication. Never trace values.
set -euo pipefail
set +x
: "${GITHUB_ENV:?GITHUB_ENV is required}"
role_json="$(aws iam get-role --role-name hoglake-ci-dev --output json)"
policy_json="$(aws iam get-role-policy --role-name hoglake-ci-dev --policy-name hoglake-ci-storage --output json)"
role="$(jq -er '.Role.Arn | select(test("^arn:aws:iam::[0-9]{12}:role/hoglake-ci-dev$"))' <<< "$role_json")"
# Require one exact CI object scope. Never infer a bucket from an arbitrary ARN
# or silently choose one of several resources; cleanup uses this same base path.
path="$(jq -er '
  def array: if type == "array" then . else [.] end;
  [.PolicyDocument.Statement[] | select(.Effect == "Allow") |
    select((.Action | array | index("s3:PutObject")) != null) |
    .Resource | array[]] | unique |
  select(length == 1) | .[0] |
  capture("^arn:aws:s3:::(?<bucket>[a-z0-9][a-z0-9.-]{1,61}[a-z0-9])/trino/ci-pr-\\*$") |
  "s3://\(.bucket)/trino/"
' <<< "$policy_json")"
bucket="${path#s3://}"
bucket="${bucket%%/*}"
account="${role#arn:aws:iam::}"
account="${account%%:*}"
for value in "$role" "$path" "$bucket" "$account"; do
  printf '::add-mask::%s\n' "$value"
done
# Publish only after every lookup and validation succeeds.
printf 'HOGLAKE_CI_POD_IDENTITY_ROLE=%s\nHOGLAKE_DATA_PATH=%s\n' "$role" "$path" >> "$GITHUB_ENV"
