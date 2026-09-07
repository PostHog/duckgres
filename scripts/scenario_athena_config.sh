#!/usr/bin/env bash
set -euo pipefail

# Emit validated GitHub environment-file assignments, never shell code to eval.
# Terraform owns this parameter and its exact-resource read permission.
parameter_name="/duckgres/perf/athena"
: "${AWS_REGION:?AWS_REGION is required to load Athena perf configuration}"

if ! config=$(aws ssm get-parameter --name "$parameter_name" --region "$AWS_REGION" \
  --query Parameter.Value --output text); then
  echo "Could not load Athena perf configuration from $parameter_name. Apply the Athena infrastructure and its CI read permission first." >&2
  exit 1
fi

# Validate the complete document before emitting anything: a bad value must not
# append a partial configuration or inject another entry into GITHUB_ENV.
if ! assignments=$(jq -ers '
  if length != 1 then error("expected one configuration object") else .[0] end
  | {
      SCENARIO_POD_IDENTITY_ROLE: .pod_identity_role_arn,
      DUCKGRES_SCENARIO_ATHENA_WORKGROUP: .workgroup_name,
      DUCKGRES_SCENARIO_ATHENA_DATABASE: .glue_database_name,
      DUCKGRES_SCENARIO_ATHENA_RESULTS_S3_URI: .results_s3_uri
    }
  | if all(.[]; type == "string" and test("\\S") and (test("[[:cntrl:]]") | not))
    then to_entries[] | "\(.key)=\(.value)"
    else error("expected nonempty single-line strings for all four Athena settings")
    end
' <<< "$config"); then
  echo "Invalid Athena perf configuration in $parameter_name. Check the Terraform-managed parameter; no settings were exported." >&2
  exit 1
fi

printf '%s\n' "$assignments"
