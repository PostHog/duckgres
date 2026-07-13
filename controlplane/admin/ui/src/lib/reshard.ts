// Client-side guidance for the cnpg→external reshard form's AWS Secrets
// Manager secret name. The composition wires an ESO ExternalSecret that reads
// the secret with the external-secrets IAM role, whose policy only allows
// GetSecretValue on a per-environment name-prefix allowlist — `posthog-*` and
// `duckling-*` in every managed-warehouse environment (see the
// external-secrets-pod-identity terraform module in posthog-cloud-infra). An
// RDS-managed master secret (`rds!db-…` / `rds/…/master`) never matches, so
// the cutover would hang on an ESO AccessDenied. The server enforces a POSITIVE
// allowlist: it 400s any name that isn't `posthog-`/`duckling-`-prefixed (the
// RDS-managed shape gets a more specific message). The two prefixes are
// identical across every managed-warehouse environment, so this classifier
// mirrors the server rule and warns before submit — an unknown prefix will be
// rejected, not merely flagged.
export type SecretNameVerdict = "empty" | "rds-managed" | "unknown-prefix" | "ok";

export function classifySecretName(name: string): SecretNameVerdict {
  const n = name.trim();
  if (n === "") return "empty";
  if (/^rds[/!]/.test(n)) return "rds-managed";
  if (!/^(duckling|posthog)-/.test(n)) return "unknown-prefix";
  return "ok";
}
