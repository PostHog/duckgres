// Client-side guidance for the cnpg→external reshard form's AWS Secrets
// Manager secret name. The composition wires an ESO ExternalSecret that reads
// the secret with the external-secrets IAM role, whose policy only allows
// GetSecretValue on a per-environment name-prefix allowlist — `posthog-*` and
// `duckling-*` in every managed-warehouse environment (see the
// external-secrets-pod-identity terraform module in posthog-cloud-infra). An
// RDS-managed master secret (`rds!db-…` / `rds/…/master`) never matches, so
// the cutover would hang on an ESO AccessDenied; the server rejects those
// with a 400 and the form warns before submit. Names outside the known
// prefixes only WARN (soft): the prefix list is per-environment terraform and
// the console can't know its environment's extras.
export type SecretNameVerdict = "empty" | "rds-managed" | "unknown-prefix" | "ok";

export function classifySecretName(name: string): SecretNameVerdict {
  const n = name.trim();
  if (n === "") return "empty";
  if (/^rds[/!]/.test(n)) return "rds-managed";
  if (!/^(duckling|posthog)-/.test(n)) return "unknown-prefix";
  return "ok";
}
