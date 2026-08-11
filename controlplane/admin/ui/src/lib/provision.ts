// Client-side mirror of the warehouse provisioning contract.
//
// The console posts to POST /api/v1/orgs/:id/provision — the exact endpoint the
// PostHog backend (Django) posts to (controlplane/provisioning/api.go). The
// server is and stays authoritative: everything here is a pre-submit courtesy
// so an operator sees "org id must be a DNS-1123 label" while typing instead of
// as a 400 afterwards. Keep these rules in lockstep with api.go — a rule that
// drifts LOOSER just moves the error to submit time (safe); a rule that drifts
// STRICTER silently blocks a body the PostHog backend is allowed to send, which
// is the divergence this whole surface exists to prevent.

import type { ProvisionBody, ProvisionDataStore, ProvisionMetadataStore } from "@/types/api";

// Mirrors provisioning/api.go: ducklingOrgIDPattern (a single DNS-1123 label)
// and canonicalDucklingUUIDPattern (the UUID-shaped org ids PostHog sends).
const ORG_ID_RE = /^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/;
const ORG_ID_UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
// maxDucklingSlugOrgIDLength: a non-UUID org id must leave room for the
// derived S3 bucket name (63-char cap minus the managed-warehouse suffix).
export const MAX_SLUG_ORG_ID_LENGTH = 35;

// Mirrors configstore.ValidateOrgTeamSchemaName.
const SCHEMA_NAME_RE = /^[a-z_][a-z0-9_]*$/;
const MAX_SCHEMA_NAME_LENGTH = 63;

export function validateOrgId(orgID: string): string | null {
  const v = orgID.trim();
  if (v === "") return "org id is required";
  if (!ORG_ID_RE.test(v)) {
    return "org id must be a DNS-1123 label (lowercase alphanumerics and hyphens, starting and ending alphanumeric)";
  }
  if (!ORG_ID_UUID_RE.test(v) && v.length > MAX_SLUG_ORG_ID_LENGTH) {
    return `org id must be a canonical UUID or a slug of at most ${MAX_SLUG_ORG_ID_LENGTH} characters`;
  }
  return null;
}

export function validateSchemaName(name: string): string | null {
  const v = name.trim();
  if (v === "") return null; // optional — the server derives "team_<id>"
  if (v.length > MAX_SCHEMA_NAME_LENGTH) {
    return `schema_name must be at most ${MAX_SCHEMA_NAME_LENGTH} characters`;
  }
  if (!SCHEMA_NAME_RE.test(v)) {
    return "schema_name must be a lowercase identifier: [a-z0-9_], not starting with a digit";
  }
  return null;
}

// The form's editable state. Kept as strings (what inputs hold) and narrowed
// into a ProvisionBody by buildProvisionBody.
export interface ProvisionForm {
  orgId: string;
  databaseName: string;
  teamId: string;
  schemaName: string;
  metadataType: "cnpg-shard" | "external";
  externalEndpoint: string;
  externalSecret: string;
  externalUser: string;
  externalDatabase: string;
  dataStoreType: "s3bucket" | "external";
  bucketName: string;
  region: string;
}

// The defaults the PostHog backend sends for a standard managed warehouse:
// a cnpg shard for the DuckLake catalog, a fresh control-plane-named per-org
// S3 bucket, DuckLake on. An operator provisioning from the console starts
// from exactly that shape.
export const DEFAULT_PROVISION_FORM: ProvisionForm = {
  orgId: "",
  databaseName: "",
  teamId: "",
  schemaName: "",
  metadataType: "cnpg-shard",
  externalEndpoint: "",
  externalSecret: "",
  externalUser: "postgres",
  externalDatabase: "postgres",
  dataStoreType: "s3bucket",
  bucketName: "",
  region: "",
};

// validateProvisionForm returns field-keyed messages for everything the server
// would reject. `orgExists` mirrors the one server-side rule the client cannot
// evaluate alone: team_id is REQUIRED when the provision creates a NEW org
// (ErrProvisionTeamRequired → 400) and optional when re-provisioning an
// existing one (the stored teams are preserved, never wiped).
export function validateProvisionForm(
  f: ProvisionForm,
  orgExists: boolean,
): Partial<Record<keyof ProvisionForm, string>> {
  const errs: Partial<Record<keyof ProvisionForm, string>> = {};

  const orgErr = validateOrgId(f.orgId);
  if (orgErr) errs.orgId = orgErr;

  if (f.databaseName.trim() === "") errs.databaseName = "database_name is required";

  const teamId = f.teamId.trim();
  if (teamId === "") {
    if (!orgExists) {
      errs.teamId = "team_id is required when provisioning a warehouse for a new org";
    }
  } else if (!/^\d+$/.test(teamId) || Number(teamId) <= 0) {
    errs.teamId = "team_id must be a positive PostHog team id";
  }

  const schemaErr = validateSchemaName(f.schemaName);
  if (schemaErr) errs.schemaName = schemaErr;
  if (f.schemaName.trim() !== "" && teamId === "") errs.schemaName = "schema_name requires team_id";

  if (f.metadataType === "external") {
    if (f.externalEndpoint.trim() === "")
      errs.externalEndpoint = "endpoint is required for an external metadata store";
    if (f.externalSecret.trim() === "") {
      errs.externalSecret = "password_aws_secret is required for an external metadata store";
    }
  }

  if (f.dataStoreType === "external" && f.bucketName.trim() === "") {
    errs.bucketName = "bucket_name is required for an external data store";
  }

  return errs;
}

// buildProvisionBody narrows the form into the wire body. Optional fields are
// OMITTED rather than sent empty, so the request is byte-identical to what the
// PostHog backend sends for the same intent — the server's defaults (and the
// XRD's) apply to absent fields, never to an empty string.
export function buildProvisionBody(f: ProvisionForm): ProvisionBody {
  const metadata_store: ProvisionMetadataStore =
    f.metadataType === "external"
      ? {
          type: "external",
          external: {
            endpoint: f.externalEndpoint.trim(),
            password_aws_secret: f.externalSecret.trim(),
            ...(f.externalUser.trim() !== "" ? { user: f.externalUser.trim() } : {}),
            ...(f.externalDatabase.trim() !== "" ? { database: f.externalDatabase.trim() } : {}),
          },
        }
      : { type: "cnpg-shard" };

  const data_store: ProvisionDataStore =
    f.dataStoreType === "external"
      ? {
          type: "external",
          bucket_name: f.bucketName.trim(),
          ...(f.region.trim() !== "" ? { region: f.region.trim() } : {}),
        }
      : { type: "s3bucket" };

  const teamId = f.teamId.trim();
  return {
    database_name: f.databaseName.trim(),
    ...(teamId !== "" ? { team_id: Number(teamId) } : {}),
    ...(f.schemaName.trim() !== "" ? { schema_name: f.schemaName.trim() } : {}),
    metadata_store,
    data_store,
    // Never operator-settable: the server rejects `false` outright (a warehouse
    // without a catalog has nothing to attach), so exposing it as a toggle
    // would only offer a guaranteed 400.
    ducklake: { enabled: true },
  };
}
