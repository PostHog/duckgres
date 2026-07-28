import { describe, expect, it } from "vitest";
import {
  buildProvisionBody,
  DEFAULT_PROVISION_FORM,
  MAX_SLUG_ORG_ID_LENGTH,
  validateOrgId,
  validateProvisionForm,
  validateSchemaName,
  type ProvisionForm,
} from "./provision";

const form = (over: Partial<ProvisionForm> = {}): ProvisionForm => ({
  ...DEFAULT_PROVISION_FORM,
  orgId: "acme",
  databaseName: "acme",
  teamId: "42",
  ...over,
});

describe("validateOrgId", () => {
  it("accepts DNS-1123 slugs and canonical UUIDs", () => {
    expect(validateOrgId("acme")).toBeNull();
    expect(validateOrgId("acme-prod-1")).toBeNull();
    // 36 chars — over the slug cap, but allowed because the bucket name
    // compacts a UUID's hyphens (configstore.DucklingBucketName).
    expect(validateOrgId("0192f3c4-5d6e-7f80-9123-456789abcdef")).toBeNull();
  });

  it("rejects non-DNS-1123 shapes", () => {
    expect(validateOrgId("")).toMatch(/required/);
    expect(validateOrgId("Acme")).toMatch(/DNS-1123/);
    expect(validateOrgId("acme_prod")).toMatch(/DNS-1123/);
    expect(validateOrgId("-acme")).toMatch(/DNS-1123/);
    expect(validateOrgId("acme-")).toMatch(/DNS-1123/);
  });

  it("caps non-UUID slugs at the bucket-name budget", () => {
    expect(validateOrgId("a".repeat(MAX_SLUG_ORG_ID_LENGTH))).toBeNull();
    expect(validateOrgId("a".repeat(MAX_SLUG_ORG_ID_LENGTH + 1))).toMatch(/at most 35/);
  });
});

describe("validateSchemaName", () => {
  it("treats empty as unset (the server derives team_<id>)", () => {
    expect(validateSchemaName("")).toBeNull();
  });

  it("mirrors configstore.ValidateOrgTeamSchemaName", () => {
    expect(validateSchemaName("team_42")).toBeNull();
    expect(validateSchemaName("_private")).toBeNull();
    expect(validateSchemaName("42team")).toMatch(/lowercase identifier/);
    expect(validateSchemaName("Team")).toMatch(/lowercase identifier/);
    expect(validateSchemaName("a".repeat(64))).toMatch(/at most 63/);
  });
});

describe("validateProvisionForm", () => {
  it("passes the default cnpg-shard shape", () => {
    expect(validateProvisionForm(form(), false)).toEqual({});
  });

  it("requires team_id only when the provision creates a new org", () => {
    expect(validateProvisionForm(form({ teamId: "" }), false).teamId).toMatch(/team_id is required/);
    expect(validateProvisionForm(form({ teamId: "" }), true).teamId).toBeUndefined();
  });

  it("rejects a non-positive-integer team id", () => {
    expect(validateProvisionForm(form({ teamId: "0" }), false).teamId).toMatch(/positive/);
    expect(validateProvisionForm(form({ teamId: "-3" }), false).teamId).toMatch(/positive/);
    expect(validateProvisionForm(form({ teamId: "1.5" }), false).teamId).toMatch(/positive/);
  });

  it("requires database_name", () => {
    expect(validateProvisionForm(form({ databaseName: "  " }), false).databaseName).toMatch(/required/);
  });

  it("requires team_id for a schema_name override", () => {
    const errs = validateProvisionForm(form({ teamId: "", schemaName: "legacy" }), true);
    expect(errs.schemaName).toMatch(/requires team_id/);
  });

  it("requires endpoint + secret for an external metadata store", () => {
    const errs = validateProvisionForm(form({ metadataType: "external" }), false);
    expect(errs.externalEndpoint).toMatch(/endpoint/);
    expect(errs.externalSecret).toMatch(/password_aws_secret/);
  });

  it("requires bucket_name for an external data store", () => {
    const errs = validateProvisionForm(form({ dataStoreType: "external" }), false);
    expect(errs.bucketName).toMatch(/bucket_name/);
  });
});

describe("buildProvisionBody", () => {
  it("emits the standard PostHog-backend body for the defaults", () => {
    expect(buildProvisionBody(form())).toEqual({
      database_name: "acme",
      team_id: 42,
      metadata_store: { type: "cnpg-shard" },
      data_store: { type: "s3bucket" },
      ducklake: { enabled: true },
    });
  });

  it("omits optional fields instead of sending them empty", () => {
    const body = buildProvisionBody(form({ teamId: "", schemaName: "" }));
    expect("team_id" in body).toBe(false);
    expect("schema_name" in body).toBe(false);
  });

  it("always requests DuckLake (the server rejects false)", () => {
    expect(buildProvisionBody(form()).ducklake).toEqual({ enabled: true });
  });

  it("carries the external metadata store block, defaulted user/database included", () => {
    const body = buildProvisionBody(
      form({
        metadataType: "external",
        externalEndpoint: " db.example.internal ",
        externalSecret: " duckling-acme-rds-password ",
      }),
    );
    expect(body.metadata_store).toEqual({
      type: "external",
      external: {
        endpoint: "db.example.internal",
        password_aws_secret: "duckling-acme-rds-password",
        user: "postgres",
        database: "postgres",
      },
    });
  });

  it("omits user/database when cleared so the XRD default applies", () => {
    const body = buildProvisionBody(
      form({
        metadataType: "external",
        externalEndpoint: "db.example.internal",
        externalSecret: "duckling-acme-rds-password",
        externalUser: "",
        externalDatabase: "",
      }),
    );
    expect(body.metadata_store.external).toEqual({
      endpoint: "db.example.internal",
      password_aws_secret: "duckling-acme-rds-password",
    });
  });

  it("carries an external data store with its optional region", () => {
    expect(buildProvisionBody(form({ dataStoreType: "external", bucketName: "existing-bucket" })).data_store).toEqual({
      type: "external",
      bucket_name: "existing-bucket",
    });
    expect(
      buildProvisionBody(form({ dataStoreType: "external", bucketName: "existing-bucket", region: "us-east-1" }))
        .data_store,
    ).toEqual({ type: "external", bucket_name: "existing-bucket", region: "us-east-1" });
  });

  it("trims whitespace out of every identifier", () => {
    const body = buildProvisionBody(form({ orgId: " acme ", databaseName: " acme_db ", schemaName: " team_42 " }));
    expect(body.database_name).toBe("acme_db");
    expect(body.schema_name).toBe("team_42");
  });
});
