import { describe, expect, it } from "vitest";
import { classifySecretName } from "./reshard";

describe("classifySecretName", () => {
  it("accepts the duckling-* convention the ESO policy allows", () => {
    expect(classifySecretName("duckling-acme-managed-warehouse-dev-us-east-1-rds-password")).toBe("ok");
    expect(classifySecretName("posthog-warehouse-thing")).toBe("ok");
    expect(classifySecretName("  duckling-x-rds-password  ")).toBe("ok");
  });

  it("flags RDS-managed master secrets (never ESO-readable)", () => {
    expect(classifySecretName("rds/duckling-example-managed-warehouse-dev/master")).toBe("rds-managed");
    expect(classifySecretName("rds!db-12345678-abcd")).toBe("rds-managed");
    expect(classifySecretName("rds!cluster-12345678")).toBe("rds-managed");
  });

  it("soft-warns on names outside the known prefixes", () => {
    expect(classifySecretName("my-own-secret")).toBe("unknown-prefix");
    // A bare prefix without the hyphen doesn't match the IAM glob (`duckling-*`).
    expect(classifySecretName("ducklingfoo")).toBe("unknown-prefix");
    expect(classifySecretName("rdsish-name")).toBe("unknown-prefix");
  });

  it("treats empty as its own state (no warning while typing)", () => {
    expect(classifySecretName("")).toBe("empty");
    expect(classifySecretName("   ")).toBe("empty");
  });
});
