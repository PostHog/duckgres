import { describe, expect, it } from "vitest";
import { fmtCompact, fmtMetricAxis, fmtMetricValue, orgLabel } from "./format";

describe("fmtCompact", () => {
  it("renders compact SI so large numbers don't overflow an axis", () => {
    expect(fmtCompact(20_000_000)).toBe("20M");
    expect(fmtCompact(1500)).toBe("1.5K");
    expect(fmtCompact(0)).toBe("0");
    expect(fmtCompact(0.42)).toBe("0.4");
  });
  it("handles nullish", () => {
    expect(fmtCompact(null)).toBe("—");
    expect(fmtCompact(undefined)).toBe("—");
    expect(fmtCompact(NaN)).toBe("—");
  });
});

describe("fmtMetricAxis", () => {
  it("formats byte units with binary prefixes (the S3 bytes-rate axis)", () => {
    // 20,000,000 B ≈ 19.1 MiB — the bug was this rendering as a clipped '00000'.
    expect(fmtMetricAxis(20_000_000, "B/s")).toBe("19.1 MB");
    expect(fmtMetricAxis(1024, "B")).toBe("1.0 KB");
  });
  it("formats non-byte units compactly", () => {
    expect(fmtMetricAxis(20_000_000, "ops/s")).toBe("20M");
    expect(fmtMetricAxis(0.5, "")).toBe("0.5");
  });
  it("returns empty string for nullish (recharts skips the tick)", () => {
    expect(fmtMetricAxis(null, "B/s")).toBe("");
    expect(fmtMetricAxis(NaN)).toBe("");
  });
});

describe("fmtMetricValue", () => {
  it("byte rate gets a /s suffix; plain bytes don't", () => {
    expect(fmtMetricValue(20_000_000, "B/s")).toBe("19.1 MB/s");
    expect(fmtMetricValue(1024, "B")).toBe("1.0 KB");
  });
  it("non-byte value keeps its unit label", () => {
    expect(fmtMetricValue(1500, "ops/s")).toBe("1.5K ops/s");
    expect(fmtMetricValue(0.42, "")).toBe("0.4");
  });
  it("nullish renders a dash", () => {
    expect(fmtMetricValue(null, "B/s")).toBe("—");
  });
});

describe("orgLabel", () => {
  const uuid = "019740a8-ac01-0000-cad1-26dbbe0cde55";

  it("prefers the database name", () => {
    expect(orgLabel({ name: uuid, database_name: "product_analytics", hostname_alias: "alias" })).toBe(
      "product_analytics",
    );
  });
  it("falls back to the hostname alias, then the UUID", () => {
    expect(orgLabel({ name: uuid, database_name: "", hostname_alias: "eu-prod" })).toBe("eu-prod");
    expect(orgLabel({ name: uuid, database_name: "", hostname_alias: null })).toBe(uuid);
    expect(orgLabel({ name: uuid })).toBe(uuid);
  });
});
