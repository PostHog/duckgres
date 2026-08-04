import { describe, expect, it } from "vitest";
import { categoryLabel, categoryVariant, isSystemError, sqlstateClass } from "./errors";

describe("categoryVariant", () => {
  it("marks system errors destructive", () => {
    expect(categoryVariant("system")).toBe("destructive");
  });
  it("marks DuckLake conflict/metadata variants as warning", () => {
    expect(categoryVariant("conflict")).toBe("warning");
    expect(categoryVariant("metadata_connection_lost")).toBe("warning");
  });
  it("marks user + unknown as low-signal secondary", () => {
    expect(categoryVariant("user")).toBe("secondary");
    expect(categoryVariant("something-new")).toBe("secondary");
  });
});

describe("categoryLabel", () => {
  it("humanizes known categories", () => {
    expect(categoryLabel("user")).toBe("User");
    expect(categoryLabel("system")).toBe("System");
    expect(categoryLabel("metadata_connection_lost")).toBe("Metadata lost");
  });
  it("passes through unknown, falling back to 'unknown' when empty", () => {
    expect(categoryLabel("weird")).toBe("weird");
    expect(categoryLabel("")).toBe("unknown");
  });
});

describe("isSystemError", () => {
  it("is true only for server-attributable categories", () => {
    expect(isSystemError("system")).toBe(true);
    expect(isSystemError("metadata_connection_lost")).toBe(true);
    expect(isSystemError("user")).toBe(false);
    expect(isSystemError("conflict")).toBe(false);
  });
});

describe("sqlstateClass", () => {
  it("buckets by the 2-char SQLSTATE class", () => {
    expect(sqlstateClass("42P01")).toBe("42");
    expect(sqlstateClass("XX000")).toBe("XX");
    expect(sqlstateClass("53200")).toBe("53");
  });
  it("returns ?? for empty/short codes", () => {
    expect(sqlstateClass("")).toBe("??");
    expect(sqlstateClass("4")).toBe("??");
    expect(sqlstateClass(undefined)).toBe("??");
  });
});
