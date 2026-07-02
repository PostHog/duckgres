import { describe, expect, it } from "vitest";
import { hashColor, hashColorBg } from "./colors";
import { ducklingEntryFor } from "./format";

describe("hashColor", () => {
  it("is deterministic and distinct per shard", () => {
    expect(hashColor("shard-001")).toBe(hashColor("shard-001"));
    expect(hashColor("shard-001")).not.toBe(hashColor("shard-002"));
  });

  it("skips the success-green hue band (95–150°)", () => {
    for (const key of ["shard-001", "shard-002", "shard-042", "x", "abcdef"]) {
      const hue = Number(/hsl\((\d+) /.exec(hashColor(key))![1]);
      expect(hue < 95 || hue >= 150, `hue ${hue} for ${key}`).toBe(true);
    }
  });

  it("bg variant matches the fg hue with alpha", () => {
    expect(hashColorBg("shard-001")).toBe(hashColor("shard-001").replace(")", " / 0.15)"));
  });
});

describe("ducklingEntryFor", () => {
  const entries = { "acme-corp": 1, acmecorp: 2, custom: 3 };

  it("prefers the stored duckling_name, then canonical, then legacy", () => {
    expect(ducklingEntryFor(entries, "Acme-Corp", "custom")).toBe(3);
    expect(ducklingEntryFor(entries, "Acme-Corp")).toBe(1);
    expect(ducklingEntryFor({ acmecorp: 2 }, "Acme-Corp")).toBe(2);
    expect(ducklingEntryFor(entries, "unknown-org")).toBeUndefined();
    expect(ducklingEntryFor(undefined, "Acme-Corp")).toBeUndefined();
  });
});
