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
  const entries = { "acme-corp": 1, custom: 3 };

  it("matches the stored duckling_name or the org name exactly — no derivation", () => {
    expect(ducklingEntryFor(entries, "Acme-Corp", "custom")).toBe(3);
    expect(ducklingEntryFor(entries, "acme-corp", undefined)).toBe(1);
    // No lowercasing or hyphen-stripping of the org name anymore.
    expect(ducklingEntryFor(entries, "Acme-Corp", undefined)).toBeUndefined();
    expect(ducklingEntryFor(entries, "unknown-org", undefined)).toBeUndefined();
    expect(ducklingEntryFor(undefined, "acme-corp", "custom")).toBeUndefined();
  });
});
