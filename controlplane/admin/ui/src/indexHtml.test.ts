import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

describe("index.html", () => {
  it("uses the hotdog image as the admin dashboard favicon", () => {
    const html = readFileSync(resolve(__dirname, "../index.html"), "utf8");
    const document = new DOMParser().parseFromString(html, "text/html");

    const icon = document.querySelector<HTMLLinkElement>('link[rel="icon"]');

    expect(icon?.getAttribute("type")).toBe("image/png");
    expect(icon?.getAttribute("href")).toBe("/src/assets/hotdog.png");
  });
});
