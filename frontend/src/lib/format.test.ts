import { describe, expect, it } from "vitest";
import { formatInterval, formatNumber, parseInterval } from "./format";

describe("formatNumber", () => {
  it("renders em dash for null", () => {
    expect(formatNumber(null)).toBe("—");
    expect(formatNumber(undefined)).toBe("—");
  });

  it("formats integers", () => {
    expect(formatNumber(42, "INTEGER4")).toBe("42");
  });

  it("formats floats with fixed precision", () => {
    expect(formatNumber(1.23456, "FLOAT2")).toBe("1.23");
    expect(formatNumber(1.23456, "FLOAT4")).toBe("1.23456");
  });
});

describe("formatInterval", () => {
  it("formats milliseconds", () => {
    expect(formatInterval(1000)).toBe("1s");
    expect(formatInterval(60_000)).toBe("1m");
    expect(formatInterval(3_600_000)).toBe("1h");
    expect(formatInterval(250)).toBe("250ms");
    expect(formatInterval(null)).toBe("auto");
  });
});

describe("parseInterval", () => {
  it("parses human intervals", () => {
    expect(parseInterval("1s")).toBe(1000);
    expect(parseInterval("5m")).toBe(300_000);
    expect(parseInterval("1h")).toBe(3_600_000);
    expect(parseInterval("500ms")).toBe(500);
  });

  it("rejects garbage", () => {
    expect(parseInterval("abc")).toBeNull();
    expect(parseInterval("")).toBeNull();
  });
});
