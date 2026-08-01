import { describe, expect, it } from "vitest";
import { parseSearch, stringifyFilter } from "./search-parser";

describe("parseSearch", () => {
  it("parses an empty string", () => {
    expect(parseSearch("")).toEqual({ filter: { pattern: "", metadata: {} } });
  });

  it("parses a bare series pattern", () => {
    expect(parseSearch("temp.*")).toEqual({ filter: { pattern: "temp.*", metadata: {} } });
  });

  it("parses a metadata filter", () => {
    const result = parseSearch("room:kitchen");
    expect(result.error).toBeUndefined();
    expect(result.filter.pattern).toBe("");
    expect(result.filter.metadata).toEqual({ room: "kitchen" });
  });

  it("parses pattern plus metadata filters", () => {
    const result = parseSearch("temp.* room:kitchen floor:2");
    expect(result.error).toBeUndefined();
    expect(result.filter.pattern).toBe("temp.*");
    expect(result.filter.metadata).toEqual({ room: "kitchen", floor: "2" });
  });

  it("supports quoted values with spaces", () => {
    const result = parseSearch('room:"kitchen floor"');
    expect(result.error).toBeUndefined();
    expect(result.filter.metadata).toEqual({ room: "kitchen floor" });
  });

  it("supports quoted keys with spaces and colons", () => {
    const result = parseSearch('"room name":floor "a:b":c');
    expect(result.error).toBeUndefined();
    expect(result.filter.metadata).toEqual({ "room name": "floor", "a:b": "c" });
  });

  it("handles escape sequences inside quotes", () => {
    const result = parseSearch('key:"say \\"hi\\" and \\\\"');
    expect(result.error).toBeUndefined();
    expect(result.filter.metadata).toEqual({ key: 'say "hi" and \\' });
  });

  it("rejects unbalanced quotes with position", () => {
    const result = parseSearch('temp.* room:"kitchen');
    expect(result.error).toEqual({ message: "Unbalanced quote", position: 12 });
  });

  it("rejects an empty metadata key", () => {
    const result = parseSearch(":value");
    expect(result.error?.message).toBe("Empty metadata key");
  });

  it("rejects an empty metadata value", () => {
    const result = parseSearch("room:");
    expect(result.error?.message).toBe("Empty metadata value");
  });

  it("rejects more than one series pattern", () => {
    const result = parseSearch("a.* b.*");
    expect(result.error?.message).toBe("Only one series pattern is allowed");
  });

  it("does not treat a colon inside a quoted value as a separator", () => {
    const result = parseSearch('room:"1:2"');
    expect(result.error).toBeUndefined();
    expect(result.filter.metadata).toEqual({ room: "1:2" });
  });

  it("allows regex metacharacters in the pattern", () => {
    const result = parseSearch("temp-\\d+");
    expect(result.error).toBeUndefined();
    expect(result.filter.pattern).toBe("temp-\\d+");
  });
});

describe("stringifyFilter", () => {
  it("round-trips a simple filter", () => {
    const input = "temp.* room:kitchen";
    const parsed = parseSearch(input);
    expect(parsed.error).toBeUndefined();
    expect(stringifyFilter(parsed.filter)).toBe(input);
  });

  it("quotes tokens that need it", () => {
    expect(stringifyFilter({ pattern: "", metadata: { room: "kitchen floor" } })).toBe('room:"kitchen floor"');
    expect(stringifyFilter({ pattern: "", metadata: { "room name": "floor" } })).toBe('"room name":floor');
  });

  it("escapes quotes and backslashes on round-trip", () => {
    const filter = { pattern: "", metadata: { key: 'say "hi" \\done' } };
    const reparsed = parseSearch(stringifyFilter(filter));
    expect(reparsed.error).toBeUndefined();
    expect(reparsed.filter.metadata).toEqual(filter.metadata);
  });

  it("omits the pattern when empty", () => {
    expect(stringifyFilter({ pattern: "", metadata: { a: "b" } })).toBe("a:b");
  });
});
