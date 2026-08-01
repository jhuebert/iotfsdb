import { describe, expect, it } from "vitest";
import { decodeDataUrl, defaultDataState, encodeDataUrl, serializeDataState, type DataUrlState } from "./url-state";

describe("serializeDataState", () => {
  it("serializes a minimal state", () => {
    const state: DataUrlState = {
      ...defaultDataState(),
      q: "temp.*",
    };
    expect(serializeDataState(state)).toBe("q=temp.*&preset=LAST_24_HOURS");
  });

  it("serializes sampling and selected series", () => {
    const state: DataUrlState = {
      ...defaultDataState(),
      q: "temp.*",
      interval: 60,
      size: 500,
      sel: ["a", "b"],
    };
    const query = serializeDataState(state);
    expect(query).toContain("q=temp.*");
    expect(query).toContain("interval=60s");
    expect(query).toContain("size=500");
    expect(query).toContain("sel=a");
    expect(query).toContain("sel=b");
  });

  it("omits defaults", () => {
    const state = defaultDataState();
    expect(serializeDataState(state)).toBe("preset=LAST_24_HOURS");
  });
});

describe("decodeDataUrl", () => {
  it("decodes a readable query", async () => {
    const state = await decodeDataUrl("q=temp.*&preset=LAST_24_HOURS&interval=60s&size=500&sel=a&sel=b&tz=UTC");
    expect(state.q).toBe("temp.*");
    expect(state.preset).toBe("LAST_24_HOURS");
    expect(state.interval).toBe(60);
    expect(state.size).toBe(500);
    expect(state.sel).toEqual(["a", "b"]);
    expect(state.usePrevious).toBe(false);
  });

  it("falls back to defaults for missing values", async () => {
    const state = await decodeDataUrl("");
    expect(state.q).toBe("");
    expect(state.preset).toBe("LAST_24_HOURS");
    expect(state.size).toBe(250);
    expect(state.sel).toEqual([]);
  });

  it("parses custom ranges and toggles", async () => {
    const state = await decodeDataUrl(
      "preset=&from=2024-01-01T00%3A00&to=2024-01-02T00%3A00&usePrevious=1&live=1&seriesReducer=SUM",
    );
    expect(state.preset).toBe("");
    expect(state.from).toBe("2024-01-01T00:00");
    expect(state.to).toBe("2024-01-02T00:00");
    expect(state.usePrevious).toBe(true);
    expect(state.live).toBe(true);
    expect(state.seriesReducer).toBe("SUM");
  });
});

describe("encodeDataUrl", () => {
  it("returns readable params for normal states", async () => {
    const state: DataUrlState = {
      ...defaultDataState(),
      q: "temp.*",
      sel: ["a"],
    };
    const query = await encodeDataUrl(state);
    expect(query).toBe("q=temp.*&preset=LAST_24_HOURS&sel=a");
  });

  it("drops selected series when the query is too large", async () => {
    const state: DataUrlState = {
      ...defaultDataState(),
      q: "temp.*",
      // push the readable form over the lenient budget with a huge metadata value
      sel: Array.from({ length: 200 }, (_, i) => `series-${i}`),
    };
    const query = await encodeDataUrl(state);
    expect(query.length).toBeLessThanOrEqual(8000);
  });
});
