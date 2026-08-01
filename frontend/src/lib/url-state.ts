/**
 * Shareable, readable URL state (§6.3.8).
 *
 * The data query serializes into explicit URLSearchParams (e.g.
 * `/data?q=temp.*&preset=LAST_24_HOURS&interval=60s&size=500&sel=a&sel=b`).
 * If the encoded query grows beyond a conservative budget the least-important
 * parameters are dropped first, and as a last resort the whole state is
 * compressed into a single opaque `state=` parameter.
 */

import { DEFAULT_PRESET, type DateTimePreset, type Reducer } from "./constants";

export const URL_BUDGET_LENIENT = 4000;
export const URL_BUDGET_STRICT = 6000;
export const URL_BUDGET_HARD = 8000;

export interface DataUrlState {
  q: string;
  preset: DateTimePreset | "";
  /** Local date-time strings ("yyyy-MM-ddTHH:mm") used when preset is empty. */
  from?: string;
  to?: string;
  /** IANA timezone id applied to from/to and output. */
  tz: string;
  /** Sampling interval in seconds (undefined → auto). */
  interval?: number;
  size: number;
  timeReducer: Reducer;
  seriesReducer?: Reducer;
  usePrevious: boolean;
  nullValue?: number;
  live: boolean;
  sel: string[];
}

export const DEFAULT_SIZE = 250;

export function defaultDataState(): DataUrlState {
  return {
    q: "",
    preset: DEFAULT_PRESET,
    tz: browserTimezone(),
    size: DEFAULT_SIZE,
    timeReducer: "AVERAGE",
    usePrevious: false,
    live: false,
    sel: [],
  };
}

export function browserTimezone(): string {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
  } catch {
    return "UTC";
  }
}

export function serializeDataState(state: DataUrlState): string {
  const params = new URLSearchParams();
  if (state.q) {
    params.set("q", state.q);
  }
  if (state.preset) {
    params.set("preset", state.preset);
  }
  if (!state.preset) {
    if (state.from) {
      params.set("from", state.from);
    }
    if (state.to) {
      params.set("to", state.to);
    }
  }
  if (state.tz && state.tz !== browserTimezone()) {
    params.set("tz", state.tz);
  }
  if (state.interval) {
    params.set("interval", `${state.interval}s`);
  }
  if (state.size !== DEFAULT_SIZE) {
    params.set("size", String(state.size));
  }
  if (state.timeReducer !== "AVERAGE") {
    params.set("timeReducer", state.timeReducer);
  }
  if (state.seriesReducer) {
    params.set("seriesReducer", state.seriesReducer);
  }
  if (state.usePrevious) {
    params.set("usePrevious", "1");
  }
  if (state.nullValue !== undefined) {
    params.set("nullValue", String(state.nullValue));
  }
  if (state.live) {
    params.set("live", "1");
  }
  for (const id of state.sel) {
    params.append("sel", id);
  }
  return params.toString();
}

/** Drops least-important parameters until the serialized state fits the budget. */
function trimState(state: DataUrlState): DataUrlState {
  let current = { ...state };
  let serialized = serializeDataState(current);
  if (serialized.length <= URL_BUDGET_LENIENT) {
    return current;
  }
  // 1. Drop the selected-series list (recomputed from q on reload).
  current = { ...current, sel: [] };
  serialized = serializeDataState(current);
  if (serialized.length <= URL_BUDGET_STRICT) {
    return current;
  }
  // 2. Drop advanced options.
  current = { ...current, seriesReducer: undefined, usePrevious: false, live: false, size: DEFAULT_SIZE };
  serialized = serializeDataState(current);
  if (serialized.length <= URL_BUDGET_HARD) {
    return current;
  }
  return current;
}

/**
 * Serializes the state into a full query string (without the leading "?").
 * Falls back to a compressed `state=` parameter when the readable form is too
 * large for the 8KB URL budget.
 */
export async function encodeDataUrl(state: DataUrlState): Promise<string> {
  const trimmed = trimState(state);
  const readable = serializeDataState(trimmed);
  if (readable.length <= URL_BUDGET_HARD) {
    return readable;
  }
  const params = new URLSearchParams();
  params.set("state", await gzipToBase64(JSON.stringify(trimmed)));
  return params.toString();
}

/**
 * Decodes a query string into {@link DataUrlState}. Handles both the readable
 * param form and the compressed `state=` fallback. Missing values fall back to
 * defaults; unknown parameters are ignored.
 */
export async function decodeDataUrl(query: string): Promise<DataUrlState> {
  const params = new URLSearchParams(query);
  const compressed = params.get("state");
  if (compressed) {
    try {
      const raw = await gunzipFromBase64(compressed);
      return { ...defaultDataState(), ...(JSON.parse(raw) as Partial<DataUrlState>) };
    } catch {
      // Fall through to the readable form (e.g. truncated share link).
    }
  }
  const intervalRaw = params.get("interval");
  const interval = intervalRaw ? parseIntervalSeconds(intervalRaw) : undefined;
  const sel = params.getAll("sel");
  const presetParam = params.get("preset");
  return {
    q: params.get("q") ?? "",
    preset: presetParam === null ? DEFAULT_PRESET : (presetParam as DateTimePreset | ""),
    from: params.get("from") ?? undefined,
    to: params.get("to") ?? undefined,
    tz: params.get("tz") ?? browserTimezone(),
    interval,
    size: parseInt(params.get("size") ?? String(DEFAULT_SIZE), 10) || DEFAULT_SIZE,
    timeReducer: (params.get("timeReducer") as Reducer) ?? "AVERAGE",
    seriesReducer: (params.get("seriesReducer") as Reducer | null) ?? undefined,
    usePrevious: params.get("usePrevious") === "1",
    nullValue: params.get("nullValue") !== null ? Number(params.get("nullValue")) : undefined,
    live: params.get("live") === "1",
    sel,
  };
}

function parseIntervalSeconds(raw: string): number | undefined {
  const match = /^(\d+)\s*(ms|s|m|h)$/i.exec(raw);
  if (!match) {
    return undefined;
  }
  const value = Number(match[1]);
  const unit = match[2].toLowerCase();
  const factor = unit === "ms" ? 1 / 1000 : unit === "s" ? 1 : unit === "m" ? 60 : 3600;
  return Math.round(value * factor);
}

async function gzipToBase64(text: string): Promise<string> {
  const bytes = new TextEncoder().encode(text);
  const stream = new Blob([bytes]).stream().pipeThrough(new CompressionStream("gzip"));
  const buffer = await new Response(stream).arrayBuffer();
  let binary = "";
  const view = new Uint8Array(buffer);
  for (let i = 0; i < view.length; i++) {
    binary += String.fromCharCode(view[i]);
  }
  return btoa(binary);
}

async function gunzipFromBase64(encoded: string): Promise<string> {
  const binary = atob(encoded);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"));
  const buffer = await new Response(stream).arrayBuffer();
  return new TextDecoder().decode(buffer);
}
