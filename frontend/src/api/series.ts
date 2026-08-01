import { postJson, putJson, deleteRequest, request } from "./client";
import type { FindSeriesRequest, SeriesFile, UiConfig } from "./types";

const BASE = "/v2";

export function findSeries(request: FindSeriesRequest): Promise<SeriesFile[]> {
  return postJson(`${BASE}/series/find`, request);
}

export function getSeries(id: string): Promise<SeriesFile> {
  return request<SeriesFile>(`${BASE}/series/${encodeURIComponent(id)}`);
}

export function getSeriesMetadata(id: string): Promise<Record<string, string>> {
  return request<Record<string, string>>(`${BASE}/series/${encodeURIComponent(id)}/metadata`);
}

export function createSeries(series: SeriesFile): Promise<void> {
  return postJson(`${BASE}/series`, series);
}

export function deleteSeries(id: string): Promise<void> {
  return deleteRequest(`${BASE}/series/${encodeURIComponent(id)}`);
}

export function updateMetadata(id: string, metadata: Record<string, string>): Promise<void> {
  return putJson(`${BASE}/series/${encodeURIComponent(id)}/metadata`, metadata);
}

export function getUiConfig(): Promise<UiConfig> {
  return request<UiConfig>(`${BASE}/ui/config`);
}

/** Builds an anchored alternation pattern matching exactly the given series ids. */
export function patternForIds(ids: string[]): string {
  if (ids.length === 0) {
    return ".*";
  }
  const escaped = ids.map((id) => id.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  return `^(${escaped.join("|")})$`;
}
