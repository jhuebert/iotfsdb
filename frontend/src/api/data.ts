import { download, postJson } from "./client";
import type { FindDataRequest, FindDataResponse, InsertRequest } from "./types";

const BASE = "/v2/data";

export function findData(request: FindDataRequest): Promise<FindDataResponse[]> {
  return postJson(`${BASE}/find`, request);
}

export function insertData(request: InsertRequest[]): Promise<void> {
  return postJson(`${BASE}`, request);
}

export function exportData(request: { pattern: string; metadata?: Record<string, string> }): Promise<void> {
  return download(`${BASE}/export`, request);
}

export function importData(file: File): Promise<void> {
  const form = new FormData();
  form.append("file", file);
  return fetch(`${BASE}/import`, {
    method: "POST",
    body: form,
  }).then(async (response) => {
    if (!response.ok) {
      let message = `Import failed with status ${response.status}`;
      try {
        const body = (await response.json()) as { message?: string };
        message = body.message ?? message;
      } catch {
        // keep generic message
      }
      throw new Error(message);
    }
  });
}
