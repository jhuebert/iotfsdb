/**
 * Thin same-origin fetch client for the /v2 REST API. No CSRF/token handling
 * today (the SPA is same-origin behind the same proxy as the old UI); the
 * wrapper keeps the door open for API keys/OIDC later.
 */

export class ApiError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

async function parseError(response: Response): Promise<ApiError> {
  let message = `Request failed with status ${response.status}`;
  try {
    const body = (await response.json()) as { message?: string; detail?: string };
    message = body.message ?? body.detail ?? message;
  } catch {
    // Non-JSON error body; keep the generic message.
  }
  return new ApiError(response.status, message);
}

export async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...init?.headers,
    },
  });
  if (!response.ok) {
    throw await parseError(response);
  }
  if (response.status === 204) {
    return undefined as T;
  }
  return (await response.json()) as T;
}

export function postJson<T>(path: string, body: unknown): Promise<T> {
  return request<T>(path, { method: "POST", body: JSON.stringify(body) });
}

export function putJson<T>(path: string, body: unknown): Promise<T> {
  return request<T>(path, { method: "PUT", body: JSON.stringify(body) });
}

export function deleteRequest<T>(path: string): Promise<T> {
  return request<T>(path, { method: "DELETE" });
}

/**
 * Streams a binary response (e.g. an export zip) and triggers a browser
 * download using the Content-Disposition filename when available.
 */
export async function download(path: string, body: unknown): Promise<void> {
  const response = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    throw await parseError(response);
  }
  const blob = await response.blob();
  let filename = "iotfsdb-export.zip";
  const disposition = response.headers.get("Content-Disposition");
  const match = disposition?.match(/filename=([^;]+)/i);
  if (match) {
    filename = match[1].trim().replace(/^"|"$/g, "");
  }
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}
