import { format as dateFnsFormat } from "date-fns";
import type { NumberType } from "./constants";

/** Formats a number according to its series type. */
export function formatNumber(value: number | null | undefined, type?: NumberType): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }
  if (type?.startsWith("INTEGER")) {
    return new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(value);
  }
  if (type?.startsWith("CURVED") || type?.startsWith("MAPPED")) {
    return new Intl.NumberFormat(undefined, { maximumFractionDigits: 4 }).format(value);
  }
  if (type?.startsWith("FLOAT1")) {
    return value.toFixed(1);
  }
  if (type?.startsWith("FLOAT2")) {
    return value.toFixed(2);
  }
  if (type?.startsWith("FLOAT3")) {
    return value.toFixed(3);
  }
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 6 }).format(value);
}

export function formatDate(value: string | Date): string {
  const date = typeof value === "string" ? new Date(value) : value;
  return dateFnsFormat(date, "yyyy-MM-dd HH:mm:ss");
}

export function formatAxisDate(value: Date): string {
  return dateFnsFormat(value, "HH:mm:ss");
}

/** Formats a millisecond interval into a human readable label (e.g. 60000 → "1m"). */
export function formatInterval(ms: number | null | undefined): string {
  if (!ms) {
    return "auto";
  }
  if (ms % 3_600_000 === 0) {
    return `${ms / 3_600_000}h`;
  }
  if (ms % 60_000 === 0) {
    return `${ms / 60_000}m`;
  }
  if (ms % 1000 === 0) {
    return `${ms / 1000}s`;
  }
  return `${ms}ms`;
}

/** Parses a human interval label ("1m", "30s", "5h") back to milliseconds. */
export function parseInterval(input: string): number | null {
  const match = /^(\d+)\s*(ms|s|m|h)$/i.exec(input.trim());
  if (!match) {
    return null;
  }
  const value = Number(match[1]);
  const unit = match[2].toLowerCase();
  const factor = unit === "ms" ? 1 : unit === "s" ? 1000 : unit === "m" ? 60_000 : 3_600_000;
  return value * factor;
}

export function formatBytes(bytes: number): string {
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  const units = ["KB", "MB", "GB", "TB"];
  let value = bytes;
  let unit = "B";
  for (const u of units) {
    value /= 1024;
    unit = u;
    if (value < 1024) {
      break;
    }
  }
  return `${value.toFixed(1)} ${unit}`;
}
