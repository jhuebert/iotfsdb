/**
 * Shared constants mirroring the backend schema (org.huebert.iotfsdb.api.schema).
 */

export const ID_PATTERN = "[a-z0-9][a-z0-9._-]{0,127}";

export const NUMBER_TYPES = [
  "CURVED1",
  "CURVED2",
  "CURVED4",
  "FLOAT1",
  "FLOAT2",
  "FLOAT3",
  "FLOAT4",
  "FLOAT8",
  "INTEGER1",
  "INTEGER2",
  "INTEGER4",
  "INTEGER8",
  "MAPPED1",
  "MAPPED2",
  "MAPPED4",
] as const;

export type NumberType = (typeof NUMBER_TYPES)[number];

/** Types that support a min/max range. */
export const RANGE_TYPES = new Set<NumberType>(["CURVED1", "CURVED2", "CURVED4", "MAPPED1", "MAPPED2", "MAPPED4"]);

export const PARTITION_PERIODS = ["DAY", "MONTH", "YEAR"] as const;
export type PartitionPeriod = (typeof PARTITION_PERIODS)[number];

export const REDUCERS = [
  "AVERAGE",
  "COUNT",
  "COUNT_DISTINCT",
  "FIRST",
  "LAST",
  "MAXIMUM",
  "MEDIAN",
  "MINIMUM",
  "MODE",
  "MULTIPLY",
  "SQUARE_SUM",
  "SUM",
] as const;
export type Reducer = (typeof REDUCERS)[number];

export const REDUCED_ID = "reduced";

export const DATE_TIME_PRESETS = [
  { value: "LAST_5_MINUTES", label: "Last 5m" },
  { value: "LAST_15_MINUTES", label: "Last 15m" },
  { value: "LAST_30_MINUTES", label: "Last 30m" },
  { value: "LAST_1_HOUR", label: "Last 1h" },
  { value: "LAST_3_HOURS", label: "Last 3h" },
  { value: "LAST_6_HOURS", label: "Last 6h" },
  { value: "LAST_12_HOURS", label: "Last 12h" },
  { value: "LAST_24_HOURS", label: "Last 24h" },
  { value: "LAST_2_DAYS", label: "Last 2d" },
  { value: "LAST_7_DAYS", label: "Last 7d" },
  { value: "LAST_30_DAYS", label: "Last 30d" },
  { value: "LAST_90_DAYS", label: "Last 90d" },
  { value: "LAST_6_MONTHS", label: "Last 6mo" },
  { value: "LAST_1_YEAR", label: "Last 1y" },
  { value: "LAST_2_YEARS", label: "Last 2y" },
  { value: "LAST_5_YEARS", label: "Last 5y" },
] as const;

export type DateTimePreset = (typeof DATE_TIME_PRESETS)[number]["value"];

export const DEFAULT_PRESET = "LAST_24_HOURS";

/** Sampling interval presets (display label → milliseconds). */
export const INTERVAL_PRESETS: { label: string; ms: number | null }[] = [
  { label: "Auto", ms: null },
  { label: "1s", ms: 1000 },
  { label: "1m", ms: 60_000 },
  { label: "5m", ms: 300_000 },
  { label: "1h", ms: 3_600_000 },
];
