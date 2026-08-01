/**
 * TypeScript mirror of the /v2 REST API schema
 * (org.huebert.iotfsdb.api.schema). Keep in sync with the backend; the
 * springdoc spec at /v3/api-docs.yaml is the source of truth.
 */

export type NumberType =
  | "CURVED1"
  | "CURVED2"
  | "CURVED4"
  | "FLOAT1"
  | "FLOAT2"
  | "FLOAT3"
  | "FLOAT4"
  | "FLOAT8"
  | "INTEGER1"
  | "INTEGER2"
  | "INTEGER4"
  | "INTEGER8"
  | "MAPPED1"
  | "MAPPED2"
  | "MAPPED4";

export type PartitionPeriod = "DAY" | "MONTH" | "YEAR";

export type Reducer =
  | "AVERAGE"
  | "COUNT"
  | "COUNT_DISTINCT"
  | "FIRST"
  | "LAST"
  | "MAXIMUM"
  | "MEDIAN"
  | "MINIMUM"
  | "MODE"
  | "MULTIPLY"
  | "SQUARE_SUM"
  | "SUM";

export type DateTimePreset =
  | "NONE"
  | "LAST_5_MINUTES"
  | "LAST_15_MINUTES"
  | "LAST_30_MINUTES"
  | "LAST_1_HOUR"
  | "LAST_3_HOURS"
  | "LAST_6_HOURS"
  | "LAST_12_HOURS"
  | "LAST_24_HOURS"
  | "LAST_2_DAYS"
  | "LAST_7_DAYS"
  | "LAST_30_DAYS"
  | "LAST_90_DAYS"
  | "LAST_6_MONTHS"
  | "LAST_1_YEAR"
  | "LAST_2_YEARS"
  | "LAST_5_YEARS";

export interface SeriesDefinition {
  id: string;
  type: NumberType;
  interval: number;
  partition: PartitionPeriod;
  min?: number;
  max?: number;
}

export interface SeriesFile {
  definition: SeriesDefinition;
  metadata: Record<string, string>;
}

export interface FindSeriesRequest {
  pattern: string;
  metadata: Record<string, string>;
}

export interface SeriesData {
  time: string;
  value: number | null;
}

export interface FindDataResponse {
  series: SeriesFile;
  data: SeriesData[];
}

export interface FindDataRequest {
  dateTimePreset?: DateTimePreset;
  timezone?: string;
  from?: string;
  to?: string;
  series: FindSeriesRequest;
  interval?: number;
  size?: number;
  includeNull?: boolean;
  useBigDecimal?: boolean;
  usePrevious?: boolean;
  nullValue?: number;
  timeReducer?: Reducer;
  seriesReducer?: Reducer;
}

export interface InsertRequest {
  series: string;
  values: SeriesData[];
  reducer?: Reducer;
}

export interface UiConfig {
  version: string;
  readOnly: boolean;
  springdocEnabled: boolean;
  statsEnabled: boolean;
  maxQuerySize: number;
  defaultSeries: SeriesFile;
}
